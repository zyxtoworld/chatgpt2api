import assert from "node:assert/strict";
import test from "node:test";

import { createAuthStorageCoordinator } from "../src/lib/auth-storage-coordinator.js";
import { normalizeStoredAuthSession } from "../src/lib/auth-storage-record.js";

function createMemoryStorage() {
  const values = new Map([
    ["key", "old-key"],
    ["session", { key: "old-key", role: "admin" }],
  ]);
  const events = [];
  return {
    values,
    events,
    storage: {
      async setItem(key, value) {
        events.push(`set:${key}`);
        values.set(key, value);
      },
      async removeItem(key) {
        events.push(`remove:${key}`);
        values.delete(key);
      },
      async getItem(key) {
        events.push(`get:${key}`);
        return values.get(key);
      },
    },
  };
}

test("logout fences a stale validation before it can restore the cleared session", async () => {
  const memory = createMemoryStorage();
  const coordinator = createAuthStorageCoordinator({
    keyName: "key",
    sessionName: "session",
    getItem: memory.storage.getItem,
    ...memory.storage,
  });
  const validationLease = coordinator.beginValidation();

  const clearPromise = coordinator.clearSession();
  const staleCommit = await coordinator.setSessionIfCurrent(
    { key: "old-key", role: "admin" },
    validationLease,
  );

  assert.equal(staleCommit, false);
  await clearPromise;
  assert.equal(memory.values.has("key"), false);
  assert.equal(memory.values.has("session"), false);
  assert.deepEqual(memory.events, ["remove:session", "remove:key"]);
});

test("a fresh validation lease can commit after the fenced clear", async () => {
  const memory = createMemoryStorage();
  const coordinator = createAuthStorageCoordinator({
    keyName: "key",
    sessionName: "session",
    getItem: memory.storage.getItem,
    ...memory.storage,
  });

  await coordinator.clearSession();
  const freshLease = coordinator.beginValidation();
  const committed = await coordinator.setSessionIfCurrent(
    { key: "new-key", role: "user" },
    freshLease,
  );

  assert.equal(committed, true);
  assert.deepEqual(memory.values.get("key"), "new-key");
  assert.deepEqual(memory.values.get("session"), { key: "new-key", role: "user" });
  assert.deepEqual(memory.events, ["remove:session", "remove:key", "set:key", "set:session"]);
});

test("concurrent validations share the mutation epoch and both commit in either order", async () => {
  for (const order of ["a-first", "b-first"]) {
    const memory = createMemoryStorage();
    const coordinator = createAuthStorageCoordinator({
      keyName: "key",
      sessionName: "session",
      getItem: memory.storage.getItem,
      ...memory.storage,
    });
    const validationA = coordinator.beginValidation();
    const validationB = coordinator.beginValidation();
    const commitA = order === "a-first"
      ? coordinator.setSessionIfCurrent({ key: "old-key", role: "admin" }, validationA)
      : null;
    const commitB = order === "b-first"
      ? coordinator.setSessionIfCurrent({ key: "old-key", role: "admin" }, validationB)
      : null;
    const first = order === "a-first" ? commitA : commitB;
    const second = order === "a-first"
      ? coordinator.setSessionIfCurrent({ key: "old-key", role: "admin" }, validationB)
      : coordinator.setSessionIfCurrent({ key: "old-key", role: "admin" }, validationA);
    assert.equal(await first, true);
    assert.equal(await second, true);
    assert.equal(coordinator.isCurrent(validationA), true);
    assert.equal(coordinator.isCurrent(validationB), true);
    assert.deepEqual(memory.values.get("session"), { key: "old-key", role: "admin" });
  }
});

test("a new explicit login invalidates older validation without invalidating itself", async () => {
  const memory = createMemoryStorage();
  const coordinator = createAuthStorageCoordinator({
    keyName: "key",
    sessionName: "session",
    getItem: memory.storage.getItem,
    ...memory.storage,
  });
  const oldValidation = coordinator.beginValidation();
  const newLogin = coordinator.beginMutation();
  const newLoginCommit = coordinator.setSessionIfCurrent(
    { key: "new-key", role: "user" },
    newLogin,
  );
  const oldValidationCommit = coordinator.setSessionIfCurrent(
    { key: "old-key", role: "admin" },
    oldValidation,
  );

  assert.equal(await newLoginCommit, true);
  assert.equal(await oldValidationCommit, false);
  assert.deepEqual(memory.values.get("session"), { key: "new-key", role: "user" });
  assert.equal(coordinator.isCurrent(oldValidation), false);
  assert.equal(coordinator.isCurrent(newLogin), true);
});

test("an accepted logout is not cancelled by a fresh validation queued behind it", async () => {
  const memory = createMemoryStorage();
  let releaseFirstWrite;
  const firstWrite = new Promise((resolve) => {
    releaseFirstWrite = resolve;
  });
  const coordinator = createAuthStorageCoordinator({
    keyName: "key",
    sessionName: "session",
    getItem: memory.storage.getItem,
    setItem: async (...args) => {
      await firstWrite;
      return memory.storage.setItem(...args);
    },
    removeItem: memory.storage.removeItem,
  });

  const blocker = coordinator.enqueue(() => firstWrite);
  const clearPromise = coordinator.clearSession();
  const freshLease = coordinator.beginValidation();
  const freshSet = coordinator.setSessionIfCurrent(
    { key: "fresh-key", role: "user" },
    freshLease,
  );

  releaseFirstWrite();
  await Promise.all([blocker, clearPromise, freshSet]);
  assert.deepEqual(memory.events, [
    "remove:session",
    "remove:key",
    "set:key",
    "set:session",
  ]);
  assert.equal(memory.values.get("key"), "fresh-key");
  assert.deepEqual(memory.values.get("session"), { key: "fresh-key", role: "user" });
});

test("a partial session write is cleaned up and remains fail-closed", async () => {
  const memory = createMemoryStorage();
  const failure = new Error("session write failed");
  const coordinator = createAuthStorageCoordinator({
    keyName: "key",
    sessionName: "session",
    getItem: memory.storage.getItem,
    setItem: async (key, value) => {
      memory.events.push(`set:${key}`);
      if (key === "session") {
        throw failure;
      }
      memory.values.set(key, value);
    },
    removeItem: memory.storage.removeItem,
  });

  await assert.rejects(
    coordinator.setSessionIfCurrent(
      { key: "new-key", role: "user" },
      coordinator.beginValidation(),
    ),
    failure,
  );
  assert.deepEqual(memory.events, [
    "set:key",
    "set:session",
    "remove:session",
    "remove:key",
  ]);
  assert.equal(memory.values.has("key"), false);
  assert.equal(memory.values.has("session"), false);
});

test("clear attempts both removals even when the first removal fails", async () => {
  const memory = createMemoryStorage();
  const failure = new Error("session remove failed");
  const coordinator = createAuthStorageCoordinator({
    keyName: "key",
    sessionName: "session",
    getItem: memory.storage.getItem,
    setItem: memory.storage.setItem,
    removeItem: async (key) => {
      memory.events.push(`remove:${key}`);
      if (key === "session") {
        throw failure;
      }
      memory.values.delete(key);
    },
  });

  await assert.rejects(coordinator.clearSession(), failure);
  assert.deepEqual(memory.events, ["remove:session", "remove:key"]);
  assert.equal(memory.values.has("key"), false);
});

test("a validation cleanup cannot delete a newer explicit login", async () => {
  const memory = createMemoryStorage();
  const coordinator = createAuthStorageCoordinator({
    keyName: "key",
    sessionName: "session",
    getItem: memory.storage.getItem,
    setItem: memory.storage.setItem,
    removeItem: memory.storage.removeItem,
  });
  const validation = coordinator.beginValidation();
  const cleanup = coordinator.clearSessionIfCurrent(validation);
  const login = coordinator.beginMutation();
  const loginCommit = coordinator.setSessionIfCurrent(
    { key: "new-key", role: "user" },
    login,
  );

  assert.equal(await cleanup, false);
  assert.equal(await loginCommit, true);
  assert.deepEqual(memory.values.get("session"), { key: "new-key", role: "user" });
  assert.deepEqual(memory.events, ["set:key", "set:session"]);
});

test("an auth mutation invalidated during its first write cannot finish a stale session", async () => {
  const memory = createMemoryStorage();
  let releaseKeyWrite;
  let markKeyWriteStarted;
  const keyWriteStarted = new Promise((resolve) => {
    markKeyWriteStarted = resolve;
  });
  const keyWriteBlocked = new Promise((resolve) => {
    releaseKeyWrite = resolve;
  });
  const coordinator = createAuthStorageCoordinator({
    keyName: "key",
    sessionName: "session",
    getItem: memory.storage.getItem,
    setItem: async (key, value) => {
      if (key === "key") {
        markKeyWriteStarted();
        await keyWriteBlocked;
      }
      return memory.storage.setItem(key, value);
    },
    removeItem: memory.storage.removeItem,
  });
  const staleLease = coordinator.beginMutation();
  const staleWrite = coordinator.setSessionIfCurrent(
    { key: "stale-key", role: "admin" },
    staleLease,
  );

  await keyWriteStarted;
  const currentLease = coordinator.beginMutation();
  releaseKeyWrite();

  assert.equal(await staleWrite, false);
  assert.equal(memory.values.has("key"), false);
  assert.equal(memory.values.has("session"), false);
  assert.equal(coordinator.isCurrent(currentLease), true);
});

test("an auth mutation invalidated during its session write removes the stale pair", async () => {
  const memory = createMemoryStorage();
  let releaseSessionWrite;
  let markSessionWriteStarted;
  const sessionWriteStarted = new Promise((resolve) => {
    markSessionWriteStarted = resolve;
  });
  const sessionWriteBlocked = new Promise((resolve) => {
    releaseSessionWrite = resolve;
  });
  const coordinator = createAuthStorageCoordinator({
    keyName: "key",
    sessionName: "session",
    getItem: memory.storage.getItem,
    setItem: async (key, value) => {
      if (key === "session") {
        markSessionWriteStarted();
        await sessionWriteBlocked;
      }
      return memory.storage.setItem(key, value);
    },
    removeItem: memory.storage.removeItem,
  });
  const staleLease = coordinator.beginMutation();
  const staleWrite = coordinator.setSessionIfCurrent(
    { key: "stale-key", role: "admin" },
    staleLease,
  );

  await sessionWriteStarted;
  const currentLease = coordinator.beginMutation();
  releaseSessionWrite();

  assert.equal(await staleWrite, false);
  assert.equal(memory.values.has("key"), false);
  assert.equal(memory.values.has("session"), false);
  assert.equal(coordinator.isCurrent(currentLease), true);
});

test("persisted auth requires an exact key/session pair", () => {
  const session = { key: "session-key", role: "admin", subjectId: "1", name: "A" };
  assert.deepEqual(normalizeStoredAuthSession("session-key", session), session);
  assert.equal(normalizeStoredAuthSession("", session), null);
  assert.equal(normalizeStoredAuthSession("session-key", { role: "admin" }), null);
  assert.equal(normalizeStoredAuthSession("other-key", session), null);
});

test("a clear started during a pair read makes the read unusable", async () => {
  const memory = createMemoryStorage();
  let releaseRead;
  const readBlocked = new Promise((resolve) => {
    releaseRead = resolve;
  });
  const coordinator = createAuthStorageCoordinator({
    keyName: "key",
    sessionName: "session",
    getItem: async (key) => {
      await readBlocked;
      return memory.storage.getItem(key);
    },
    setItem: memory.storage.setItem,
    removeItem: memory.storage.removeItem,
  });

  const lease = coordinator.beginValidation();
  const read = coordinator.readPairIfCurrent(lease);
  const clear = coordinator.clearSession();
  releaseRead();

  assert.equal(await read, null);
  await clear;
});
