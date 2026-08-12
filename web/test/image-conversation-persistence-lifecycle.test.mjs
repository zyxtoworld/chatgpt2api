import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { createConversationQueueGate } from "../src/lib/image-conversation-queue-gate.js";
import { applyImageConversationUpdate } from "../src/lib/image-conversation-update.js";
import { createImageConversationWriteCoordinator } from "../src/lib/image-conversation-write-coordinator.js";

function deferred() {
  let resolve;
  const promise = new Promise((nextResolve) => {
    resolve = nextResolve;
  });
  return { promise, resolve };
}

test("a destructive write fences an old save and a stale lease cannot create a third save", async () => {
  const coordinator = createImageConversationWriteCoordinator();
  const gate = createConversationQueueGate();
  gate.activate();
  const lease = gate.begin("conversation-a");
  const save = deferred();
  const events = [];

  const savePromise = coordinator.enqueue(async () => {
    events.push("save:start");
    await save.promise;
    if (gate.accepts(lease)) {
      events.push("stale-save");
    }
  });
  gate.invalidate("conversation-a");
  const deletePromise = coordinator.enqueue(async () => {
    events.push("delete");
  });
  const hypotheticalLaterSave = coordinator.enqueue(async () => {
    if (gate.accepts(lease)) {
      events.push("third-save");
    }
  });

  await Promise.resolve();
  await Promise.resolve();
  assert.deepEqual(events, ["save:start"]);

  save.resolve();
  await savePromise;
  await deletePromise;
  await hypotheticalLaterSave;
  assert.deepEqual(events, ["save:start", "delete"]);
});

test("a failed save still drains so a later destructive write can run", async () => {
  const coordinator = createImageConversationWriteCoordinator();
  const events = [];
  const savePromise = coordinator.enqueue(async () => {
    events.push("save:start");
    throw new Error("save failed");
  });
  const deletePromise = coordinator.enqueue(async () => {
    events.push("delete");
  });

  await assert.rejects(savePromise, /save failed/);
  await deletePromise;
  assert.deepEqual(events, ["save:start", "delete"]);
});

test("a null conversation update is a no-op and cannot create a list entry", () => {
  const original = [];
  let changed = false;
  const result = applyImageConversationUpdate(original, "conversation-a", () => null);
  if (result.changed) changed = true;

  assert.equal(changed, false);
  assert.strictEqual(result.conversations, original);
  assert.deepEqual(result.conversations, original);
});

test("a deferred queue update cannot revive a conversation deleted before it settles", async () => {
  const coordinator = createImageConversationWriteCoordinator();
  const gate = createConversationQueueGate();
  gate.activate();
  const lease = gate.begin("conversation-a");
  const pending = deferred();
  let conversations = [{ id: "conversation-a", revision: 1 }];

  const staleQueueWrite = coordinator.enqueue(async () => {
    await pending.promise;
    const result = applyImageConversationUpdate(conversations, "conversation-a", (current) => {
      if (!gate.accepts(lease) || !current) {
        return null;
      }
      return { ...current, revision: 2 };
    });
    if (result.changed) {
      conversations = result.conversations;
    }
  });

  gate.invalidate("conversation-a");
  conversations = [];
  const deleteWrite = coordinator.enqueue(async () => {
    conversations = conversations.filter((conversation) => conversation.id !== "conversation-a");
  });

  pending.resolve();
  await Promise.all([staleQueueWrite, deleteWrite]);
  assert.deepEqual(conversations, []);
});

test("a clear fence rejects every deferred queue update before the clear commits", async () => {
  const coordinator = createImageConversationWriteCoordinator();
  const gate = createConversationQueueGate();
  gate.activate();
  const lease = gate.begin("conversation-a");
  const pending = deferred();
  let conversations = [{ id: "conversation-a", revision: 1 }];

  const staleQueueWrite = coordinator.enqueue(async () => {
    await pending.promise;
    const result = applyImageConversationUpdate(conversations, "conversation-a", (current) => {
      if (!gate.accepts(lease) || !current) {
        return null;
      }
      return { ...current, revision: 2 };
    });
    if (result.changed) {
      conversations = result.conversations;
    }
  });

  gate.invalidateAll();
  const clearWrite = coordinator.enqueue(async () => {
    conversations = [];
  });

  pending.resolve();
  await Promise.all([staleQueueWrite, clearWrite]);
  assert.deepEqual(conversations, []);
});

test("all image conversation mutations use the same production coordinator", () => {
  const source = readFileSync(new URL("../src/store/image-conversations.ts", import.meta.url), "utf8");
  for (const functionName of [
    "saveImageConversations",
    "saveImageConversation",
    "renameImageConversation",
    "deleteImageConversation",
    "clearImageConversations",
  ]) {
    const start = source.indexOf(`export async function ${functionName}`);
    const next = source.indexOf("\nexport ", start + 1);
    assert.ok(start >= 0, `${functionName} must exist`);
    const body = source.slice(start, next >= 0 ? next : source.length);
    assert.match(body, /queueImageConversationWrite\(/, `${functionName} must use the shared write queue`);
  }
});
