import assert from "node:assert/strict";
import { register } from "node:module";
import test from "node:test";

globalThis.window = {};
register("./auth-session-loader.mjs", { parentURL: import.meta.url });

const [{ getValidatedAuthSession }, authStore, storage, api] = await Promise.all([
  import("../src/lib/auth-session.ts"),
  import("../src/store/auth.ts"),
  import("./fixtures/localforage-mock.mjs"),
  import("./fixtures/auth-api-mock.mjs"),
]);

test("a validation login returning after logout cannot restore the old session", async () => {
  const oldSession = { key: "old-key", role: "admin", subjectId: "old", name: "Old" };
  storage.reset({
    [authStore.AUTH_KEY_STORAGE_KEY]: oldSession.key,
    [authStore.AUTH_SESSION_STORAGE_KEY]: oldSession,
  });
  api.reset();
  const login = api.queueLoginResponse();

  const validation = getValidatedAuthSession();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(api.state.calls, ["old-key"]);

  await authStore.clearStoredAuthSession();
  login.resolve({ role: "admin", subject_id: "old", name: "Old refreshed" });

  assert.equal(await validation, null);
  assert.equal(storage.state.values.has(authStore.AUTH_KEY_STORAGE_KEY), false);
  assert.equal(storage.state.values.has(authStore.AUTH_SESSION_STORAGE_KEY), false);
});

test("a fresh explicit session and subsequent validation still commit", async () => {
  storage.reset();
  api.reset();
  const freshSession = { key: "fresh-key", role: "user", subjectId: "fresh", name: "Fresh" };

  await authStore.setStoredAuthSession(freshSession);
  const validated = await getValidatedAuthSession();

  assert.deepEqual(validated, freshSession);
  assert.equal(storage.state.values.get(authStore.AUTH_KEY_STORAGE_KEY), "fresh-key");
  assert.deepEqual(storage.state.values.get(authStore.AUTH_SESSION_STORAGE_KEY), freshSession);
});

test("concurrent real validations both succeed regardless of login response order", async () => {
  for (const order of ["a-first", "b-first"]) {
    const oldSession = { key: "shared-key", role: "admin", subjectId: "shared", name: "Shared" };
    storage.reset({
      [authStore.AUTH_KEY_STORAGE_KEY]: oldSession.key,
      [authStore.AUTH_SESSION_STORAGE_KEY]: oldSession,
    });
    api.reset();
    const loginA = api.queueLoginResponse();
    const loginB = api.queueLoginResponse();
    const validationA = getValidatedAuthSession();
    const validationB = getValidatedAuthSession();

    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(api.state.calls, ["shared-key", "shared-key"]);
    const response = { role: "admin", subject_id: "shared", name: "Shared" };
    if (order === "a-first") {
      loginA.resolve(response);
      loginB.resolve(response);
    } else {
      loginB.resolve(response);
      loginA.resolve(response);
    }

    assert.deepEqual(await Promise.all([validationA, validationB]), [oldSession, oldSession]);
    assert.deepEqual(storage.state.values.get(authStore.AUTH_SESSION_STORAGE_KEY), oldSession);
  }
});
