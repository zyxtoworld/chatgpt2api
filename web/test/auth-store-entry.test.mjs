import assert from "node:assert/strict";
import { register } from "node:module";
import test from "node:test";

globalThis.window = {};
register("./auth-store-loader.mjs", { parentURL: import.meta.url });

const [{ AUTH_KEY_STORAGE_KEY, AUTH_SESSION_STORAGE_KEY, beginStoredAuthValidation, clearStoredAuthSession, getStoredAuthKey, setStoredAuthSessionIfCurrent }, { request }, { state, reset }] = await Promise.all([
  import("../src/store/auth.ts"),
  import("../src/lib/request.ts"),
  import("./fixtures/localforage-mock.mjs"),
]);

const readAuthConfig = request.interceptors.request.handlers[0].fulfilled;

async function authHeader(values) {
  reset(values);
  const config = await readAuthConfig({ headers: {} });
  return config.headers.Authorization || "";
}

test("getStoredAuthKey and request interceptor reject incomplete or mismatched pairs", async () => {
  const session = { key: "session-key", role: "admin", subjectId: "1", name: "A" };
  for (const values of [
    { [AUTH_KEY_STORAGE_KEY]: "key-only" },
    { [AUTH_SESSION_STORAGE_KEY]: session },
    { [AUTH_KEY_STORAGE_KEY]: "other-key", [AUTH_SESSION_STORAGE_KEY]: session },
  ]) {
    reset(values);
    assert.equal(await getStoredAuthKey(), "");
    assert.equal(await authHeader(values), "");
    assert.equal(state.values.size, 0, "invalid pairs are cleared instead of reused");
  }
});

test("a valid pair is the only source of the Authorization header", async () => {
  const session = { key: "valid-key", role: "user", subjectId: "2", name: "B" };
  assert.equal(
    await authHeader({ [AUTH_KEY_STORAGE_KEY]: "valid-key", [AUTH_SESSION_STORAGE_KEY]: session }),
    "Bearer valid-key",
  );
});

test("logout propagates any deletion failure instead of claiming success", async () => {
  for (const failureKey of ["session", "key", "both"]) {
    const failure = new Error(`remove ${failureKey} failed`);
    reset({ [AUTH_KEY_STORAGE_KEY]: "key", [AUTH_SESSION_STORAGE_KEY]: { key: "key", role: "user" } });
    if (failureKey === "both") {
      state.failures.set("remove", failure);
    } else {
      state.failures.set(`remove:${failureKey === "session" ? AUTH_SESSION_STORAGE_KEY : AUTH_KEY_STORAGE_KEY}`, failure);
    }

    await assert.rejects(clearStoredAuthSession(), failure);
    assert.deepEqual(state.events, [
      "remove:" + AUTH_SESSION_STORAGE_KEY,
      "remove:" + AUTH_KEY_STORAGE_KEY,
    ]);
    assert.equal(
      state.values.has(AUTH_KEY_STORAGE_KEY),
      failureKey === "key" || failureKey === "both",
      `${failureKey}: key deletion state`,
    );
    assert.equal(
      state.values.has(AUTH_SESSION_STORAGE_KEY),
      failureKey === "session" || failureKey === "both",
      `${failureKey}: session deletion state`,
    );
  }
});

test("the real auth store fences a stale validation around logout", async () => {
  const oldSession = { key: "old-key", role: "admin", subjectId: "1", name: "A" };
  reset({ [AUTH_KEY_STORAGE_KEY]: oldSession.key, [AUTH_SESSION_STORAGE_KEY]: oldSession });

  const validation = beginStoredAuthValidation();
  const clear = clearStoredAuthSession();
  const staleCommit = setStoredAuthSessionIfCurrent(oldSession, validation);

  assert.equal(await staleCommit, false);
  await clear;
  assert.equal(state.values.has(AUTH_KEY_STORAGE_KEY), false);
  assert.equal(state.values.has(AUTH_SESSION_STORAGE_KEY), false);
});

test("the auth store has no single-key mutation exports", async () => {
  const authStore = await import("../src/store/auth.ts");
  assert.equal("setStoredAuthKey" in authStore, false);
  assert.equal("clearStoredAuthKey" in authStore, false);
});
