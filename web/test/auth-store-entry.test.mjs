import assert from "node:assert/strict";
import { register } from "node:module";
import test from "node:test";

globalThis.window = {};
register("./auth-store-loader.mjs", { parentURL: import.meta.url });

const [{ AUTH_KEY_STORAGE_KEY, AUTH_SESSION_STORAGE_KEY, beginStoredAuthValidation, clearStoredAuthSession, getStoredAuthKey, setStoredAuthSession, setStoredAuthSessionIfCurrent }, { request }, { state, reset }] = await Promise.all([
  import("../src/store/auth.ts"),
  import("../src/lib/request.ts"),
  import("./fixtures/localforage-mock.mjs"),
]);

const readAuthConfig = request.interceptors.request.handlers[0].fulfilled;
const rejectAuthResponse = request.interceptors.response.handlers[0].rejected;

async function authHeader() {
  const config = await readAuthConfig({ headers: {} });
  return config.headers.Authorization || "";
}

async function settleByNextTurn(promise) {
  return Promise.race([
    promise.then(
      (value) => ({ status: "resolved", value }),
      (error) => ({ status: "rejected", error }),
    ),
    new Promise((resolve) => setImmediate(() => resolve({ status: "pending" }))),
  ]);
}

test("getStoredAuthKey and request interceptor reject incomplete or mismatched pairs", async () => {
  const session = { key: "session-key", role: "admin", subjectId: "1", name: "A" };
  for (const values of [
    { [AUTH_KEY_STORAGE_KEY]: "key-only" },
    { [AUTH_SESSION_STORAGE_KEY]: session },
    { [AUTH_KEY_STORAGE_KEY]: "other-key", [AUTH_SESSION_STORAGE_KEY]: session },
  ]) {
    reset();
    await setStoredAuthSession(session);
    reset(values);
    assert.equal(await getStoredAuthKey(), "");
    assert.equal(await authHeader(), "");
    assert.equal(state.values.size, 0, "invalid pairs are cleared instead of reused");
  }
});

test("a valid pair is the only source of the Authorization header", async () => {
  const session = { key: "valid-key", role: "user", subjectId: "2", name: "B" };
  reset();
  await setStoredAuthSession(session);
  assert.equal(await authHeader(), "Bearer valid-key");
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

test("a protected 401 redirects and settles instead of leaving the caller pending", async () => {
  const session = { key: "expired-key", role: "admin", subjectId: "1", name: "A" };
  const redirects = [];
  globalThis.window.location = {
    pathname: "/settings",
    replace(path) {
      redirects.push(path);
    },
  };
  reset({ [AUTH_KEY_STORAGE_KEY]: session.key, [AUTH_SESSION_STORAGE_KEY]: session });

  const outcome = await settleByNextTurn(rejectAuthResponse({
    config: {},
    message: "Request failed with status code 401",
    response: { status: 401, data: { detail: { error: "expired" } } },
  }));

  assert.equal(outcome.status, "rejected");
  assert.equal(outcome.error?.message, "登录已失效，请重新登录");
  assert.deepEqual(redirects, ["/login"]);
  assert.equal(state.values.size, 0);
});

test("a protected 401 storage failure is safe and does not claim a redirect", async () => {
  const storageSecret = "indexeddb-internal-secret";
  const session = { key: "expired-key", role: "admin", subjectId: "1", name: "A" };
  const redirects = [];
  globalThis.window.location = {
    pathname: "/settings",
    replace(path) {
      redirects.push(path);
    },
  };
  reset({ [AUTH_KEY_STORAGE_KEY]: session.key, [AUTH_SESSION_STORAGE_KEY]: session });
  state.failures.set("remove", new Error(storageSecret));

  const outcome = await settleByNextTurn(rejectAuthResponse({
    config: {},
    message: "Request failed with status code 401",
    response: { status: 401, data: { detail: { error: "expired" } } },
  }));

  assert.equal(outcome.status, "rejected");
  assert.equal(outcome.error?.message, "登录状态清理失败，请刷新页面后重试");
  assert.doesNotMatch(outcome.error?.message || "", new RegExp(storageSecret));
  assert.deepEqual(redirects, []);
});
