import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

const source = readFileSync(
  fileURLToPath(new URL("../src/app/login/page.tsx", import.meta.url)),
  "utf8",
);

test("login owns auth persistence before the request and cancels late callbacks on cleanup", () => {
  assert.match(source, /createLoginRequestGate/);
  assert.match(source, /beginStoredAuthMutation/);
  assert.match(source, /setStoredAuthSessionIfCurrent/);
  assert.match(source, /return \(\) => loginGate\.cancel\(\)/);
  assert.doesNotMatch(source, /\bsetStoredAuthSession\(/);

  const beginIndex = source.indexOf("loginGateRef.current.begin(normalizedAuthKey)");
  const requestIndex = source.indexOf("await login(normalizedAuthKey)");
  const commitIndex = source.indexOf("await setStoredAuthSessionIfCurrent(");
  assert.ok(beginIndex >= 0);
  assert.ok(requestIndex >= 0);
  assert.ok(commitIndex >= 0);
  assert.ok(beginIndex < requestIndex);
  assert.ok(requestIndex < commitIndex);
});

test("each login page setup reactivates its captured gate after cleanup replay", () => {
  const effectIndex = source.indexOf("useEffect(() => {");
  const captureIndex = source.indexOf(
    "const loginGate = loginGateRef.current",
    effectIndex,
  );
  const activateIndex = source.indexOf("loginGate.activate()", captureIndex);
  const cancelIndex = source.indexOf("loginGate.cancel()", activateIndex);
  assert.ok(effectIndex >= 0);
  assert.ok(captureIndex > effectIndex);
  assert.ok(activateIndex > captureIndex);
  assert.ok(cancelIndex > activateIndex);
  assert.doesNotMatch(source, /return \(\) => loginGateRef\.current\.cancel\(\)/);
});

test("editing the credential invalidates a pending login before changing the field", () => {
  const handlerStart = source.indexOf("const handleAuthKeyChange");
  const handlerEnd = source.indexOf("const handleLogin", handlerStart);
  assert.ok(handlerStart >= 0, "missing auth key change handler");
  assert.ok(handlerEnd > handlerStart, "missing auth key change handler boundary");

  const handler = source.slice(handlerStart, handlerEnd);
  const invalidateIndex = handler.indexOf("loginGateRef.current.invalidate()");
  const clearBusyIndex = handler.indexOf("setIsSubmitting(false)");
  const updateFieldIndex = handler.indexOf("setAuthKey(value)");
  assert.ok(invalidateIndex >= 0, "credential edits must invalidate the pending login lease");
  assert.ok(clearBusyIndex > invalidateIndex, "credential edits must release the old login busy state");
  assert.ok(updateFieldIndex >= 0, "credential edits must update the field");
  assert.match(source, /onChange=\{\(event\) => handleAuthKeyChange\(event\.target\.value\)\}/);
});

test("login checks single-writer admission before starting the request", () => {
  const handlerStart = source.indexOf("const handleLogin");
  const handlerEnd = source.indexOf("if (isCheckingAuth)", handlerStart);
  assert.ok(handlerStart >= 0, "missing login handler");
  assert.ok(handlerEnd > handlerStart, "missing login handler boundary");

  const handler = source.slice(handlerStart, handlerEnd);
  const beginIndex = handler.indexOf("loginGateRef.current.begin(normalizedAuthKey)");
  const rejectedIndex = handler.indexOf("if (!loginOwner) return", beginIndex);
  const requestIndex = handler.indexOf("await login(normalizedAuthKey)");
  assert.ok(beginIndex >= 0, "missing login admission");
  assert.ok(rejectedIndex > beginIndex, "rejected duplicate login must stop before the request");
  assert.ok(requestIndex > rejectedIndex, "network login must start only after admission");
});
