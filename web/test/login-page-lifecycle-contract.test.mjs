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
