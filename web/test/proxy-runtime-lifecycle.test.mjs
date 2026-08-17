import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import {
  createProxyRuntimeRequestGate,
  runCurrentProxyFollowup,
  runProxyRuntimeSave,
} from "../src/lib/proxy-runtime-request-gate.js";

function deferred() {
  let resolve;
  const promise = new Promise((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

const source = readFileSync(
  fileURLToPath(new URL("../src/app/settings/components/proxy-runtime-card.tsx", import.meta.url)),
  "utf8",
);

test("ProxyRuntimeCard wires independent owners through mount cleanup and guarded effects", () => {
  assert.match(source, /createProxyRuntimeRequestGate/);
  assert.match(source, /runCurrentProxyFollowup/);
  assert.match(source, /return \(\) => requestGate\.cancel\(\)/);
  assert.match(source, /requestGate\.acceptsProxy\(request\)/);
  assert.match(source, /requestGate\.acceptsClearance\(request, candidate\)/);
  assert.match(source, /invalidateClearance\(\)/);
  assert.match(source, /const handleSave = async \(\) => \{[\s\S]*?runProxyRuntimeSave\(invalidateRuntimeTests, saveConfig\);/);
  assert.match(source, /onClick=\{\(\) => void handleSave\(\)\}/);
});

test("production runtime save invalidates both in-flight test owners before the write settles", async () => {
  const gate = createProxyRuntimeRequestGate();
  gate.activate();
  const proxy = gate.beginProxy();
  const clearance = gate.beginClearance("https://example.test/a");
  const save = deferred();

  const saving = runProxyRuntimeSave(
    () => {
      gate.invalidateProxy();
      gate.invalidateClearance();
    },
    () => save.promise,
  );

  assert.equal(gate.acceptsProxy(proxy), false);
  assert.equal(gate.acceptsClearance(clearance, "https://example.test/a"), false);
  save.resolve(true);
  assert.equal(await saving, true);
  assert.equal(gate.acceptsProxy(gate.beginProxy()), true);
  assert.equal(gate.acceptsClearance(gate.beginClearance("https://example.test/b"), "https://example.test/b"), true);
});

test("proxy and clearance tests have independent latest owners", () => {
  const gate = createProxyRuntimeRequestGate();
  gate.activate();

  const proxyRequest = gate.beginProxy();
  const clearanceA = gate.beginClearance("https://example.test/a");
  const clearanceB = gate.beginClearance("https://example.test/b");

  assert.equal(gate.acceptsProxy(proxyRequest), true);
  assert.equal(gate.acceptsClearance(clearanceA, "https://example.test/a"), false);
  assert.equal(gate.acceptsClearance(clearanceB, "https://example.test/b"), true);
});

test("invalidating a clearance candidate drops old callbacks but permits a new target", () => {
  const gate = createProxyRuntimeRequestGate();
  gate.activate();

  const first = gate.beginClearance("https://example.test/a");
  gate.invalidateClearance();
  const second = gate.beginClearance("https://example.test/b");
  const effects = [];

  if (gate.acceptsClearance(first, "https://example.test/a")) effects.push("old-result");
  if (gate.acceptsClearance(first, "https://example.test/a")) effects.push("old-finally");
  if (gate.acceptsClearance(second, "https://example.test/b")) effects.push("new-result");

  assert.deepEqual(effects, ["new-result"]);
});

test("component cancellation rejects late proxy and clearance callbacks", () => {
  const gate = createProxyRuntimeRequestGate();
  gate.activate();

  const proxyRequest = gate.beginProxy();
  const clearanceRequest = gate.beginClearance("https://example.test/a");
  gate.cancel();

  assert.equal(gate.acceptsProxy(proxyRequest), false);
  assert.equal(gate.acceptsClearance(clearanceRequest, "https://example.test/a"), false);
});

test("an invalidated proxy test does not start its follow-up request after save settles", async () => {
  const gate = createProxyRuntimeRequestGate();
  gate.activate();
  const request = gate.beginProxy();
  const save = deferred();
  let followupCalls = 0;

  const result = runCurrentProxyFollowup(
    () => save.promise,
    () => gate.acceptsProxy(request),
    async () => {
      followupCalls += 1;
      return "proxy-result";
    },
  );
  gate.invalidateProxy();
  save.resolve(true);

  assert.deepEqual(await result, { started: false });
  assert.equal(followupCalls, 0);
});

test("an unmounted clearance test does not start its follow-up request after save settles", async () => {
  const gate = createProxyRuntimeRequestGate();
  gate.activate();
  const candidate = "https://example.test/a";
  const request = gate.beginClearance(candidate);
  const save = deferred();
  let followupCalls = 0;

  const result = runCurrentProxyFollowup(
    () => save.promise,
    () => gate.acceptsClearance(request, candidate),
    async () => {
      followupCalls += 1;
      return "clearance-result";
    },
  );
  gate.cancel();
  save.resolve(true);

  assert.deepEqual(await result, { started: false });
  assert.equal(followupCalls, 0);
});
