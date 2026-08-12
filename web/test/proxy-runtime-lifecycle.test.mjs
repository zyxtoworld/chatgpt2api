import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createProxyRuntimeRequestGate } from "../src/lib/proxy-runtime-request-gate.js";

const source = readFileSync(
  fileURLToPath(new URL("../src/app/settings/components/proxy-runtime-card.tsx", import.meta.url)),
  "utf8",
);

test("ProxyRuntimeCard wires independent owners through mount cleanup and guarded effects", () => {
  assert.match(source, /createProxyRuntimeRequestGate/);
  assert.match(source, /return \(\) => requestGate\.cancel\(\)/);
  assert.match(source, /requestGate\.acceptsProxy\(request\)/);
  assert.match(source, /requestGate\.acceptsClearance\(request, candidate\)/);
  assert.match(source, /invalidateClearance\(\)/);
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
