import assert from "node:assert/strict";
import test from "node:test";

import { createProxySettingsWriteGate } from "../src/lib/proxy-settings-write-gate.js";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const source = readFileSync(
  fileURLToPath(new URL("../src/app/settings/components/proxy-settings.tsx", import.meta.url)),
  "utf8",
);

test("a remounted proxy settings card cannot start a second write before the first settles", () => {
  assert.match(source, /proxySettingsWriteGate\.begin\(\)/);
  assert.match(source, /proxySettingsWriteGate\.finish\(writeOwner\)/);
  assert.doesNotMatch(source, /proxySettingsWriteGate\.cancel\(\)/);
  const gate = createProxySettingsWriteGate();
  const first = gate.begin();
  assert.equal(first.accepted, true);

  const remounted = gate.begin();
  assert.equal(remounted.accepted, false);
  assert.equal(gate.accepts(first), true);
  assert.equal(gate.finish(first), true);

  const third = gate.begin();
  assert.equal(third.accepted, true);
  assert.equal(gate.finish(third), true);
});
