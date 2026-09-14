import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createLatestActionOwner } from "../src/lib/latest-action-owner.js";

const source = readFileSync(
  fileURLToPath(new URL("../src/app/settings/components/config-card.tsx", import.meta.url)),
  "utf8",
);

test("ConfigCard proxy tests are owned by the candidate and component lifetime", () => {
  assert.match(source, /createLatestActionOwner/);
  assert.match(source, /proxyTestOwnerRef/);
  assert.match(source, /proxyTestOwner\.cancel\(\)/);
  assert.match(source, /proxyTestOwner\.accepts\(testOwner, candidate\)/);
  assert.match(source, /proxyTestOwnerRef\.current\.invalidate\(\);\s*setIsTestingProxy\(false\)/);
});

test("a changed candidate drops the old result and permits the new test", () => {
  const owner = createLatestActionOwner();
  owner.activate();
  const first = owner.begin("proxy-a");
  owner.invalidate();
  const second = owner.begin("proxy-b");
  const events = [];

  if (owner.accepts(first, "proxy-a")) events.push("old-result");
  if (owner.accepts(first, "proxy-a")) events.push("old-finally");
  if (owner.accepts(second, "proxy-b")) events.push("new-result");

  assert.deepEqual(events, ["new-result"]);
});

test("an unmounted ConfigCard drops late proxy success, error, and finally effects", () => {
  const owner = createLatestActionOwner();
  owner.activate();
  const request = owner.begin("proxy-a");
  owner.cancel();
  const events = [];

  if (owner.accepts(request, "proxy-a")) events.push("result");
  if (owner.accepts(request, "proxy-a")) events.push("error");
  if (owner.accepts(request, "proxy-a")) events.push("finally");

  assert.deepEqual(events, []);
});
