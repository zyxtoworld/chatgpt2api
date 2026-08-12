import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createLatestActionOwner } from "../src/lib/latest-action-owner.js";

const source = readFileSync(
  fileURLToPath(new URL("../src/hooks/use-version-check.ts", import.meta.url)),
  "utf8",
);

test("version checks use one latest owner and cancel it on cleanup", () => {
  assert.match(source, /createLatestActionOwner/);
  assert.match(source, /releaseCheckOwnerRef/);
  assert.match(source, /releaseCheckOwner\.cancel\(\)/);
  assert.match(source, /releaseCheckOwner\.accepts\(requestOwner\)/);
});

test("a replaced version check cannot publish stale state or clear the latest loading owner", () => {
  const owner = createLatestActionOwner();
  const first = owner.begin();
  let version = "current";
  let checking = true;

  const second = owner.begin();
  if (owner.accepts(first)) {
    version = "stale";
    checking = false;
  }
  assert.equal(version, "current");
  assert.equal(checking, true);

  if (owner.accepts(second)) {
    version = "latest";
    checking = false;
  }
  assert.equal(version, "latest");
  assert.equal(checking, false);
});

test("an unmounted version check publishes no state or toast", () => {
  const owner = createLatestActionOwner();
  const requestOwner = owner.begin();
  const events = [];

  owner.cancel();
  if (owner.accepts(requestOwner)) events.push("state");
  if (owner.accepts(requestOwner)) events.push("toast");
  if (owner.accepts(requestOwner)) events.push("finally");

  assert.deepEqual(events, []);
});
