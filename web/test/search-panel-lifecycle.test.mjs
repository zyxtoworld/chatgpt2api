import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createLatestActionOwner } from "../src/lib/latest-action-owner.js";

const source = readFileSync(
  fileURLToPath(new URL("../src/app/debug/components/search-panel.tsx", import.meta.url)),
  "utf8",
);

test("SearchPanel owns the search request and cancels it on unmount", () => {
  assert.match(source, /createLatestActionOwner/);
  assert.match(source, /searchOwnerRef/);
  assert.match(source, /searchOwner\.cancel\(\)/);
  assert.match(source, /searchOwner\.accepts\(requestOwner\)/);
});

test("a late search completion after unmount publishes no success, error, or finally state", () => {
  const owner = createLatestActionOwner();
  const requestOwner = owner.begin();
  const events = [];

  owner.cancel();
  if (owner.accepts(requestOwner)) events.push("success");
  if (owner.accepts(requestOwner)) events.push("error");
  if (owner.accepts(requestOwner)) events.push("finally");

  assert.deepEqual(events, []);
});

test("a replaced search cannot overwrite the current search result", () => {
  const owner = createLatestActionOwner();
  const first = owner.begin();
  let result = "current";

  owner.invalidate();
  if (owner.accepts(first)) result = "stale";
  assert.equal(result, "current");

  const second = owner.begin();
  if (owner.accepts(second)) result = "latest";
  assert.equal(result, "latest");
});
