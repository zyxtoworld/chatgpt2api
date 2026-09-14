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

test("editing the query invalidates the pending search before a new search starts", () => {
  const owner = createLatestActionOwner();
  const first = owner.begin("query-a");
  let loading = true;
  const events = [];

  owner.invalidate();
  loading = false;

  const second = owner.begin("query-b");
  loading = true;
  if (owner.accepts(first, "query-a")) events.push("stale-success");
  if (owner.accepts(first, "query-a")) loading = false;
  if (owner.accepts(second, "query-b")) events.push("current-success");

  assert.deepEqual(events, ["current-success"]);
  assert.equal(loading, true);
});

test("SearchPanel wires prompt edits to pending-request invalidation", () => {
  const handlerStart = source.indexOf("const handlePromptChange");
  const handlerEnd = source.indexOf("const runSearch", handlerStart);
  assert.ok(handlerStart >= 0, "missing prompt change handler");
  assert.ok(handlerEnd > handlerStart, "missing prompt change handler boundary");

  const handler = source.slice(handlerStart, handlerEnd);
  const invalidateIndex = handler.indexOf("searchOwnerRef.current.invalidate()");
  const clearLoadingIndex = handler.indexOf("setLoading(false)");
  assert.ok(invalidateIndex >= 0, "prompt edits must invalidate the active search");
  assert.ok(clearLoadingIndex > invalidateIndex, "prompt edits must release the invalidated search busy state");
  assert.match(source, /onChange=\{\(event\) => handlePromptChange\(event\.target\.value\)\}/);
});
