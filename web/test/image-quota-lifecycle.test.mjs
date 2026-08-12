import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createLatestActionOwner } from "../src/lib/latest-action-owner.js";

const source = readFileSync(
  fileURLToPath(new URL("../src/app/image/page.tsx", import.meta.url)),
  "utf8",
);

test("image quota loads use a cancellable latest owner", () => {
  assert.match(source, /createLatestActionOwner/);
  assert.match(source, /quotaOwnerRef/);
  assert.match(source, /quotaOwner\.cancel\(\)/);
  assert.match(source, /quotaOwner\.accepts\(requestOwner\)/);
});

test("a stale quota response cannot overwrite a newer quota", () => {
  const owner = createLatestActionOwner();
  const first = owner.begin();
  let quota = "current";

  const second = owner.begin();
  if (owner.accepts(first)) quota = "stale";
  assert.equal(quota, "current");
  if (owner.accepts(second)) quota = "latest";
  assert.equal(quota, "latest");
});

test("an unmounted quota load cannot publish a result or finish state", () => {
  const owner = createLatestActionOwner();
  const requestOwner = owner.begin();
  const events = [];

  owner.cancel();
  if (owner.accepts(requestOwner)) events.push("quota");
  if (owner.accepts(requestOwner)) events.push("error");
  if (owner.accepts(requestOwner)) events.push("finally");

  assert.deepEqual(events, []);
});
