import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createLifecycleActionOwner } from "../src/lib/lifecycle-action-owner.js";
import { createLatestActionOwner } from "../src/lib/latest-action-owner.js";

const source = readFileSync(
  fileURLToPath(new URL("../src/app/settings/components/backup-settings-card.tsx", import.meta.url)),
  "utf8",
);

test("backup detail reads have a latest owner and cancel on unmount", () => {
  assert.match(source, /createLatestActionOwner/);
  assert.match(source, /backupDetailOwnerRef/);
  assert.match(source, /backupDetailOwner\.cancel\(\)/);
  assert.match(source, /backupDetailOwner\.accepts\(requestOwner\)/);
});

test("a late backup detail response cannot overwrite the latest selection", async () => {
  const owner = createLatestActionOwner();
  const pending = new Map();
  const events = [];

  const load = async (key) => {
    const requestOwner = owner.begin(key);
    events.push(`loading:${key}`);
    const item = await new Promise((resolve) => pending.set(key, resolve));
    if (owner.accepts(requestOwner)) events.push(`detail:${item}`);
    if (owner.accepts(requestOwner)) events.push(`finally:${key}`);
  };

  const first = load("A");
  const second = load("B");
  pending.get("A")("A-detail");
  await new Promise((resolve) => queueMicrotask(resolve));
  assert.deepEqual(events, ["loading:A", "loading:B"]);

  pending.get("B")("B-detail");
  await Promise.all([first, second]);
  assert.deepEqual(events, ["loading:A", "loading:B", "detail:B-detail", "finally:B"]);
});

test("an unmounted backup detail response publishes no detail, error, or finally state", async () => {
  const owner = createLatestActionOwner();
  const events = [];
  const requestOwner = owner.begin("A");

  owner.cancel();
  if (owner.accepts(requestOwner)) events.push("detail");
  if (owner.accepts(requestOwner)) events.push("error");
  if (owner.accepts(requestOwner)) events.push("finally");

  assert.deepEqual(events, []);
});

test("backup downloads share a lifecycle owner and reject late completion after unmount", () => {
  assert.match(source, /createLifecycleActionOwner/);
  assert.match(source, /backupDownloadOwnerRef/);
  assert.match(source, /backupDownloadOwner\.cancel\(\)/);
  assert.match(source, /backupDownloadOwner\.accepts\(downloadOwner\)/);

  const owner = createLifecycleActionOwner();
  owner.activate();
  const first = owner.begin();
  const second = owner.begin();
  const events = [];

  if (owner.accepts(first)) events.push("first-download");
  if (owner.accepts(second)) events.push("second-download");
  assert.deepEqual(events, ["first-download", "second-download"]);

  owner.cancel();
  assert.equal(owner.accepts(first), false);
  assert.equal(owner.accepts(second), false);
});
