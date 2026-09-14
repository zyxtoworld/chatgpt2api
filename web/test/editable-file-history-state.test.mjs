import assert from "node:assert/strict";
import test from "node:test";

import {
  mergeDeletedEditableFileIds,
  resolveDeletedEditableFileIds,
} from "../src/lib/editable-file-history-state.js";

test("rapid delete updates preserve every deleted task id", () => {
  let deleted = new Set();
  deleted = mergeDeletedEditableFileIds(deleted, ["task-a"]);
  deleted = mergeDeletedEditableFileIds(deleted, ["task-b"]);

  assert.deepEqual([...deleted].sort(), ["task-a", "task-b"]);
});

test("clear history merges with the current deleted set", () => {
  const current = new Set(["task-a"]);
  const next = mergeDeletedEditableFileIds(current, ["task-b", "task-c"]);

  assert.deepEqual([...next].sort(), ["task-a", "task-b", "task-c"]);
  assert.deepEqual([...current], ["task-a"]);
});

test("a failed optional history read preserves current hidden ids without blocking tasks", async () => {
  const current = new Set(["task-a"]);

  const result = await resolveDeletedEditableFileIds(current, async () => {
    throw new Error("indexeddb unavailable");
  });

  assert.deepEqual([...result.ids], ["task-a"]);
  assert.equal(result.storageFailed, true);
});

test("a successful optional history read merges durable and in-memory hidden ids", async () => {
  const result = await resolveDeletedEditableFileIds(new Set(["task-a"]), async () => new Set(["task-b"]));

  assert.deepEqual([...result.ids].sort(), ["task-a", "task-b"]);
  assert.equal(result.storageFailed, false);
});
