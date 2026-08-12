import assert from "node:assert/strict";
import test from "node:test";

import { mergeDeletedEditableFileIds } from "../src/lib/editable-file-history-state.js";

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
