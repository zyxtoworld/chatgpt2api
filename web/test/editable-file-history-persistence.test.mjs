import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { createEditableFileHistoryWriteCoordinator } from "../src/lib/editable-file-history-write-coordinator.js";

const storeSource = readFileSync(new URL("../src/store/editable-file-history.ts", import.meta.url), "utf8");

function deferred() {
  let resolve;
  const promise = new Promise((nextResolve) => {
    resolve = nextResolve;
  });
  return { promise, resolve };
}

test("editable history writes are serialized across draft and deletion snapshots", async () => {
  const coordinator = createEditableFileHistoryWriteCoordinator();
  const first = deferred();
  const events = [];

  const firstWrite = coordinator.enqueue(async () => {
    events.push("first:start");
    await first.promise;
    events.push("first:end");
  });
  const secondWrite = coordinator.enqueue(async () => {
    events.push("second:start");
  });

  await Promise.resolve();
  await Promise.resolve();
  assert.deepEqual(events, ["first:start"]);

  first.resolve();
  await firstWrite;
  await secondWrite;
  assert.deepEqual(events, ["first:start", "first:end", "second:start"]);
});

test("all editable history snapshot writes use the shared coordinator", () => {
  assert.match(storeSource, /createEditableFileHistoryWriteCoordinator/);
  assert.match(storeSource, /saveEditableFileDrafts[\s\S]*?queueEditableFileHistoryWrite\(/);
  assert.match(storeSource, /saveDeletedEditableFileIds[\s\S]*?queueEditableFileHistoryWrite\(/);
});
