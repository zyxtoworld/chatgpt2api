import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { createEditableFileHistoryWriteCoordinator } from "../src/lib/editable-file-history-write-coordinator.js";
import { commitSynchronousSnapshot } from "../src/lib/synchronous-snapshot.js";

const storeSource = readFileSync(new URL("../src/store/editable-file-history.ts", import.meta.url), "utf8");
const panelSource = readFileSync(new URL("../src/app/debug/components/editable-file-panel.tsx", import.meta.url), "utf8");

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

test("editable draft snapshots compose synchronously before React publishes them", () => {
  const ref = { current: { old: { title: "old" } } };
  const first = commitSynchronousSnapshot(ref, (current) => ({
    ...current,
    first: { title: "first" },
  }));
  const second = commitSynchronousSnapshot(ref, (current) => ({
    ...current,
    second: { title: "second" },
  }));

  assert.deepEqual(first, {
    old: { title: "old" },
    first: { title: "first" },
  });
  assert.deepEqual(second, {
    old: { title: "old" },
    first: { title: "first" },
    second: { title: "second" },
  });
});

test("EditableFilePanel keeps draft persistence outside React state updaters", () => {
  assert.match(panelSource, /const draftsRef = useRef<Record<string, EditableFileDraft>>\(\{\}\)/);
  assert.match(panelSource, /commitSynchronousSnapshot\(draftsRef, next\)/);
  assert.doesNotMatch(panelSource, /setDrafts\(\(current\) => \{[\s\S]*?saveEditableFileDrafts/);
  assert.match(panelSource, /\{ \.\.\.nextDrafts, \.\.\.current \}/);
});
