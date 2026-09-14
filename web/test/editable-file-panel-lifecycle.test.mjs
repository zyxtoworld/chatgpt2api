import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import {
  createLifecycleActionOwner,
  observeLifecycleAction,
} from "../src/lib/lifecycle-action-owner.js";
import { createLatestActionOwner } from "../src/lib/latest-action-owner.js";

const source = readFileSync(
  fileURLToPath(new URL("../src/app/debug/components/editable-file-panel.tsx", import.meta.url)),
  "utf8",
);

test("EditableFilePanel owns submit and image-read effects for its lifetime", () => {
  assert.match(source, /submitOwnerRef/);
  assert.match(source, /imageReadOwnerRef/);
  assert.match(source, /submitOwner\.cancel\(\)/);
  assert.match(source, /imageReadOwner\.cancel\(\)/);
  assert.match(source, /submitOwner\.accepts\(requestOwner(?:,\s*kind)?\)/);
  assert.match(source, /imageReadOwner\.accepts\(readOwner\)/);
});

test("a late editable-file submit cannot publish after the panel is unmounted", () => {
  const owner = createLatestActionOwner();
  const requestOwner = owner.begin("ppt");
  const events = [];

  owner.cancel();
  if (owner.accepts(requestOwner)) events.push("task", "draft", "selection");
  if (owner.accepts(requestOwner)) events.push("error");
  if (owner.accepts(requestOwner)) events.push("finally");

  assert.deepEqual(events, []);
});

test("switching submit or clearing images invalidates only the old action", () => {
  const submitOwner = createLatestActionOwner();
  const firstSubmit = submitOwner.begin("ppt");
  submitOwner.invalidate();
  const secondSubmit = submitOwner.begin("ppt");
  assert.equal(submitOwner.accepts(firstSubmit), false);
  assert.equal(submitOwner.accepts(secondSubmit), true);

  const imageOwner = createLatestActionOwner();
  const firstRead = imageOwner.begin();
  imageOwner.invalidate();
  assert.equal(imageOwner.accepts(firstRead), false);
  const secondRead = imageOwner.begin();
  assert.equal(imageOwner.accepts(secondRead), true);
});

test("concurrent editable image reads share an epoch until clear or unmount", () => {
  const imageOwner = createLifecycleActionOwner();
  const firstRead = imageOwner.begin();
  const secondRead = imageOwner.begin();

  assert.equal(imageOwner.accepts(firstRead), true);
  assert.equal(imageOwner.accepts(secondRead), true);

  imageOwner.invalidate();
  assert.equal(imageOwner.accepts(firstRead), false);
  assert.equal(imageOwner.accepts(secondRead), false);

  const afterClear = imageOwner.begin();
  assert.equal(imageOwner.accepts(afterClear), true);
  imageOwner.cancel();
  assert.equal(imageOwner.accepts(afterClear), false);
});

test("owned persistence failures are consumed and reported while the panel is active", async () => {
  const owner = createLifecycleActionOwner();
  const failures = [];

  await observeLifecycleAction(
    owner,
    async () => {
      throw new Error("indexeddb write failed");
    },
    {
      onError: () => failures.push("本地历史记录保存失败"),
    },
  );

  assert.deepEqual(failures, ["本地历史记录保存失败"]);
});

test("an unmounted panel drops a late persistence failure without rejecting", async () => {
  const owner = createLifecycleActionOwner();
  const failures = [];
  let rejectWrite;
  const write = new Promise((_, reject) => {
    rejectWrite = reject;
  });

  const observed = observeLifecycleAction(owner, () => write, {
    onError: () => failures.push("stale error"),
  });
  owner.cancel();
  rejectWrite(new Error("late indexeddb failure"));

  await observed;
  assert.deepEqual(failures, []);
});

test("EditableFilePanel observes every fire-and-forget history operation", () => {
  assert.match(source, /const historyPersistenceOwnerRef = useRef\(createLifecycleActionOwner\(\)\)/);
  assert.match(source, /observeLifecycleAction\([\s\S]*?listEditableFileDrafts\(kind\)/);
  assert.match(source, /observeLifecycleAction\([\s\S]*?listDeletedEditableFileIds\(kind\)/);
  assert.match(source, /observeLifecycleAction\([\s\S]*?saveEditableFileDrafts\(kind, next\)/);
  assert.match(source, /observeLifecycleAction\([\s\S]*?saveDeletedEditableFileIds\(kind, nextDeleted\)/);
  assert.doesNotMatch(source, /void save(?:EditableFileDrafts|DeletedEditableFileIds)\(/);
});

test("an optional deleted-id read failure cannot discard a successful task response", () => {
  assert.match(source, /resolveDeletedEditableFileIds\(/);
  assert.match(source, /storageFailed[\s\S]*?setError\("本地历史记录读取失败"\)[\s\S]*?setTasks\(/);
});

test("EditableFilePanel wires image reads to the shared lifecycle epoch", () => {
  assert.match(source, /const imageReadOwnerRef = useRef\(createLifecycleActionOwner\(\)\)/);
  assert.match(source, /imageReadOwnerRef\.current\.invalidate\(\)/);
  assert.match(source, /imageReadOwner\.cancel\(\)/);
});

test("stale image reads cannot clear the pending count of a newer epoch", () => {
  const imageOwner = createLifecycleActionOwner();
  let pending = 0;
  const beginRead = () => {
    const action = imageOwner.begin();
    pending += 1;
    return action;
  };
  const settleRead = (action) => {
    if (imageOwner.accepts(action)) pending = Math.max(0, pending - 1);
  };

  const first = beginRead();
  const second = beginRead();
  assert.equal(pending, 2);

  imageOwner.invalidate();
  pending = 0;
  const third = beginRead();
  settleRead(first);
  settleRead(second);
  assert.equal(pending, 1);
  settleRead(third);
  assert.equal(pending, 0);
});

test("EditableFilePanel blocks generation until every selected image is read", () => {
  assert.match(source, /const \[pendingImageReads, setPendingImageReads\] = useState\(0\)/);
  assert.match(source, /pendingImageReadsRef\.current \+= 1/);
  assert.match(source, /setPendingImageReads\(pendingImageReadsRef\.current\)/);
  assert.match(source, /if \(pendingImageReads > 0\)/);
  assert.match(source, /disabled=\{submitting \|\| running \|\| pendingImageReads > 0\}/);
});

test("a stale task fetch cannot clear a newer polling owner", () => {
  let requestGeneration = 0;
  let polling = false;
  const beginFetch = () => {
    const requestId = ++requestGeneration;
    polling = true;
    return {
      invalidate() {
        requestGeneration += 1;
        polling = false;
      },
      settle() {
        if (requestId === requestGeneration) polling = false;
      },
    };
  };

  const first = beginFetch();
  first.invalidate();
  assert.equal(polling, false);

  const second = beginFetch();
  assert.equal(polling, true);
  first.settle();
  assert.equal(polling, true, "the old fetch must not clear the new fetch busy state");
  second.settle();
  assert.equal(polling, false);
});

test("clearing history releases the old submit owner without letting it clear a new submit", () => {
  const owner = createLatestActionOwner();
  let submitting = false;
  const beginSubmit = () => {
    const action = owner.begin("ppt");
    submitting = true;
    return action;
  };
  const clearHistory = () => {
    owner.invalidate();
    submitting = false;
  };
  const settleSubmit = (action) => {
    if (owner.accepts(action, "ppt")) submitting = false;
  };

  const first = beginSubmit();
  clearHistory();
  assert.equal(submitting, false);
  const second = beginSubmit();
  settleSubmit(first);
  assert.equal(submitting, true, "the old submit must not clear the new submit busy state");
  settleSubmit(second);
  assert.equal(submitting, false);
});

test("destructive panel actions clear their own busy state in the production wiring", () => {
  assert.match(source, /taskFetchRequestRef\.current \+= 1;[\s\S]*?setPolling\(false\)/);
  assert.match(source, /submitOwnerRef\.current\.invalidate\(\);[\s\S]*?setPolling\(false\);[\s\S]*?setSubmitting\(false\)/);
});
