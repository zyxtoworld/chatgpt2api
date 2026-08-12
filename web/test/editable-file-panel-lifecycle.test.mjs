import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

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
