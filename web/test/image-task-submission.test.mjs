import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { settleImageTaskSubmissions } from "../src/lib/image-task-submission.js";

const imagePageSource = readFileSync(
  new URL("../src/app/image/page.tsx", import.meta.url),
  "utf8",
);

test("one rejected image task does not discard sibling successes", async () => {
  const images = [
    { id: "image-a", taskId: "task-a" },
    { id: "image-b", taskId: "task-b" },
    { id: "image-c", taskId: "task-c" },
  ];

  const result = await settleImageTaskSubmissions(images, async (image) => {
    if (image.taskId === "task-b") throw new Error("task-b rejected");
    return { id: image.taskId, status: "queued" };
  });

  assert.deepEqual(result.tasks, [
    { id: "task-a", status: "queued" },
    { id: "task-c", status: "queued" },
  ]);
  assert.deepEqual(result.failures.map(({ imageId, message }) => ({ imageId, message })), [
    { imageId: "image-b", message: "task-b rejected" },
  ]);
});

test("non-Error rejection has a fixed actionable fallback", async () => {
  const result = await settleImageTaskSubmissions(
    [{ id: "image-a", taskId: "task-a" }],
    async () => Promise.reject(null),
  );

  assert.deepEqual(result.tasks, []);
  assert.deepEqual(result.failures, [
    { imageId: "image-a", error: null, message: "图片任务提交失败" },
  ]);
});

test("image queue uses settled submissions for both initial create and missing-task retry", () => {
  const calls = imagePageSource.match(/settleImageTaskSubmissions\(/g) || [];
  assert.equal(calls.length, 2);
  assert.doesNotMatch(imagePageSource, /const submitted = await Promise\.all\(/);
  assert.doesNotMatch(imagePageSource, /const resubmitted = await Promise\.all\(/);
});
