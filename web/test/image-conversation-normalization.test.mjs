import assert from "node:assert/strict";
import { register } from "node:module";
import test from "node:test";

register("./auth-store-loader.mjs", { parentURL: import.meta.url });

const [{ listImageConversations }, { reset }] = await Promise.all([
  import("../src/store/image-conversations.ts"),
  import("./fixtures/localforage-mock.mjs"),
]);

function storedConversation(image) {
  return {
    id: "conversation-1",
    title: "test",
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    turns: [{
      id: "turn-1",
      prompt: "test",
      model: "gpt-image-2",
      mode: "generate",
      referenceImages: [],
      count: 1,
      size: "1024x1024",
      ratio: "1:1",
      tier: "1k",
      quality: "auto",
      images: [image],
      createdAt: "2026-01-01T00:00:00.000Z",
      status: "success",
    }],
  };
}

test("persisted image normalization drops malformed render fields before they reach the UI", async () => {
  reset({
    items: [storedConversation({
      id: "image-1",
      status: "success",
      url: "/images/fallback.png",
      b64_json: { malformed: true },
      error: { opaque: "not-renderable" },
      progress: ["not", "renderable"],
      revised_prompt: 42,
      startTime: "1",
      elapsedSecs: Number.NaN,
      elapsedUpdatedAt: Number.POSITIVE_INFINITY,
      durationMs: -1,
    })],
  });

  const [conversation] = await listImageConversations();
  const [image] = conversation.turns[0].images;

  assert.equal(image.url, "/images/fallback.png");
  assert.equal(image.b64_json, undefined);
  assert.equal(image.error, undefined);
  assert.equal(image.progress, undefined);
  assert.equal(image.revised_prompt, undefined);
  assert.equal(image.startTime, undefined);
  assert.equal(image.elapsedSecs, undefined);
  assert.equal(image.elapsedUpdatedAt, undefined);
  assert.equal(image.durationMs, undefined);
});

test("persisted image normalization preserves canonical render fields", async () => {
  reset({
    items: [storedConversation({
      id: "image-2",
      taskId: "task-2",
      status: "error",
      taskStatus: "running",
      progress: "generating",
      b64_json: "aGVsbG8=",
      revised_prompt: "safe prompt",
      error: "safe error",
      startTime: 1,
      elapsedSecs: 2.5,
      elapsedUpdatedAt: 3,
      durationMs: 4,
    })],
  });

  const [conversation] = await listImageConversations();
  assert.deepEqual(conversation.turns[0].images[0], {
    id: "image-2",
    taskId: "task-2",
    status: "error",
    taskStatus: "running",
    progress: "generating",
    b64_json: "aGVsbG8=",
    url: undefined,
    revised_prompt: "safe prompt",
    error: "safe error",
    startTime: 1,
    elapsedSecs: 2.5,
    elapsedUpdatedAt: 3,
    durationMs: 4,
  });
});
