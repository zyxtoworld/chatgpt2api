import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { loadManagedImagesWithTags } from "../src/lib/image-manager-load.js";

const imageManagerSource = readFileSync(
  new URL("../src/app/image-manager/page.tsx", import.meta.url),
  "utf8",
);

test("a failed auxiliary tag request does not hide a valid image list", async () => {
  const data = {
    items: [
      { rel: "a.png", tags: ["red", "fruit"] },
      { rel: "b.png", tags: ["fruit", "green"] },
    ],
  };

  const result = await loadManagedImagesWithTags(
    async () => data,
    async () => Promise.reject(new Error("tags unavailable")),
  );

  assert.equal(result.data, data);
  assert.deepEqual(result.tags, ["red", "fruit", "green"]);
});

test("the authoritative tag response is used when it succeeds", async () => {
  const result = await loadManagedImagesWithTags(
    async () => ({ items: [{ rel: "a.png", tags: ["derived"] }] }),
    async () => ({ tags: ["server", "ordered"] }),
  );

  assert.deepEqual(result.tags, ["server", "ordered"]);
});

test("image and tag loaders share a child cancellation signal", async () => {
  const parentController = new AbortController();
  let imageSignal;
  let tagSignal;
  await loadManagedImagesWithTags(
    async (receivedSignal) => {
      imageSignal = receivedSignal;
      return { items: [] };
    },
    async (receivedSignal) => {
      tagSignal = receivedSignal;
      return { tags: [] };
    },
    parentController.signal,
  );
  assert.equal(imageSignal, tagSignal);
  assert.notEqual(imageSignal, parentController.signal);

  const pendingParentController = new AbortController();
  let pendingSignal;
  const pending = loadManagedImagesWithTags(
    (receivedSignal) => {
      pendingSignal = receivedSignal;
      return new Promise(() => {});
    },
    (receivedSignal) => new Promise(() => {
      assert.equal(receivedSignal, pendingSignal);
    }),
    pendingParentController.signal,
  );
  await new Promise((resolve) => queueMicrotask(resolve));
  pendingParentController.abort();
  assert.equal(pendingSignal.aborted, true);
  await assert.rejects(pending, (error) => error?.name === "AbortError");
});

test("a failed image request cancels the already-started tag request", async () => {
  let rejectImages;
  let tagSignal;
  let tagStarted;
  const tagStartedPromise = new Promise((resolve) => { tagStarted = resolve; });
  const loading = loadManagedImagesWithTags(
    () => new Promise((_, reject) => { rejectImages = reject; }),
    async (signal) => {
      tagSignal = signal;
      tagStarted();
      return new Promise(() => {});
    },
  );
  await tagStartedPromise;
  rejectImages(new Error("images unavailable"));
  await assert.rejects(loading, /images unavailable/);
  assert.equal(tagSignal.aborted, true);
});

test("a failed image list request still rejects the load", async () => {
  await assert.rejects(
    loadManagedImagesWithTags(
      async () => Promise.reject(new Error("images unavailable")),
      async () => ({ tags: ["server"] }),
    ),
    /images unavailable/,
  );
});

test("parent cancellation after images finish does not publish fallback tags", async () => {
  const parentController = new AbortController();
  let rejectTags;
  const loading = loadManagedImagesWithTags(
    async () => ({ items: [{ rel: "a.png", tags: ["derived"] }] }),
    (signal) => new Promise((_, reject) => {
      rejectTags = reject;
      signal.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
    }),
    parentController.signal,
  );

  await new Promise((resolve) => queueMicrotask(resolve));
  parentController.abort();
  rejectTags(new DOMException("aborted", "AbortError"));
  await assert.rejects(loading, (error) => error?.name === "AbortError");
});

test("parent cancellation does not wait forever for a tag loader that ignores abort", async () => {
  const parentController = new AbortController();
  let tagStarted;
  const tagStartedPromise = new Promise((resolve) => { tagStarted = resolve; });
  const loading = loadManagedImagesWithTags(
    async () => ({ items: [] }),
    () => {
      tagStarted();
      return new Promise(() => {});
    },
    parentController.signal,
  );

  await tagStartedPromise;
  parentController.abort();
  await Promise.race([
    assert.rejects(loading, (error) => error?.name === "AbortError"),
    new Promise((_, reject) => setTimeout(() => reject(new Error("abort timed out")), 100)),
  ]);
});

test("parent cancellation does not wait forever for an image loader that ignores abort", async () => {
  const parentController = new AbortController();
  let imageStarted;
  const imageStartedPromise = new Promise((resolve) => { imageStarted = resolve; });
  const loading = loadManagedImagesWithTags(
    () => {
      imageStarted();
      return new Promise(() => {});
    },
    async () => ({ tags: [] }),
    parentController.signal,
  );

  await imageStartedPromise;
  parentController.abort();
  await Promise.race([
    assert.rejects(loading, (error) => error?.name === "AbortError"),
    new Promise((_, reject) => setTimeout(() => reject(new Error("abort timed out")), 100)),
  ]);
});

test("an already-aborted parent rejects before either loader starts", async () => {
  const parentController = new AbortController();
  parentController.abort();
  let imageCalls = 0;
  let tagCalls = 0;

  await assert.rejects(
    loadManagedImagesWithTags(
      async () => { imageCalls += 1; return { items: [] }; },
      async () => { tagCalls += 1; return { tags: [] }; },
      parentController.signal,
    ),
    (error) => error?.name === "AbortError",
  );
  assert.equal(imageCalls, 0);
  assert.equal(tagCalls, 0);
});

test("the image manager production entry uses the auxiliary-failure fallback", () => {
  assert.match(imageManagerSource, /loadManagedImagesWithTags/);
  assert.doesNotMatch(imageManagerSource, /const \[data, tagsData\] = await Promise\.all\(/);
});
