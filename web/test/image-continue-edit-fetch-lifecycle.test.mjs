import assert from "node:assert/strict";
import test from "node:test";

import { fetchImageAsFile } from "../src/lib/image-download.js";

test("aborting an owned image fetch cancels the response body", async () => {
  const originalFetch = globalThis.fetch;
  const controller = new AbortController();
  let cancelled = false;
  let rejectBlob;
  const blobPromise = new Promise((_, reject) => {
    rejectBlob = reject;
  });
  const response = {
    ok: true,
    body: {
      cancel() {
        cancelled = true;
        rejectBlob(new DOMException("aborted", "AbortError"));
        return Promise.resolve();
      },
    },
    blob: () => blobPromise,
  };

  globalThis.fetch = async (_url, options) => {
    assert.equal(options.signal, controller.signal);
    return response;
  };

  try {
    const pending = fetchImageAsFile("https://example.test/image.png", "image.png", controller.signal);
    await Promise.resolve();
    controller.abort();
    await assert.rejects(pending, { name: "AbortError" });
    assert.equal(cancelled, true);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("an abort that wins before listener registration still cancels the response body", async () => {
  const originalFetch = globalThis.fetch;
  const controller = new AbortController();
  let resolveFetch;
  let rejectBlob;
  let cancelled = false;
  let blobStarted = false;
  const blobPromise = new Promise((_, reject) => {
    rejectBlob = reject;
  });
  const response = {
    ok: true,
    body: {
      cancel() {
        cancelled = true;
        if (blobStarted) {
          rejectBlob(new DOMException("aborted", "AbortError"));
        }
        return Promise.resolve();
      },
    },
    blob: () => {
      blobStarted = true;
      return blobPromise;
    },
  };

  globalThis.fetch = () => new Promise((resolve) => {
    resolveFetch = resolve;
  });

  let pending;
  try {
    pending = fetchImageAsFile("https://example.test/image.png", "image.png", controller.signal);
    resolveFetch(response);
    controller.abort();
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(cancelled, true);
  } finally {
    if (!cancelled) {
      rejectBlob(new DOMException("aborted", "AbortError"));
    }
    await pending?.catch(() => undefined);
    globalThis.fetch = originalFetch;
  }
});

test("an already-aborted response is not returned when body cancellation resolves cleanly", async () => {
  const originalFetch = globalThis.fetch;
  const controller = new AbortController();
  let cancelled = false;
  let blobCalls = 0;
  const response = {
    ok: true,
    body: {
      cancel() {
        cancelled = true;
        return Promise.resolve();
      },
    },
    blob: async () => {
      blobCalls += 1;
      return new Blob(["late-image"], { type: "image/png" });
    },
  };

  let resolveFetch;
  globalThis.fetch = () => new Promise((resolve) => {
    resolveFetch = resolve;
  });

  try {
    const pending = fetchImageAsFile("https://example.test/image.png", "image.png", controller.signal);
    resolveFetch(response);
    controller.abort();
    await assert.rejects(pending, { name: "AbortError" });
    assert.equal(cancelled, true);
    assert.equal(blobCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
