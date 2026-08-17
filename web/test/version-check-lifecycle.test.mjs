import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";
import vm from "node:vm";

import { createLatestActionOwner } from "../src/lib/latest-action-owner.js";
import { fetchReleaseText } from "../src/lib/version-release-fetch.js";

const source = readFileSync(
  fileURLToPath(new URL("../src/hooks/use-version-check.ts", import.meta.url)),
  "utf8",
);

test("malformed build-time release metadata falls back without breaking the hook", () => {
  const functionSource = source.match(
    /function readLocalReleases\(\): ReleaseInfo\[\] \{[\s\S]*?\n\}/,
  )?.[0];
  assert.ok(functionSource);
  const executable = functionSource
    .replace("(): ReleaseInfo[]", "()")
    .replace("const parsed: unknown", "const parsed")
    .replace(") as ReleaseInfo[];", ");");
  for (const raw of ["{broken-json", "{}", "null", "[null]", "[{\"version\":\"v1\"}]"]) {
    const readLocalReleases = vm.runInNewContext(`(${executable})`, {
      process: { env: { NEXT_PUBLIC_APP_RELEASES: raw } },
    });
    const releases = readLocalReleases();
    assert.equal(Array.isArray(releases), true);
    assert.equal(releases.length, 0);
  }
});

test("version checks use one latest owner and cancel it on cleanup", () => {
  assert.match(source, /createLatestActionOwner/);
  assert.match(source, /releaseCheckOwnerRef/);
  assert.match(source, /releaseCheckOwner\.cancel\(\)/);
  assert.match(source, /releaseCheckOwner\.accepts\(requestOwner\)/);
  assert.match(source, /fetchReleaseText/);
  assert.match(source, /new AbortController\(\)/);
  assert.match(source, /releaseAbortControllerRef\.current\?\.abort\(\)/);
});

test("a replaced version check cannot publish stale state or clear the latest loading owner", () => {
  const owner = createLatestActionOwner();
  const first = owner.begin();
  let version = "current";
  let checking = true;

  const second = owner.begin();
  if (owner.accepts(first)) {
    version = "stale";
    checking = false;
  }
  assert.equal(version, "current");
  assert.equal(checking, true);

  if (owner.accepts(second)) {
    version = "latest";
    checking = false;
  }
  assert.equal(version, "latest");
  assert.equal(checking, false);
});

test("an unmounted version check publishes no state or toast", () => {
  const owner = createLatestActionOwner();
  const requestOwner = owner.begin();
  const events = [];

  owner.cancel();
  if (owner.accepts(requestOwner)) events.push("state");
  if (owner.accepts(requestOwner)) events.push("toast");
  if (owner.accepts(requestOwner)) events.push("finally");

  assert.deepEqual(events, []);
});

function responseFromChunks(chunks, contentLength = null) {
  let index = 0;
  let cancelled = false;
  return {
    ok: true,
    headers: { get: (name) => (name.toLowerCase() === "content-length" ? contentLength : null) },
    body: {
      async cancel() {
        cancelled = true;
      },
      getReader() {
        return {
          async read() {
            if (index >= chunks.length) return { done: true, value: undefined };
            return { done: false, value: chunks[index++] };
          },
          async cancel() {
            cancelled = true;
          },
          releaseLock() {},
        };
      },
    },
    wasCancelled: () => cancelled,
  };
}

test("version response rejects an oversized declared body before reading it", async () => {
  const response = responseFromChunks([new Uint8Array([1, 2, 3])], "4");
  await assert.rejects(
    fetchReleaseText("https://example.test/VERSION", {
      maxBytes: 3,
      fetchImpl: async () => response,
    }),
    /response too large/,
  );
  assert.equal(response.wasCancelled(), true);
});

test("version response cancels a stream when it exceeds the byte budget", async () => {
  const response = responseFromChunks([new Uint8Array([65, 66]), new Uint8Array([67, 68])]);
  await assert.rejects(
    fetchReleaseText("https://example.test/CHANGELOG", {
      maxBytes: 3,
      fetchImpl: async () => response,
    }),
    /response too large/,
  );
  assert.equal(response.wasCancelled(), true);
});

test("aborting after fetch returns cancels the reader and preserves AbortError", async () => {
  const response = responseFromChunks([new Uint8Array([65])]);
  const controller = new AbortController();
  await assert.rejects(
    fetchReleaseText("https://example.test/CHANGELOG", {
      maxBytes: 3,
      signal: controller.signal,
      fetchImpl: async () => {
        controller.abort();
        return response;
      },
    }),
    (error) => error?.name === "AbortError",
  );
  assert.equal(response.wasCancelled(), true);
});

test("a reader failure still releases the response body", async () => {
  let cancelled = false;
  const response = {
    ok: true,
    headers: { get: () => null },
    body: {
      async cancel() {
        cancelled = true;
      },
      getReader() {
        return {
          async read() {
            throw new Error("stream failed");
          },
          async cancel() {
            cancelled = true;
          },
          releaseLock() {},
        };
      },
    },
  };
  await assert.rejects(
    fetchReleaseText("https://example.test/CHANGELOG", {
      maxBytes: 3,
      fetchImpl: async () => response,
    }),
    /stream failed/,
  );
  assert.equal(cancelled, true);
});
