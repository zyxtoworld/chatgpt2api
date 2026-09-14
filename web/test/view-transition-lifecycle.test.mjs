import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  observeViewTransition,
  startObservedViewTransition,
} from "../src/lib/view-transition-lifecycle.js";

const themeToggleSource = readFileSync(
  new URL("../src/components/ui/animated-theme-toggler.tsx", import.meta.url),
  "utf8",
);

const deferred = () => {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
};

const flushPromises = () => new Promise((resolve) => setImmediate(resolve));

test("view transition rejection is handled and cleanup runs once", async () => {
  const ready = deferred();
  const finished = deferred();
  let readyCalls = 0;
  let cleanupCalls = 0;

  observeViewTransition(
    { ready: ready.promise, finished: finished.promise },
    {
      onReady: () => {
        readyCalls += 1;
      },
      onSettled: () => {
        cleanupCalls += 1;
      },
    },
  );

  ready.reject(new Error("view transition snapshot rejected"));
  finished.reject(new Error("view transition finished rejected"));
  await flushPromises();

  assert.equal(readyCalls, 0);
  assert.equal(cleanupCalls, 1);
});

test("view transition success animates once and settles once", async () => {
  const ready = deferred();
  const finished = deferred();
  let readyCalls = 0;
  let cleanupCalls = 0;

  observeViewTransition(
    { ready: ready.promise, finished: finished.promise },
    {
      onReady: () => {
        readyCalls += 1;
      },
      onSettled: () => {
        cleanupCalls += 1;
      },
    },
  );

  ready.resolve();
  await flushPromises();
  assert.equal(readyCalls, 1);
  assert.equal(cleanupCalls, 0);

  finished.resolve();
  await flushPromises();
  assert.equal(cleanupCalls, 1);
});

test("synchronous start failure cleans up and applies the theme fallback", () => {
  let cleanupCalls = 0;
  let applyCalls = 0;

  startObservedViewTransition(
    () => {
      throw new Error("another transition is active");
    },
    () => {
      applyCalls += 1;
    },
    {
      onReady: () => {},
      onSettled: () => {
        cleanupCalls += 1;
      },
    },
  );

  assert.equal(cleanupCalls, 1);
  assert.equal(applyCalls, 1);
});

test("theme toggle delegates rejected transition promises to the lifecycle observer", () => {
  assert.match(themeToggleSource, /startObservedViewTransition\(/);
  assert.doesNotMatch(themeToggleSource, /\.finished\.finally\(/);
  assert.doesNotMatch(themeToggleSource, /\bready\.then\(\(\s*\)\s*=>/);
});
