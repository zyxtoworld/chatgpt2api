import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";

import { createReplaceableTimeout } from "../src/lib/replaceable-timeout.js";

const componentSource = readFileSync(
  new URL("../src/components/ui/dia-text-reveal.tsx", import.meta.url),
  "utf8",
);

function manualScheduler() {
  const pending = [];
  return {
    setTimer(callback) {
      pending.push(callback);
      return callback;
    },
    clearTimer(callback) {
      const index = pending.indexOf(callback);
      if (index >= 0) pending.splice(index, 1);
    },
    runNext() {
      assert.equal(pending.length, 1);
      pending.shift()();
    },
    size() {
      return pending.length;
    },
  };
}

test("turning repeat off cancels an already queued reveal cycle", () => {
  const scheduler = manualScheduler();
  const timer = createReplaceableTimeout(scheduler.setTimer, scheduler.clearTimer);
  let cycles = 0;

  timer.schedule(() => {
    cycles += 1;
  }, 500);
  assert.equal(scheduler.size(), 1);

  // This is the cleanup performed by DiaTextReveal when repeat changes to false.
  timer.cancel();
  assert.equal(scheduler.size(), 0);
  assert.equal(cycles, 0);
});

test("DiaTextReveal wires repeat-option changes and unmount to the same timer owner", () => {
  assert.match(componentSource, /const repeatTimerRef = useRef(?:<[\s\S]*?>)?\(/);
  assert.match(componentSource, /if \(!repeat\) repeatTimerRef\.current\?\.cancel\(\)/);
  assert.match(componentSource, /repeatTimerRef\.current\?\.cancel\(\)/);
  assert.doesNotMatch(componentSource, /clearTimeout\(timerRef\.current\)/);
});
