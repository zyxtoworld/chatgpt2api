import assert from "node:assert/strict";
import test from "node:test";

import { formatElapsedSeconds, getElapsedSeconds } from "../src/lib/elapsed-display.js";

test("stale clock never reduces the persisted elapsed value", () => {
  assert.equal(getElapsedSeconds(12, 2_000, 1_000), 12);
});

test("current clock adds only the elapsed time since the server update", () => {
  assert.equal(getElapsedSeconds(12, 2_000, 4_500), 14.5);
});

test("missing timestamps preserve the server value", () => {
  assert.equal(getElapsedSeconds(12, undefined, 4_500), 12);
  assert.equal(getElapsedSeconds(12, 2_000, undefined), 12);
});

test("elapsed display uses one non-negative whole-second value", () => {
  assert.equal(formatElapsedSeconds(14.5), "0m 14s");
  assert.equal(formatElapsedSeconds(59.9), "0m 59s");
  assert.equal(formatElapsedSeconds(60), "1m 00s");
  assert.equal(formatElapsedSeconds(-1), "0m 00s");
  assert.equal(formatElapsedSeconds(Number.NaN), "0m 00s");
  assert.equal(formatElapsedSeconds(Number.POSITIVE_INFINITY), "0m 00s");
});
