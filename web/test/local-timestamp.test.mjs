import assert from "node:assert/strict";
import test from "node:test";

import { formatLocalTimestamp } from "../src/lib/local-timestamp";

test("formats an ISO timestamp in the browser local format", () => {
  const originalOffset = process.env.TZ;
  process.env.TZ = "Asia/Shanghai";
  try {
    assert.equal(formatLocalTimestamp("2026-10-14T09:08:29.560864+00:00"), "2026-10-14 17:08:29");
  } finally {
    if (originalOffset === undefined) {
      delete process.env.TZ;
    } else {
      process.env.TZ = originalOffset;
    }
  }
});

test("keeps invalid or missing timestamps safe", () => {
  assert.equal(formatLocalTimestamp("not-a-time"), "not-a-time");
  assert.equal(formatLocalTimestamp(""), "");
  assert.equal(formatLocalTimestamp(null), "");
});
