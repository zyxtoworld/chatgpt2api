import assert from "node:assert/strict";
import test from "node:test";

import { formatAvailableImageQuota } from "../src/lib/image-account-state.js";

test("available image quota matches the backend image-account predicate", () => {
  assert.equal(
    formatAvailableImageQuota([
      { status: "正常", quota: 3 },
      { status: "限流", quota: 99 },
      { status: "异常", quota: 88 },
      { status: "禁用", quota: 77 },
      { status: "正常", quota: 0 },
      { status: "正常", quota: -1 },
      { status: "正常", quota: "42" },
    ]),
    "3",
  );
});

test("malformed account collections do not advertise quota", () => {
  assert.equal(formatAvailableImageQuota(null), "0");
  assert.equal(formatAvailableImageQuota([{ status: "正常", quota: 1.5 }]), "0");
  assert.equal(formatAvailableImageQuota([{ status: "正常", quota: true }]), "0");
});
