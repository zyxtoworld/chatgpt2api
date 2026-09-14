import assert from "node:assert/strict";
import test from "node:test";

import { filenameFromContentDisposition } from "../src/lib/content-disposition.js";

test("malformed UTF-8 filename escapes do not throw", () => {
  const header = "attachment; filename*=UTF-8''%";

  assert.doesNotThrow(() => filenameFromContentDisposition(header));
  assert.equal(filenameFromContentDisposition(header), "%");
});

test("valid UTF-8 filename escapes are decoded", () => {
  assert.equal(
    filenameFromContentDisposition("attachment; filename*=UTF-8''%E6%B5%8B%E8%AF%95.zip"),
    "测试.zip",
  );
});

test("plain filename remains supported", () => {
  assert.equal(filenameFromContentDisposition('attachment; filename="backup.zip"'), "backup.zip");
});
