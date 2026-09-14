import assert from "node:assert/strict";
import test from "node:test";

import { filenameFromUrl } from "../src/lib/file-display.js";

test("malformed URL percent escapes do not crash filename display", () => {
  const url = "https://example.test/files/%";

  assert.doesNotThrow(() => filenameFromUrl(url));
  assert.equal(filenameFromUrl(url), "%");
});

test("valid URL filename escapes are decoded", () => {
  assert.equal(filenameFromUrl("https://example.test/files/%E6%B5%8B%E8%AF%95.pptx"), "测试.pptx");
});
