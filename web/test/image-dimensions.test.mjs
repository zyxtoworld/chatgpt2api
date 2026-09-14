import assert from "node:assert/strict";
import test from "node:test";

import { setImageDimension } from "../src/lib/image-dimensions.js";

test("image dimensions update produces renderable state without mutating the previous map", () => {
  const previous = {};
  const next = setImageDimension(previous, "image-1", 1024, 768);

  assert.deepEqual(next, { "image-1": "1024 x 768" });
  assert.notStrictEqual(next, previous);
  assert.deepEqual(previous, {});
});

test("unchanged image dimensions preserve state identity", () => {
  const previous = { "image-1": "1024 x 768" };

  assert.strictEqual(setImageDimension(previous, "image-1", 1024, 768), previous);
});
