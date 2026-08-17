import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

test("image download fallback opens untrusted URLs without an opener", () => {
  const source = readFileSync(new URL("../src/app/image/components/image-results.tsx", import.meta.url), "utf8");
  assert.match(source, /normalizeStoredImageUrl\(image\.url\)/);
  assert.match(source, /window\.open\(safeUrl, "_blank", "noopener,noreferrer"\)/);
  assert.doesNotMatch(source, /window\.open\(image\.url,/);
});
