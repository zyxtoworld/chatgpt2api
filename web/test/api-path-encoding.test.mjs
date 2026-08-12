import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { encodeApiPath } from "../src/lib/api-path.js";

const apiSource = readFileSync(new URL("../src/lib/api.ts", import.meta.url), "utf8");

test("API path encoding preserves separators and escapes filename delimiters", () => {
  assert.equal(
    encodeApiPath("2026-08-12/folder/a #1?.png"),
    "2026-08-12/folder/a%20%231%3F.png",
  );
  assert.equal(encodeApiPath("folder/literal%2Fname.png"), "folder/literal%252Fname.png");
  assert.equal(encodeApiPath("图片/苹果.png"), "%E5%9B%BE%E7%89%87/%E8%8B%B9%E6%9E%9C.png");
});

test("single-image download uses the encoded relative path", () => {
  assert.match(apiSource, /\/api\/images\/download\/\$\{encodeApiPath\(path\)\}/);
  assert.doesNotMatch(apiSource, /\/api\/images\/download\/\$\{path\}/);
});
