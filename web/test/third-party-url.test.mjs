import assert from "node:assert/strict";
import test from "node:test";

import { buildThirdPartyHref, formatThirdPartyDisplayHref } from "../src/lib/third-party-url.js";

test("malformed percent in a configured URL does not throw while displaying", () => {
  const href = "https://example.test/%";

  assert.doesNotThrow(() => formatThirdPartyDisplayHref(href));
  assert.equal(formatThirdPartyDisplayHref(href), href);
});

test("valid UTF-8 percent escapes remain readable in the display text", () => {
  assert.equal(
    formatThirdPartyDisplayHref("https://example.test/%E4%B8%AD/%E6%96%87"),
    "https://example.test/中/文",
  );
});

test("third-party navigation only appends baseUrl", () => {
  const href = buildThirdPartyHref("https://canvas.example.test/app", "https://api.example.test");
  const parsed = new URL(href);

  assert.equal(parsed.searchParams.get("baseUrl"), "https://api.example.test");
  assert.equal(parsed.searchParams.get("apiKey"), null);
});

test("malformed third-party URL values fail closed without coercion", () => {
  let coerced = false;
  const value = {
    trim() {
      coerced = true;
      return "https://canvas.example.test";
    },
  };

  assert.doesNotThrow(() => buildThirdPartyHref(value, "https://api.example.test"));
  assert.equal(buildThirdPartyHref(value, "https://api.example.test"), "");
  assert.equal(coerced, false);
});
