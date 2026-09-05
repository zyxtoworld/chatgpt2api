import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { writeClipboardText } from "../src/lib/clipboard.js";
import { maskToken } from "../src/lib/token-display.js";

const accountsSource = readFileSync(new URL("../src/app/accounts/page.tsx", import.meta.url), "utf8");
const imageManagerSource = readFileSync(new URL("../src/app/image-manager/page.tsx", import.meta.url), "utf8");
const userKeysSource = readFileSync(new URL("../src/app/settings/components/user-keys-card.tsx", import.meta.url), "utf8");

test("clipboard writes report success only after the browser accepts the write", async () => {
  const events = [];
  const clipboard = {
    async writeText(value) {
      events.push(value);
    },
  };

  assert.equal(await writeClipboardText("fixture", clipboard), true);
  assert.deepEqual(events, ["fixture"]);
});

test("clipboard writes fail closed when the API is missing or rejects", async () => {
  assert.equal(await writeClipboardText("fixture", undefined), false);
  assert.equal(await writeClipboardText("fixture", {
    async writeText() {
      throw new Error("permission denied");
    },
  }), false);
});

test("copy buttons use the guarded clipboard helper instead of fire-and-forget writes", () => {
  for (const source of [accountsSource, imageManagerSource, userKeysSource]) {
    assert.match(source, /writeClipboardText\(/);
    assert.doesNotMatch(source, /void navigator\.clipboard\.writeText/);
  }
});

test("token display masks short and long values while rejecting non-string input", () => {
  assert.equal(maskToken("short-fixture"), "shor...ture");
  const token = "0123456789abcdef-middle-fixture-uvwxyz12";
  const masked = maskToken(token);
  assert.equal(masked.slice(0, 4), token.slice(0, 4));
  assert.equal(masked.slice(-4), token.slice(-4));
  assert.match(masked, /\.\.\./);
  assert.notEqual(masked, token);
  assert.equal(maskToken(""), "—");
  assert.equal(maskToken(null), "—");
  assert.equal(maskToken({ toString: () => token }), "—");
});

test("account token copy control exposes only a generic label, never a raw-token DOM attribute", () => {
  const displayIndex = accountsSource.indexOf("{maskToken(account.access_token)}");
  assert.ok(displayIndex >= 0, "account rows must use the masking helper");
  const cellStart = accountsSource.lastIndexOf("<td", displayIndex);
  const cellEnd = accountsSource.indexOf("</td>", displayIndex) + "</td>".length;
  const tokenCell = accountsSource.slice(cellStart, cellEnd);
  assert.match(tokenCell, /aria-label="复制 token"/);
  assert.doesNotMatch(tokenCell, /(?:aria-label|title|data-[^=\s]+)=[^\n]*access_token/);
});
