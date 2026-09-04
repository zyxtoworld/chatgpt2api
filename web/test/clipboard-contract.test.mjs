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

test("token display preserves short values and keeps both ends when masking", () => {
  assert.equal(maskToken("short-fixture"), "short-fixture");
  const token = "0123456789abcdef-middle-fixture-uvwxyz12";
  const masked = maskToken(token);
  assert.equal(masked.slice(0, 16), token.slice(0, 16));
  assert.equal(masked.slice(-8), token.slice(-8));
  assert.match(masked, /\.\.\./);
  assert.notEqual(masked, token);
});
