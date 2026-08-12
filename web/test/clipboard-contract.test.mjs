import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { writeClipboardText } from "../src/lib/clipboard.js";

const accountsSource = readFileSync(new URL("../src/app/accounts/page.tsx", import.meta.url), "utf8");
const imageManagerSource = readFileSync(new URL("../src/app/image-manager/page.tsx", import.meta.url), "utf8");

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
  for (const source of [accountsSource, imageManagerSource]) {
    assert.match(source, /writeClipboardText\(/);
    assert.doesNotMatch(source, /void navigator\.clipboard\.writeText/);
  }
});
