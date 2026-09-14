import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  readOptionalStorageItem,
  removeOptionalStorageItem,
  writeOptionalStorageItem,
} from "../src/lib/optional-storage.js";

const imagePageSource = readFileSync(new URL("../src/app/image/page.tsx", import.meta.url), "utf8");
const themeToggleSource = readFileSync(
  new URL("../src/components/ui/animated-theme-toggler.tsx", import.meta.url),
  "utf8",
);

test("optional browser storage failures use fallbacks instead of throwing", () => {
  const storage = {
    getItem() {
      throw new Error("storage read denied");
    },
    setItem() {
      throw new Error("storage write denied");
    },
    removeItem() {
      throw new Error("storage remove denied");
    },
  };

  assert.equal(readOptionalStorageItem(storage, "key"), null);
  assert.equal(writeOptionalStorageItem(storage, "key", "value"), false);
  assert.equal(removeOptionalStorageItem(storage, "key"), false);
});

test("optional browser storage preserves successful reads and writes", () => {
  const values = new Map();
  const storage = {
    getItem(key) {
      return values.get(key) ?? null;
    },
    setItem(key, value) {
      values.set(key, value);
    },
    removeItem(key) {
      values.delete(key);
    },
  };

  assert.equal(writeOptionalStorageItem(storage, "key", "value"), true);
  assert.equal(readOptionalStorageItem(storage, "key"), "value");
  assert.equal(removeOptionalStorageItem(storage, "key"), true);
  assert.equal(readOptionalStorageItem(storage, "key"), null);
});

test("ImagePage routes every local preference access through optional storage", () => {
  assert.match(imagePageSource, /readOptionalStorageItem/);
  assert.match(imagePageSource, /writeOptionalStorageItem/);
  assert.match(imagePageSource, /removeOptionalStorageItem/);
  assert.doesNotMatch(imagePageSource, /window\.localStorage\.(?:getItem|setItem|removeItem)/);
});

test("the theme toggle remains usable when optional storage rejects writes", () => {
  assert.match(themeToggleSource, /toggleDocumentTheme\(document\.documentElement, localStorage\)/);
  assert.doesNotMatch(themeToggleSource, /localStorage\.setItem\(/);
});
