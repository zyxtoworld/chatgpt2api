import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { toggleDocumentTheme } from "../src/lib/theme-toggle-state.js";

const themeToggleSource = readFileSync(
  new URL("../src/components/ui/animated-theme-toggler.tsx", import.meta.url),
  "utf8",
);

function createThemeFixture() {
  const classes = new Set();
  const values = new Map();
  return {
    root: {
      classList: {
        contains(value) {
          return classes.has(value);
        },
        toggle(value, force) {
          if (force) classes.add(value);
          else classes.delete(value);
          return force;
        },
      },
      style: { colorScheme: "light" },
    },
    storage: {
      setItem(key, value) {
        values.set(key, value);
      },
    },
    classes,
    values,
  };
}

test("two immediate theme toggles stay consistent without a React render", () => {
  const fixture = createThemeFixture();

  assert.equal(toggleDocumentTheme(fixture.root, fixture.storage), true);
  assert.equal(fixture.classes.has("dark"), true);
  assert.equal(fixture.root.style.colorScheme, "dark");
  assert.equal(fixture.values.get("chatgpt2api-theme"), "dark");

  assert.equal(toggleDocumentTheme(fixture.root, fixture.storage), false);
  assert.equal(fixture.classes.has("dark"), false);
  assert.equal(fixture.root.style.colorScheme, "light");
  assert.equal(fixture.values.get("chatgpt2api-theme"), "light");
});

test("theme component delegates the state transition instead of using stale isDark", () => {
  assert.match(themeToggleSource, /toggleDocumentTheme\(document\.documentElement, localStorage\)/);
  assert.doesNotMatch(themeToggleSource, /const newTheme = !isDark/);
});
