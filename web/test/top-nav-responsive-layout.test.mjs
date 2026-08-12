import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const source = readFileSync(
  new URL("../src/components/top-nav.tsx", import.meta.url),
  "utf8",
);

test("TopNav keeps the compact navigation through tablet widths", () => {
  assert.match(source, /<SheetTrigger className="[^"]*lg:hidden/);
  assert.match(source, /<HeaderActions className="ml-auto lg:hidden"/);
  assert.match(source, /<nav className="[^"]*hidden[^"]*lg:flex[^"]*lg:overflow-visible/);
  assert.match(source, /<div className="hidden items-center justify-end gap-2 lg:flex/);
});

test("TopNav switches its row layout and desktop item styling at one breakpoint", () => {
  assert.match(source, /flex min-h-12 flex-col[^"]*lg:h-12 lg:flex-row/);
  assert.match(source, /lg:rounded-none lg:px-0 lg:text-\[15px\]/);
  assert.match(source, /lg:block/);
});
