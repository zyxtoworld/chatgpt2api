import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const source = readFileSync(
  new URL("../src/components/ui/sheet.tsx", import.meta.url),
  "utf8",
);

test("SheetContent keeps navigation reachable in short viewports", () => {
  const contentSource = source.slice(source.indexOf('data-slot="sheet-content"'));
  const defaultClasses = contentSource.match(
    /className=\{cn\(\s*"([^"]+)"\s*,/,
  )?.[1];

  assert.ok(defaultClasses, "SheetContent default class list was not found");
  assert.match(defaultClasses, /overflow-y-auto/);
  assert.match(defaultClasses, /overscroll-contain/);
});
