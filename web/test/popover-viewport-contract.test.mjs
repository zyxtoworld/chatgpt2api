import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const source = readFileSync(
  new URL("../src/components/ui/popover.tsx", import.meta.url),
  "utf8",
);

test("PopoverContent stays inside the collision-aware available height", () => {
  const contentSource = source.slice(
    source.indexOf('data-slot="popover-content"'),
  );
  const defaultClasses = contentSource.match(
    /className=\{cn\(\s*"([^"]+)"\s*,\s*className,/,
  )?.[1];

  assert.ok(defaultClasses, "PopoverContent default class list was not found");
  assert.match(
    defaultClasses,
    /max-h-\[var\(--radix-popover-content-available-height\)\]/,
  );
  assert.match(defaultClasses, /overflow-y-auto/);
  assert.match(defaultClasses, /overscroll-contain/);
});
