import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const source = readFileSync(
  new URL("../src/components/ui/dialog.tsx", import.meta.url),
  "utf8",
);

test("DialogContent keeps long dialogs reachable inside the dynamic viewport", () => {
  const dialogContentSource = source.slice(
    source.indexOf('data-slot="dialog-content"'),
  );
  const defaultClasses = dialogContentSource.match(
    /className=\{cn\(\s*"([^"]+)"\s*,\s*className,/,
  )?.[1];

  assert.ok(defaultClasses, "DialogContent default class list was not found");
  assert.match(defaultClasses, /max-h-\[calc\(100dvh-2rem\)\]/);
  assert.match(defaultClasses, /overflow-y-auto/);
});
