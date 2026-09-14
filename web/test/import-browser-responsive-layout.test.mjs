import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const entries = [
  ["CPA", "../src/app/settings/components/import-browser-dialog.tsx"],
  ["Sub2API", "../src/app/settings/components/sub2api-connections.tsx"],
  ["CCLoad", "../src/app/settings/components/ccload-connections.tsx"],
];

for (const [name, path] of entries) {
  test(`${name} import search shrinks inside narrow dialogs`, () => {
    const source = readFileSync(new URL(path, import.meta.url), "utf8");

    assert.match(
      source,
      /className="relative w-full min-w-0 sm:min-w-\[260px\]"/,
    );
    assert.match(
      source,
      /className="flex flex-wrap items-center gap-2"/,
    );
    assert.match(
      source,
      /className="flex flex-wrap items-center justify-between gap-2 border-b border-stone-100 px-4 py-3 text-sm text-stone-500"/,
    );
    assert.match(
      source,
      /className="flex flex-col gap-3 text-sm text-stone-500 sm:flex-row sm:items-center sm:justify-between"/,
    );
  });
}
