import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const source = readFileSync(
  new URL("../src/app/debug/page.tsx", import.meta.url),
  "utf8",
);

test("debug tabs remain horizontally reachable on narrow viewports", () => {
  assert.match(
    source,
    /<div className="hide-scrollbar overflow-x-auto">[\s\S]*?<TabsList variant="line" className="w-full min-w-max">/,
  );
  assert.match(source, /<TabsTrigger key=\{value\} value=\{value\} className="px-4">/);
});
