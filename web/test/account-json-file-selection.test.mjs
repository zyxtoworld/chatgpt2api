import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { settleAccountJsonFiles } from "../src/lib/account-json-file-selection.js";

const importDialogSource = readFileSync(
  new URL("../src/app/accounts/components/account-import-dialog.tsx", import.meta.url),
  "utf8",
);

test("one malformed account JSON file does not discard valid sibling files", async () => {
  const files = [
    { name: "valid-a.json" },
    { name: "broken.json" },
    { name: "valid-b.json" },
  ];

  const result = await settleAccountJsonFiles(files, async (file) => {
    if (file.name === "broken.json") throw new SyntaxError("invalid JSON");
    return [{ access_token: file.name }];
  });

  assert.deepEqual(result.accounts, [
    { access_token: "valid-a.json" },
    { access_token: "valid-b.json" },
  ]);
  assert.equal(result.errorCount, 1);
});

test("empty and failed files are both counted while valid account order is preserved", async () => {
  const files = ["empty", "valid-a", "failed", "valid-b"];

  const result = await settleAccountJsonFiles(files, async (file) => {
    if (file === "failed") return Promise.reject(new Error("read failed"));
    if (file === "empty") return [];
    return [{ access_token: file }];
  });

  assert.deepEqual(result.accounts.map((account) => account.access_token), ["valid-a", "valid-b"]);
  assert.equal(result.errorCount, 2);
});

test("the account import production entry uses settled file selection", () => {
  assert.match(importDialogSource, /settleAccountJsonFiles/);
  assert.doesNotMatch(importDialogSource, /const results = await Promise\.all\(/);
});
