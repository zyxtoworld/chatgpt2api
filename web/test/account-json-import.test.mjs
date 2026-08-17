import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { getAccountJsonAccounts } from "../src/lib/account-json-import.js";

const importDialogSource = readFileSync(
  new URL("../src/app/accounts/components/account-import-dialog.tsx", import.meta.url),
  "utf8",
);

test("imports Sub2API accounts with nested credentials and account settings", () => {
  const accounts = getAccountJsonAccounts({
    accounts: [
      {
        platform: "openai",
        name: "pro-account",
        plan_type: "pro",
        concurrency: 4,
        priority: 2,
        auto_pause_on_expired: true,
        credentials: {
          access_token: "access-token-1",
          refresh_token: "refresh-token-1",
          id_token: "id-token-1",
          organization_id: "org-1",
          plan_type: "pro",
        },
        extra: { email: "user@example.test" },
      },
    ],
  });

  assert.deepEqual(accounts, [
    {
      access_token: "access-token-1",
      source_type: "codex",
      refresh_token: "refresh-token-1",
      id_token: "id-token-1",
      organization_id: "org-1",
      concurrency: 4,
      priority: 2,
      auto_pause_on_expired: true,
      email: "user@example.test",
      type: "pro",
    },
  ]);
});

test("does not pass the nested credentials object or unsupported platforms", () => {
  const accounts = getAccountJsonAccounts({
    accounts: [
      {
        platform: "anthropic",
        credentials: { access_token: "wrong-platform-token" },
      },
      {
        platform: "openai",
        credentials: { access_token: "openai-token" },
      },
    ],
  });

  assert.equal(accounts.length, 1);
  assert.equal(accounts[0].access_token, "openai-token");
  assert.equal("credentials" in accounts[0], false);
});

test("keeps existing top-level account and array JSON formats", () => {
  assert.deepEqual(
    getAccountJsonAccounts({ access_token: "single-token", refresh_token: "refresh" }),
    [{ access_token: "single-token", refresh_token: "refresh", source_type: "codex" }],
  );
  assert.deepEqual(
    getAccountJsonAccounts([{ accessToken: "legacy-token" }]),
    [{ access_token: "legacy-token", source_type: "codex" }],
  );
});

test("the production dialog uses the shared account JSON mapper", () => {
  assert.match(importDialogSource, /@\/lib\/account-json-import/);
  assert.match(importDialogSource, /getAccountJsonAccounts\(parsed\)/);
});
