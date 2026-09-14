import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { getAccountJsonAccounts } from "../src/lib/account-json-import.js";

const importDialogSource = readFileSync(
  new URL("../src/app/accounts/components/account-import-dialog.tsx", import.meta.url),
  "utf8",
);

test("rejects Sub2API records that contain refresh or id tokens", () => {
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

  assert.deepEqual(accounts, []);
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
  assert.deepEqual(getAccountJsonAccounts({ access_token: "single-token" }), [
    { access_token: "single-token", source_type: "codex" },
  ]);
  assert.deepEqual(getAccountJsonAccounts([{ accessToken: "legacy-token" }]), []);
});

test("rejects top-level refresh_token or id_token instead of silently forwarding them", () => {
  const accounts = getAccountJsonAccounts({
    accounts: [{
      access_token: "access-only",
      refresh_token: "must-drop",
      id_token: "must-drop",
    }],
  });
  assert.deepEqual(accounts, []);
});

test("the production dialog uses the shared account JSON mapper", () => {
  assert.match(importDialogSource, /@\/lib\/account-json-import/);
  assert.match(importDialogSource, /getAccountJsonAccounts\(parsed\)/);
});
