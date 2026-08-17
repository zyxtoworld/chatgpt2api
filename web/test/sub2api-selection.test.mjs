import assert from "node:assert/strict";
import test from "node:test";

import { normalizeSub2APIRemoteAccounts } from "../src/lib/sub2api-selection.js";

test("Sub2API account normalization does not stringify container fields", () => {
  const canary = "sub2api-frontend-container-canary";
  const accounts = normalizeSub2APIRemoteAccounts([
    { id: { secret: canary }, name: "skip" },
    {
      id: "safe-account",
      name: { secret: canary },
      email: [canary],
      plan_type: { secret: canary },
      status: [canary],
      expires_at: { secret: canary },
      has_refresh_token: { secret: canary },
    },
  ]);

  assert.deepEqual(accounts, [{
    id: "safe-account",
    name: "",
    email: "",
    plan_type: "",
    status: "",
    expires_at: "",
    has_refresh_token: false,
  }]);
  assert.equal(JSON.stringify(accounts).includes(canary), false);
});
