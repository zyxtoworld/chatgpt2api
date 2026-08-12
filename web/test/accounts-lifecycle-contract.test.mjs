import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { createLatestActionOwner } from "../src/lib/latest-action-owner.js";

const sourcePath = fileURLToPath(new URL("../src/app/accounts/page.tsx", import.meta.url));
const importDialogPath = fileURLToPath(new URL("../src/app/accounts/components/account-import-dialog.tsx", import.meta.url));

test("accounts initial loads belong to the current setup and cancel on cleanup", () => {
  const text = readFileSync(sourcePath, "utf8");
  assert.match(text, /scheduleOwnedMicrotask/);
  assert.match(text, /const cancelInitialLoad = scheduleOwnedMicrotask/);
  assert.match(text, /cancelInitialLoad\(\);/);
  assert.doesNotMatch(text, /didLoadRef/);
  assert.match(text, /mountedRef\.current/);
});

test("account writes use one mutation owner and guard late results", () => {
  const text = readFileSync(sourcePath, "utf8");
  assert.match(text, /accountMutationOwnerRef/);
  assert.match(text, /beginAccountMutation/);
  assert.match(text, /acceptsAccountMutation/);
  assert.match(text, /finishAccountMutation/);
  assert.match(text, /accountListLoadingOwnerRef\.current = null/);
  assert.match(text, /setIsLoading\(false\)/);
  for (const handler of ["handleDeleteTokens", "handleRefreshAccounts", "handleReLogin", "handleUpdateAccount"]) {
    const handlerBody = text.slice(text.indexOf(`const ${handler}`), text.indexOf("\n  };", text.indexOf(`const ${handler}`)) + 5);
    assert.match(handlerBody, /beginAccountMutation/);
    assert.match(handlerBody, /acceptsAccountMutation/);
    assert.match(handlerBody, /finishAccountMutation/);
  }
  assert.match(text, /createLatestActionOwner/);
  assert.match(text, /accountProxyTestOwnerRef/);
  assert.match(text, /isAccountMutationBusy/);
});

test("account import writes use parent admission and child lifetime ownership", () => {
  const text = readFileSync(importDialogPath, "utf8");
  assert.match(text, /onMutationStart/);
  assert.match(text, /onMutationFinish/);
  assert.match(text, /onMutationStart: \(\) => \{ accepted: boolean; epoch: number \} \| null;/);
  assert.match(text, /onMutationFinish: \(owner: \{ accepted: boolean; epoch: number \}\) => void;/);
  assert.match(text, /createLatestActionOwner/);
  assert.match(text, /submitOwner\.cancel\(\)/);
  assert.match(text, /if \(!acceptsImportAction\(action\)\) return;/);
  assert.match(text, /onMutationFinishRef\.current/);
  assert.doesNotMatch(text, /onMutationStartRef\.current\?\./);
  assert.match(text, /mutation\.accepted === true/);
  const beginBody = text.slice(text.indexOf("const beginImportAction"), text.indexOf("const acceptsImportAction"));
  const admissionIndex = beginBody.indexOf("onMutationStartRef.current()");
  const actionIndex = beginBody.indexOf("submitOwnerRef.current.begin(identity)");
  assert.ok(admissionIndex >= 0, "beginImportAction must request parent admission");
  assert.ok(actionIndex >= 0, "beginImportAction must create a local action owner");
  assert.ok(
    admissionIndex < actionIndex,
    "parent admission must precede local action ownership",
  );
});

test("closing the import dialog drops a deferred OAuth start", async () => {
  const text = readFileSync(importDialogPath, "utf8");
  const start = text.indexOf("const handleOpenChange");
  const end = text.indexOf("\n  };", start) + 5;
  const closeBody = text.slice(start, end);
  assert.match(closeBody, /oauthStartOwnerRef\.current\.invalidate\(\)/);

  const owner = createLatestActionOwner();
  const action = owner.begin("oauth-start");
  const events = [];
  let resolveResponse;
  const response = new Promise((resolve) => {
    resolveResponse = resolve;
  });
  const pending = response.then(() => {
    if (owner.accepts(action, "oauth-start")) {
      events.push("session", "window.open", "toast", "finally");
    }
  });

  owner.invalidate();
  resolveResponse();
  await pending;
  assert.deepEqual(events, []);
});

test("closing the import dialog drops a deferred local file read", async () => {
  const text = readFileSync(importDialogPath, "utf8");
  assert.match(text, /fileReadOwnerRef/);
  const closeStart = text.indexOf("const handleOpenChange");
  const closeEnd = text.indexOf("\n  };", closeStart) + 5;
  assert.match(text.slice(closeStart, closeEnd), /fileReadOwnerRef\.current\.invalidate\(\)/);

  const owner = createLatestActionOwner();
  const action = owner.begin("account-json");
  const events = [];
  let resolveResponse;
  const response = new Promise((resolve) => {
    resolveResponse = resolve;
  });
  const pending = response.then(() => {
    if (owner.accepts(action, "account-json")) {
      events.push("setTokenInput", "setPendingAccountJsonImport", "toast");
    }
  });

  owner.invalidate();
  resolveResponse();
  await pending;
  assert.deepEqual(events, []);
});
