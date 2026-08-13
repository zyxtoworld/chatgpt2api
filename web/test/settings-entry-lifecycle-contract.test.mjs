import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

const componentRoot = fileURLToPath(new URL("../src/app/settings/components/", import.meta.url));
const pageSource = readFileSync(fileURLToPath(new URL("../src/app/settings/page.tsx", import.meta.url)), "utf8");

function source(name) {
  return readFileSync(`${componentRoot}${name}`, "utf8");
}

test("Sub2API list entry owns initial loading and mutation invalidation", () => {
  const text = source("sub2api-connections.tsx");
  assert.match(text, /createOwnedQueryLoader, scheduleOwnedMicrotask/);
  assert.match(text, /const cancelInitialLoad = scheduleOwnedMicrotask\(\(\) => loadServers\(\)\);/);
  assert.match(text, /cancelInitialLoad\(\);[\s\S]*?loader\.cancel\(\);/);
  assert.match(text, /listQueryRef\.current\?\.clearLoadingForMutation\(\);/);
});

test("ccLoad list entry owns initial loading and mutation invalidation", () => {
  const text = source("ccload-connections.tsx");
  assert.match(text, /createOwnedQueryLoader, scheduleOwnedMicrotask/);
  assert.match(text, /const cancelInitialLoad = scheduleOwnedMicrotask\(\(\) => loadServers\(\)\);/);
  assert.match(text, /cancelInitialLoad\(\);[\s\S]*?loader\.cancel\(\);/);
  assert.match(text, /listQueryRef\.current\?\.clearLoadingForMutation\(\);/);
});

test("ccLoad import polling remains mounted while another settings tab is active", () => {
  assert.match(
    pageSource,
    /<TabsContent value="ccload" forceMount className="data-\[state=inactive\]:hidden">/,
  );
});

test("UserKeys initial load is owned by its setup and canceled before cleanup", () => {
  const text = source("user-keys-card.tsx");
  assert.match(text, /scheduleOwnedMicrotask/);
  assert.match(text, /const cancelInitialLoad = scheduleOwnedMicrotask\(\(\) => load\(\)\);/);
  assert.match(text, /cancelInitialLoad\(\);[\s\S]*?gate\.cancel\(\);/);
  assert.match(text, /loadingOwnerRef\.current = null;[\s\S]*?setIsLoading\(false\);/);
});

test("Proxy settings entry invalidates the initial read when saving", () => {
  const text = source("proxy-settings.tsx");
  assert.match(text, /createMutationRequestGate/);
  assert.match(text, /createOwnedQueryLoader/);
  assert.match(text, /scheduleOwnedMicrotask/);
  assert.match(text, /clearLoadingForMutation/);
  assert.match(text, /acceptsMutation/);
  assert.match(text, /createLatestActionOwner/);
  assert.match(text, /testOwnerRef/);
  assert.match(text, /testOwnerRef\.current\.invalidate\(\)/);
  assert.match(text, /testOwner\.cancel\(\)/);
  assert.match(text, /testOwnerRef\.current\.accepts/);
  assert.match(text, /testOwnerRef\.current\.invalidate\(\);\s*setIsTesting\(false\);/);
  assert.match(text, /testOwner\.cancel\(\);/);
});

test("Sub2API group loading owns the editor identity and cleanup lifetime", () => {
  const text = source("sub2api-connections.tsx");
  assert.match(text, /createLatestActionOwner/);
  assert.match(text, /groupsOwnerRef/);
  assert.match(text, /groupsOwnerRef\.current\.accepts/);
  assert.match(text, /groupsOwnerRef\.current\.invalidate/);
  assert.match(text, /groupsOwner\.cancel/);
  assert.match(text, /const mutationOwner = beginMutation\(\);[\s\S]*?groupsOwnerRef\.current\.invalidate\(\);[\s\S]*?setIsLoadingGroups\(false\);/);
  assert.doesNotMatch(text, /onOpenChange=\{setDialogOpen\}/);
});

test("settings cleanup invalidates WebDAV presentation operations", () => {
  assert.match(pageSource, /cancelImageStorageOperations/);
  assert.match(pageSource, /cancelBackupOperations\(\);[\s\S]*?cancelImageStorageOperations\(\);/);
});
