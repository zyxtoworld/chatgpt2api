import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createLifecycleActionOwner } from "../src/lib/lifecycle-action-owner.js";
import { createLatestActionOwner } from "../src/lib/latest-action-owner.js";
import { runBackupMutation } from "../src/lib/backup-detail-lifecycle.js";
import {
  createAbortableLifecycleOwner,
  releaseStaleResponse,
  runBackupDownload,
} from "../src/lib/backup-download-lifecycle.js";

const source = readFileSync(
  fileURLToPath(new URL("../src/app/settings/components/backup-settings-card.tsx", import.meta.url)),
  "utf8",
);

test("backup detail reads have a latest owner and cancel on unmount", () => {
  assert.match(source, /createLatestActionOwner/);
  assert.match(source, /backupDetailOwnerRef/);
  assert.match(source, /backupDetailOwner\.cancel\(\)/);
  assert.match(source, /backupDetailOwner\.accepts\(requestOwner\)/);
});

test("a late backup detail response cannot overwrite the latest selection", async () => {
  const owner = createLatestActionOwner();
  const pending = new Map();
  const events = [];

  const load = async (key) => {
    const requestOwner = owner.begin(key);
    events.push(`loading:${key}`);
    const item = await new Promise((resolve) => pending.set(key, resolve));
    if (owner.accepts(requestOwner)) events.push(`detail:${item}`);
    if (owner.accepts(requestOwner)) events.push(`finally:${key}`);
  };

  const first = load("A");
  const second = load("B");
  pending.get("A")("A-detail");
  await new Promise((resolve) => queueMicrotask(resolve));
  assert.deepEqual(events, ["loading:A", "loading:B"]);

  pending.get("B")("B-detail");
  await Promise.all([first, second]);
  assert.deepEqual(events, ["loading:A", "loading:B", "detail:B-detail", "finally:B"]);
});

test("an unmounted backup detail response publishes no detail, error, or finally state", async () => {
  const owner = createLatestActionOwner();
  const events = [];
  const requestOwner = owner.begin("A");

  owner.cancel();
  if (owner.accepts(requestOwner)) events.push("detail");
  if (owner.accepts(requestOwner)) events.push("error");
  if (owner.accepts(requestOwner)) events.push("finally");

  assert.deepEqual(events, []);
});

test("closing backup details invalidates the pending read before late callbacks", () => {
  assert.doesNotMatch(source, /onOpenChange=\{setDetailOpen\}/);
  assert.match(source, /const handleDetailOpenChange = \(open: boolean\) => \{/);
  assert.match(source, /backupDetailOwnerRef\.current\.invalidate\(\);/);
  assert.match(source, /onOpenChange=\{handleDetailOpenChange\}/);

  const owner = createLatestActionOwner();
  const requestOwner = owner.begin("A");
  const events = [];

  owner.invalidate();
  if (owner.accepts(requestOwner)) events.push("detail");
  if (owner.accepts(requestOwner)) events.push("error");
  if (owner.accepts(requestOwner)) events.push("finally");

  assert.deepEqual(events, []);
});

test("production backup removal invalidates an in-flight detail read before the delete settles", async () => {
  assert.match(source, /const handleRemoveBackup = async \(key: string\) => \{/);
  assert.match(source, /runBackupMutation\(/);
  assert.match(source, /onClick=\{\(\) => void handleRemoveBackup\(item\.key\)\}/);

  const owner = createLatestActionOwner();
  owner.activate();
  const detailRequest = owner.begin("A");
  let resolveDelete;
  const deletePromise = new Promise((resolve) => {
    resolveDelete = resolve;
  });

  const removing = runBackupMutation(
    () => owner.invalidate(),
    () => deletePromise,
  );

  assert.equal(owner.accepts(detailRequest), false);
  resolveDelete(true);
  assert.equal(await removing, true);

  const replacement = owner.begin("B");
  assert.equal(owner.accepts(replacement), true);
});

test("backup downloads share a lifecycle owner and reject late completion after unmount", () => {
  assert.match(source, /createAbortableLifecycleOwner/);
  assert.match(source, /backupDownloadOwnerRef/);
  assert.match(source, /backupDownloadOwner\.cancel\(\)/);
  assert.match(source, /runBackupDownload\(/);

  const owner = createLifecycleActionOwner();
  owner.activate();
  const first = owner.begin();
  const second = owner.begin();
  const events = [];

  if (owner.accepts(first)) events.push("first-download");
  if (owner.accepts(second)) events.push("second-download");
  assert.deepEqual(events, ["first-download", "second-download"]);

  owner.cancel();
  assert.equal(owner.accepts(first), false);
  assert.equal(owner.accepts(second), false);
});

test("a stale backup download releases the response body", async () => {
  let cancelled = 0;
  const response = {
    body: {
      cancel: async () => {
        cancelled += 1;
      },
    },
  };

  await releaseStaleResponse(response);
  assert.equal(cancelled, 1);
});

test("backup download cancellation aborts the in-flight request and rejects late callbacks", () => {
  const owner = createAbortableLifecycleOwner();
  const download = owner.begin();

  assert.equal(download.controller.signal.aborted, false);
  assert.equal(owner.accepts(download), true);

  owner.cancel();

  assert.equal(download.controller.signal.aborted, true);
  assert.equal(owner.accepts(download), false);
});

test("the production backup download flow passes signal and drops a response after unmount", async () => {
  const owner = createAbortableLifecycleOwner();
  owner.activate();
  let resolveFetch;
  let requestOptions;
  let cancelled = 0;
  const fetchPromise = new Promise((resolve) => {
    resolveFetch = resolve;
  });
  const events = [];
  const download = runBackupDownload({
    owner,
    getAuthKey: async () => "test-key",
    fetchImpl: async (_url, options) => {
      requestOptions = options;
      return fetchPromise;
    },
    url: "/api/backups/download?key=backup-key",
    fallbackName: "backup.bin",
    filenameFromContentDisposition: () => "backup.bin",
    requestErrorMessage: () => "download failed",
    onDownload: () => events.push("download"),
    onSuccess: () => events.push("success"),
    onError: () => events.push("error"),
  });

  await new Promise((resolve) => queueMicrotask(resolve));
  assert.ok(requestOptions?.signal instanceof AbortSignal);
  assert.equal(requestOptions.signal.aborted, false);
  owner.cancel();
  assert.equal(requestOptions.signal.aborted, true);

  resolveFetch({
    ok: true,
    headers: { get: () => "" },
    blob: async () => new Blob(["late"]),
    body: { cancel: async () => { cancelled += 1; } },
  });
  await download;

  assert.deepEqual(events, []);
  assert.equal(cancelled, 1);
  assert.equal(owner.activeCount(), 0);
});
