import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import vm from "node:vm";
import { createRequire } from "node:module";

const sourceRoot = path.resolve(import.meta.dirname, "../src");
const readSource = (relativePath) => fs.readFileSync(path.join(sourceRoot, relativePath), "utf8");

test("account progress API forwards the caller signal to httpRequest", async () => {
  const require = createRequire(import.meta.url);
  const typescript = require("../node_modules/typescript");
  const source = readSource("lib/api.ts")
    .replace(/^import .*?;\r?\n/gm, "");
  const output = typescript.transpileModule(source, {
    compilerOptions: { module: typescript.ModuleKind.CommonJS, target: typescript.ScriptTarget.ES2022 },
  }).outputText;
  const calls = [];
  const module = { exports: {} };
  const context = {
    AbortSignal,
    console,
    encodeURIComponent,
    exports: module.exports,
    module,
    httpRequest: async (url, options) => {
      calls.push({ url, options });
      return {};
    },
    encodeApiPath: (value) => value,
    downloadBlobFile: async () => {},
  };
  vm.runInNewContext(output, context, { filename: "api.ts" });

  const signal = new AbortController().signal;
  await module.exports.fetchRefreshProgress("refresh-id", signal);
  await module.exports.fetchReLoginProgress("relogin-id", signal);
  assert.equal(calls.length, 2);
  assert.equal(calls[0].url, "/api/accounts/refresh/progress/refresh-id");
  assert.equal(calls[0].options.signal, signal);
  assert.equal(calls[1].url, "/api/accounts/re-login/progress/relogin-id");
  assert.equal(calls[1].options.signal, signal);
});

test("all serial poll callers retain an owned abort path", () => {
  const accounts = readSource("app/accounts/page.tsx");
  assert.equal((accounts.match(/poll: \(signal(?:: AbortSignal)?\) => fetchRefreshProgress\(/g) || []).length, 2);
  assert.equal((accounts.match(/poll: \(signal(?:: AbortSignal)?\) => fetchReLoginProgress\(/g) || []).length, 1);

  const sub2api = readSource("app/settings/components/sub2api-connections.tsx");
  const ccload = readSource("app/settings/components/ccload-connections.tsx");
  assert.match(sub2api, /const requestServers = async \(signal\?: AbortSignal\)/);
  assert.match(sub2api, /fetchSub2APIServers\(signal\)/);
  assert.match(ccload, /const requestServers = async \(signal\?: AbortSignal\)/);
  assert.match(ccload, /fetchCCLoadServers\(signal\)/);

  const settingsPage = readSource("app/settings/page.tsx");
  const settingsStore = readSource("app/settings/store.ts");
  assert.match(settingsPage, /poll: async \(signal(?:: AbortSignal)?\) => \{\s*await loadPools\(true, signal\);/);
  assert.match(settingsPage, /poll: async \(signal(?:: AbortSignal)?\) => \{\s*await loadBackups\(true, signal\);/);
  assert.match(settingsStore, /loadBackups: async \(silent = false, signal\?: AbortSignal\)/);
  assert.match(settingsStore, /const data = await fetchBackups\(signal\);/);
  assert.match(settingsStore, /loadPools: async \(silent = false, signal\?: AbortSignal\)/);
  assert.match(settingsStore, /const data = await fetchCPAPools\(signal\);/);

  const editable = readSource("app/debug/components/editable-file-panel.tsx");
  const editablePolling = readSource("lib/editable-task-polling.js");
  assert.match(editable, /fetchTasks = useCallback\(async \(ids: string\[\] = \[\], parentSignal\?: AbortSignal\)/);
  assert.match(editable, /createEditableTaskPollingLifecycle\(/);
  assert.match(editable, /taskPollingLifecycleRef\.current\?\.dispose\(\)/);
  assert.match(editable, /lifecycle\.replace\(ids\)/);
  assert.match(editable, /lifecycle\.dispose\(\)/);
  assert.match(editablePolling, /replace\(ids\)/);
  assert.match(editablePolling, /fetchTasks\(taskIds, signal\)/);
  assert.match(editablePolling, /dispose\(\)/);
});
