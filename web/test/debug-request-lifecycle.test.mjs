import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

function sourceOf(path) {
  return readFileSync(fileURLToPath(new URL(path, import.meta.url)), "utf8");
}

test("ChatPanel aborts the chat request on clear and unmount", () => {
  const source = sourceOf("../src/app/debug/components/chat-panel.tsx");
  assert.match(source, /chatAbortControllerRef/);
  assert.match(source, /signal: abortController\.signal/);
  assert.match(source, /requestGate\.cancel\(\);[\s\S]*?chatAbortControllerRef\.current\?\.abort\(\);/);
  assert.match(source, /requestGate\.clear\(\);[\s\S]*?chatAbortControllerRef\.current\?\.abort\(\);/);
});

test("SearchPanel aborts the search request on prompt invalidation and unmount", () => {
  const source = sourceOf("../src/app/debug/components/search-panel.tsx");
  assert.match(source, /searchAbortControllerRef/);
  assert.match(source, /signal: abortController\.signal/);
  assert.match(source, /searchOwner\.cancel\(\);[\s\S]*?searchAbortControllerRef\.current\?\.abort\(\);/);
  assert.match(source, /searchOwnerRef\.current\.invalidate\(\);[\s\S]*?searchAbortControllerRef\.current\?\.abort\(\);/);
});

test("EditableFilePanel aborts task polling requests without aborting task submission", () => {
  const source = sourceOf("../src/app/debug/components/editable-file-panel.tsx");
  const fetchTasks = source.slice(source.indexOf("const fetchTasks = useCallback"), source.indexOf("  useEffect(() =>", source.indexOf("const fetchTasks = useCallback")));
  assert.match(fetchTasks, /taskFetchAbortControllerRef/);
  assert.match(fetchTasks, /signal: abortController\.signal/);
  const submit = source.slice(source.indexOf("const submit ="), source.indexOf("const refreshAll ="));
  assert.doesNotMatch(submit, /signal: abortController\.signal/);
});

test("UserKeysCard aborts its in-flight list request on unmount", () => {
  const source = sourceOf("../src/app/settings/components/user-keys-card.tsx");
  assert.match(source, /loadAbortControllerRef/);
  assert.match(source, /fetchUserKeys\(abortController\.signal\)/);
  assert.match(source, /loadAbortControllerRef\.current\?\.abort\(\);/);
  assert.match(source, /return \(\) => \{[\s\S]*?loadAbortControllerRef\.current\?\.abort\(\);/);
});
