import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createMutationRequestGate } from "../src/lib/mutation-request-gate.js";
import { finishMutationAndRefresh } from "../src/lib/mutation-refresh-controller.js";
import { createReplaceableTimeout } from "../src/lib/replaceable-timeout.js";

const pagePath = fileURLToPath(new URL("../src/app/image-manager/page.tsx", import.meta.url));
const pageSource = readFileSync(pagePath, "utf8");
const coordinatorSource = readFileSync(fileURLToPath(new URL("../src/lib/mutation-refresh-controller.js", import.meta.url)), "utf8");

test("image manager routes writes through one mutation owner and cancels it on unmount", () => {
  assert.match(pageSource, /createMutationRequestGate/);
  assert.match(pageSource, /imageMutationGateRef/);
  assert.match(pageSource, /beginImageMutation/);
  assert.match(pageSource, /acceptsMutation/);
  assert.match(pageSource, /const mutationGate = imageMutationGateRef\.current;[\s\S]*?mutationGate\.cancel\(\)/);
  assert.match(pageSource, /finishMutationAndRefresh/);
  assert.match(pageSource, /createReplaceableTimeout/);
  assert.match(pageSource, /onClick=\{\(\) => void handleCompress\(\)\}/);
  assert.match(pageSource, /onSubmit=\{\(event\) => \{ event\.preventDefault\(\); void handleDeleteToTarget\(\); \}\}/);
  assert.match(pageSource, /onClick=\{\(\) => void handleDeleteByDate\(\)\}/);

  const handler = (name, nextName) => pageSource.slice(
    pageSource.indexOf(`const ${name}`),
    pageSource.indexOf(`const ${nextName}`),
  );
  for (const [name, nextName] of [
    ["handleDelete =", "handleSetTags"],
    ["handleSetTags", "handleAddTag"],
    ["handleDeleteTag", "startTagPress"],
    ["confirmDelete", "handleCompress"],
    ["handleCompress", "handleDeleteToTarget"],
    ["handleDeleteToTarget", "handleDeleteByDate"],
    ["handleDeleteByDate", "handleBatchDownload"],
  ]) {
    const body = handler(name, nextName);
    assert.notEqual(body, "", `${name} handler must exist`);
    assert.match(body, /beginImageMutation/);
    assert.match(body, /finishImageMutation\(mutationOwner\)/);
  }
  assert.match(coordinatorSource, /gate\.finishMutation\(owner\)/);
  assert.match(coordinatorSource, /void reloadList\(\)/);
  assert.match(coordinatorSource, /void refreshSecondary\(\)/);
  const openDeleteBody = pageSource.slice(
    pageSource.indexOf("const openDeleteDialog"),
    pageSource.indexOf("const handleDelete"),
  );
  assert.match(openDeleteBody, /deleteDialogCleanupTimerRef\.current\.cancel\(\)/);
});

test("a late image query cannot overwrite a mutation result", () => {
  const gate = createMutationRequestGate();
  const query = gate.beginQuery("list");
  let items = ["new-after-delete"];

  const mutation = gate.beginMutation();
  assert.equal(mutation.accepted, true);
  if (gate.acceptsQuery(query)) items = ["stale-before-delete"];

  assert.deepEqual(items, ["new-after-delete"]);
  assert.equal(gate.finishMutation(mutation), true);
});

test("a rejected second image write leaves the first owner valid, while cleanup drops both", () => {
  const gate = createMutationRequestGate();
  const first = gate.beginMutation();
  const second = gate.beginMutation();
  let committed = 0;

  assert.equal(first.accepted, true);
  assert.equal(second.accepted, false);
  if (gate.acceptsMutation(first)) committed += 1;
  assert.equal(committed, 1);

  gate.cancel();
  assert.equal(gate.acceptsMutation(first), false);
  assert.equal(gate.finishMutation(first), false);
});

test("the production mutation coordinator reloads list/storage for the current query after finish", () => {
  const gate = createMutationRequestGate();
  let listLoading = true;
  let storageLoading = true;
  let listReloads = [];
  let storageReloads = 0;
  let currentQuery = "old-date";
  let busy = true;
  const oldList = gate.beginQuery("list");
  const oldStorage = gate.beginQuery("storage");
  const mutation = gate.beginMutation();

  currentQuery = "new-date";
  if (gate.acceptsQuery(oldList)) listReloads.push("old-date");
  if (gate.acceptsQuery(oldStorage)) storageLoading = false;
  assert.equal(listReloads.length, 0);
  assert.equal(storageLoading, true);

  assert.equal(finishMutationAndRefresh({
    gate,
    owner: mutation,
    onBusyChange: (value) => { busy = value; },
    reloadList: () => {
      const newList = gate.beginQuery("list");
      assert.equal(newList.allowed, true);
      listLoading = true;
      listReloads.push(currentQuery);
      if (gate.acceptsQuery(newList)) listLoading = false;
    },
    refreshSecondary: () => {
      const newStorage = gate.beginQuery("storage");
      assert.equal(newStorage.allowed, true);
      storageLoading = true;
      storageReloads += 1;
      if (gate.acceptsQuery(newStorage)) storageLoading = false;
    },
  }), true);

  assert.deepEqual(listReloads, ["new-date"]);
  assert.equal(storageReloads, 1);
  assert.equal(busy, false);
  assert.equal(listLoading, false);
  assert.equal(storageLoading, false);
});

test("opening a new delete dialog cancels the previous target cleanup timer", () => {
  const pending = new Map();
  let nextId = 0;
  const cleanup = createReplaceableTimeout(
    (callback) => {
      const id = ++nextId;
      pending.set(id, callback);
      return id;
    },
    (id) => pending.delete(id),
  );
  let target = "A";

  cleanup.schedule(() => { target = null; }, 200);
  cleanup.cancel();
  target = "B";
  for (const callback of pending.values()) callback();

  assert.equal(target, "B");
});

test("tag long press replaces stale targets and cleanup cancels the active timer", () => {
  const pending = new Map();
  let nextId = 0;
  const longPress = createReplaceableTimeout(
    (callback) => {
      const id = ++nextId;
      pending.set(id, () => {
        pending.delete(id);
        callback();
      });
      return id;
    },
    (id) => pending.delete(id),
  );
  const opened = [];

  longPress.schedule(() => opened.push("A"), 800);
  longPress.schedule(() => opened.push("B"), 800);
  for (const callback of [...pending.values()]) callback();
  assert.deepEqual(opened, ["B"]);

  longPress.schedule(() => opened.push("C"), 800);
  longPress.cancel();
  for (const callback of [...pending.values()]) callback();
  assert.deepEqual(opened, ["B"]);

  assert.match(pageSource, /const tagPressTimerRef = useRef\(createReplaceableTimeout\(\)\)/);
  assert.match(pageSource, /tagPressTimerRef\.current\.schedule\(/);
  assert.match(pageSource, /tagPressTimerRef\.current\.cancel\(\)/);
  assert.match(pageSource, /const tagPressTimer = tagPressTimerRef\.current;[\s\S]*?tagPressTimer\.cancel\(\)/);
});
