import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createMutationRequestGate } from "../src/lib/mutation-request-gate.js";
import { finishMutationAndRefresh } from "../src/lib/mutation-refresh-controller.js";

const source = readFileSync(fileURLToPath(new URL("../src/app/logs/page.tsx", import.meta.url)), "utf8");

test("logs list and delete share query/mutation ownership", () => {
  assert.match(source, /createMutationRequestGate/);
  assert.match(source, /logMutationGateRef/);
  assert.match(source, /beginQuery\("list"\)/);
  assert.match(source, /acceptsQuery/);
  assert.match(source, /const mutationGate = logMutationGateRef\.current;[\s\S]*?mutationGate\.cancel\(\)/);
  const deleteStart = source.indexOf("const confirmDelete");
  const deleteBody = source.slice(deleteStart, source.indexOf("  useEffect(() =>", deleteStart));
  assert.match(deleteBody, /beginMutation/);
  assert.match(deleteBody, /acceptsMutation/);
  assert.match(deleteBody, /finishMutationAndRefresh/);
  assert.doesNotMatch(deleteBody, /await loadLogs\(\)/);
  assert.match(source, /onClick=\{\(\) => void confirmDelete\(\)\} disabled=\{isDeleting/);
});

test("logs list aborts its request when the page owner is cleaned up", () => {
  assert.match(source, /logAbortControllerRef/);
  assert.match(source, /fetchSystemLogs\(\s*\{[\s\S]*?\},\s*abortController\.signal\s*,?\s*\)/);
  assert.match(source, /logAbortControllerRef\.current\?\.abort\(\);/);
  assert.match(source, /return \(\) => \{[\s\S]*?logAbortControllerRef\.current\?\.abort\(\);/);
  assert.match(source, /const mutationOwner = logMutationGateRef\.current\.beginMutation\(\);[\s\S]*?logAbortControllerRef\.current\?\.abort\(\);/);
});

test("a late logs query is dropped and the production coordinator reloads the current filter", () => {
  const gate = createMutationRequestGate();
  const oldQuery = gate.beginQuery("list");
  const mutation = gate.beginMutation();
  let currentFilter = "account";
  let rows = ["after-delete"];
  let loading = true;
  let reloads = [];

  currentFilter = "call";
  if (gate.acceptsQuery(oldQuery)) rows = ["stale-before-delete"];
  assert.deepEqual(rows, ["after-delete"]);

  assert.equal(finishMutationAndRefresh({
    gate,
    owner: mutation,
    onBusyChange: (busy) => { loading = busy; },
    reloadList: () => {
      const currentQuery = gate.beginQuery("list");
      assert.equal(currentQuery.allowed, true);
      reloads.push(currentFilter);
      if (gate.acceptsQuery(currentQuery)) loading = false;
    },
  }), true);

  assert.deepEqual(reloads, ["call"]);
  assert.equal(loading, false);
});

test("a second log delete is rejected without finishing the first owner", () => {
  const gate = createMutationRequestGate();
  const first = gate.beginMutation();
  const second = gate.beginMutation();
  let reloads = 0;

  assert.equal(first.accepted, true);
  assert.equal(second.accepted, false);
  assert.equal(gate.finishMutation(second), false);
  assert.equal(gate.isMutationActive(), true);
  assert.equal(finishMutationAndRefresh({
    gate,
    owner: first,
    onBusyChange: () => undefined,
    reloadList: () => { reloads += 1; },
  }), true);
  assert.equal(reloads, 1);
  assert.equal(gate.isMutationActive(), false);
});

test("an unmounted log delete owner cannot publish late success/error/finally effects", () => {
  const gate = createMutationRequestGate();
  const owner = gate.beginMutation();
  const events = [];

  gate.cancel();
  if (gate.acceptsMutation(owner)) events.push("success");
  if (gate.acceptsMutation(owner)) events.push("error");
  if (finishMutationAndRefresh({
    gate,
    owner,
    onBusyChange: () => events.push("busy"),
    reloadList: () => events.push("reload"),
  })) events.push("finish");

  assert.deepEqual(events, []);
  assert.equal(gate.isMutationActive(), false);
});
