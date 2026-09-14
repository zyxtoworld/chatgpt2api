import assert from "node:assert/strict";
import test from "node:test";

import { createMutationRequestGate } from "../src/lib/mutation-request-gate.js";
import { createLatestActionOwner } from "../src/lib/latest-action-owner.js";

test("account list mutation admission rejects a second write and invalidates the old list query", () => {
  const gate = createMutationRequestGate();
  const firstQuery = gate.beginQuery("list");
  let listLoading = true;

  const firstMutation = gate.beginMutation();
  listLoading = false;
  assert.equal(firstMutation.accepted, true);
  assert.equal(listLoading, false);
  assert.equal(gate.acceptsQuery(firstQuery), false);

  const secondMutation = gate.beginMutation();
  assert.equal(secondMutation.accepted, false);
  assert.equal(gate.isMutationActive(), true);
  assert.equal(gate.finishMutation(secondMutation), false);
  assert.equal(gate.isMutationActive(), true);
  assert.equal(gate.finishMutation(firstMutation), true);
  assert.equal(gate.isMutationActive(), false);
});

test("a canceled account mutation cannot commit or reopen the gate after unmount", () => {
  const gate = createMutationRequestGate();
  const mutation = gate.beginMutation();
  let committed = false;

  gate.cancel();
  if (gate.acceptsMutation(mutation)) {
    committed = true;
  }

  assert.equal(committed, false);
  assert.equal(gate.finishMutation(mutation), false);
  assert.equal(gate.isMutationActive(), false);
});

test("an import action admits one write and drops its late completion after unmount", () => {
  const gate = createMutationRequestGate();
  const owner = createLatestActionOwner();
  const firstMutation = gate.beginMutation();
  const firstAction = owner.begin("account-import");
  let commits = 0;
  let notices = 0;

  const secondMutation = gate.beginMutation();
  assert.equal(secondMutation.accepted, false);

  owner.cancel();
  gate.cancel();
  if (owner.accepts(firstAction, "account-import") && gate.acceptsMutation(firstMutation)) {
    commits += 1;
    notices += 1;
  }

  assert.equal(commits, 0);
  assert.equal(notices, 0);
});

test("a rejected second import admission leaves the first action accepted", () => {
  const gate = createMutationRequestGate();
  const owner = createLatestActionOwner();
  const firstMutation = gate.beginMutation();
  const firstAction = owner.begin("account-import");

  const secondMutation = gate.beginMutation();
  assert.equal(secondMutation.accepted, false);
  assert.equal(owner.accepts(firstAction, "account-import"), true);
  assert.equal(gate.acceptsMutation(firstMutation), true);
});
