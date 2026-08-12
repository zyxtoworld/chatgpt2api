import assert from "node:assert/strict";
import test from "node:test";

import { createMutationRequestGate } from "../src/lib/mutation-request-gate.js";

test("mutation blocks new queries and invalidates an in-flight query", () => {
  const gate = createMutationRequestGate();
  const oldQuery = gate.beginQuery();
  const mutation = gate.beginMutation();
  const blockedQuery = gate.beginQuery();

  assert.equal(blockedQuery.allowed, false);
  assert.equal(gate.acceptsQuery(oldQuery), false);
  assert.equal(gate.acceptsMutation(mutation), true);

  assert.equal(gate.finishMutation(mutation), true);
  assert.equal(gate.isMutationActive(), false);
  assert.equal(gate.acceptsQuery(blockedQuery), false);
});

test("a late query cannot overwrite a newer manual query", () => {
  const gate = createMutationRequestGate();
  const backgroundQuery = gate.beginQuery();
  const manualQuery = gate.beginQuery();

  assert.equal(gate.acceptsQuery(backgroundQuery), false);
  assert.equal(gate.acceptsQuery(manualQuery), true);
});

test("independent query domains do not invalidate each other", () => {
  const gate = createMutationRequestGate();
  const filesQuery = gate.beginQuery("files");
  const listQuery = gate.beginQuery("list");
  const newerFilesQuery = gate.beginQuery("files");

  assert.equal(gate.acceptsQuery(filesQuery), false);
  assert.equal(gate.acceptsQuery(listQuery), true);
  assert.equal(gate.acceptsQuery(newerFilesQuery), true);

  const mutation = gate.beginMutation();
  assert.equal(gate.acceptsQuery(listQuery), false);
  assert.equal(gate.acceptsQuery(newerFilesQuery), false);
  assert.equal(gate.finishMutation(mutation), true);
});

test("a concurrent mutation is rejected without clearing the current owner", () => {
  const gate = createMutationRequestGate();
  const first = gate.beginMutation();
  const second = gate.beginMutation();

  assert.equal(first.accepted, true);
  assert.equal(second.accepted, false);
  assert.equal(gate.acceptsMutation(first), true);
  assert.equal(gate.acceptsMutation(second), false);
  assert.equal(gate.finishMutation(second), false);
  assert.equal(gate.isMutationActive(), true);
  assert.equal(gate.finishMutation(first), true);
  assert.equal(gate.isMutationActive(), false);
});

test("rapid write admission starts only one non-cancellable operation", () => {
  const gate = createMutationRequestGate();
  let writesStarted = 0;

  const first = gate.beginMutation();
  if (first.accepted) writesStarted += 1;
  const second = gate.beginMutation();
  if (second.accepted) writesStarted += 1;

  assert.equal(writesStarted, 1);
  assert.equal(gate.finishMutation(first), true);
  assert.equal(gate.beginMutation().accepted, true);
});

test("cancel invalidates both a mutation and its pending queries", () => {
  const gate = createMutationRequestGate();
  const query = gate.beginQuery();
  const mutation = gate.beginMutation();

  gate.cancel();

  assert.equal(gate.acceptsQuery(query), false);
  assert.equal(gate.acceptsMutation(mutation), false);
  assert.equal(gate.isMutationActive(), false);
});

test("a setup cancelled during cleanup can start a fresh query on re-setup", () => {
  const gate = createMutationRequestGate();
  const firstSetupQuery = gate.beginQuery();

  gate.cancel();

  const secondSetupQuery = gate.beginQuery();
  assert.equal(gate.acceptsQuery(firstSetupQuery), false);
  assert.equal(secondSetupQuery.allowed, true);
  assert.equal(gate.acceptsQuery(secondSetupQuery), true);
});
