import assert from "node:assert/strict";
import test from "node:test";

import { createRequestGate } from "../src/lib/query-request-gate.js";

test("a stale mutation refresh cannot commit after the query changes", () => {
  const gate = createRequestGate("A");
  const pendingA = gate.begin("A");

  gate.setQuery("B");
  const loadedB = gate.begin("B");
  const staleRefreshA = gate.begin("A");

  assert.equal(gate.isCurrent(pendingA), false);
  assert.equal(gate.isCurrent(staleRefreshA), false);
  assert.equal(gate.isCurrent(loadedB), true);
});
