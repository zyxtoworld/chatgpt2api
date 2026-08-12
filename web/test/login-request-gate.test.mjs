import assert from "node:assert/strict";
import test from "node:test";

import { createLoginRequestGate } from "../src/lib/login-request-gate.js";

test("a newer login owns presentation and auth persistence before either response returns", () => {
  let authEpoch = 0;
  const gate = createLoginRequestGate(() => ({ epoch: ++authEpoch }));

  const first = gate.begin("first-key");
  const second = gate.begin("second-key");

  assert.deepEqual(first.authLease, { epoch: 1 });
  assert.deepEqual(second.authLease, { epoch: 2 });
  assert.equal(gate.accepts(first), false);
  assert.equal(gate.finish(first), false);
  assert.equal(gate.accepts(second), true);
  assert.equal(gate.finish(second), true);
  assert.equal(gate.accepts(second), false);
});

test("login cleanup fences persistence and rejects every late UI callback", () => {
  let authEpoch = 0;
  const gate = createLoginRequestGate(() => ({ epoch: ++authEpoch }));
  const pending = gate.begin("pending-key");

  gate.cancel();

  assert.equal(authEpoch, 2);
  assert.equal(gate.accepts(pending), false);
  assert.equal(gate.finish(pending), false);
  assert.equal(gate.accepts(gate.begin("after-cancel")), false);
});

test("effect setup replay accepts a new login without reviving the old owner", () => {
  let authEpoch = 0;
  const gate = createLoginRequestGate(() => ({ epoch: ++authEpoch }));
  const stale = gate.begin("stale-key");

  gate.cancel();
  gate.activate();
  const current = gate.begin("current-key");

  assert.equal(gate.accepts(stale), false);
  assert.equal(gate.accepts(current), true);
  assert.deepEqual(current.authLease, { epoch: 3 });
});
