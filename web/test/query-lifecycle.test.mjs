import assert from "node:assert/strict";
import test from "node:test";

import { createMutationRequestGate } from "../src/lib/mutation-request-gate.js";
import { createOwnedQueryLoader, scheduleOwnedMicrotask } from "../src/lib/query-lifecycle.js";

function deferred() {
  let resolve;
  const promise = new Promise((nextResolve) => {
    resolve = nextResolve;
  });
  return { promise, resolve };
}

test("owned list loading clears on mutation and an old query cannot clear a newer owner", async () => {
  const gate = createMutationRequestGate();
  const pending = [];
  const committed = [];
  let loading = false;
  const loader = createOwnedQueryLoader({
    gate,
    domain: "list",
    request: () => {
      const request = deferred();
      pending.push(request);
      return request.promise;
    },
    onStart: () => {
      loading = true;
    },
    onCommit: (value) => {
      committed.push(value);
    },
    onFinish: () => {
      loading = false;
    },
  });

  const firstLoad = loader.run();
  assert.equal(loading, true);

  const mutation = gate.beginMutation();
  loader.clearLoadingForMutation();
  assert.equal(loading, false);
  assert.equal(gate.finishMutation(mutation), true);

  const secondLoad = loader.run();
  assert.equal(loading, true);

  pending[0].resolve({ id: "old" });
  await firstLoad;
  assert.equal(loading, true);
  assert.deepEqual(committed, []);

  pending[1].resolve({ id: "new" });
  await secondLoad;
  assert.equal(loading, false);
  assert.deepEqual(committed, [{ id: "new" }]);
});

test("a proxy-style save remains authoritative over a late initial settings read", async () => {
  const gate = createMutationRequestGate();
  const staleRead = deferred();
  let settings = "before";
  const loader = createOwnedQueryLoader({
    gate,
    request: () => staleRead.promise,
    onCommit: (value) => {
      settings = value;
    },
  });

  const initialLoad = loader.run();
  const mutation = gate.beginMutation();
  loader.clearLoadingForMutation();
  if (gate.acceptsMutation(mutation)) {
    settings = "saved";
  }
  assert.equal(gate.finishMutation(mutation), true);

  staleRead.resolve("stale");
  await initialLoad;
  assert.equal(settings, "saved");
});

test("owned setup microtasks do not run after cleanup and replay starts exactly once", () => {
  const callbacks = [];
  let runs = 0;
  const schedule = (callback) => {
    callbacks.push(callback);
    return callback;
  };
  const setup = () => scheduleOwnedMicrotask(() => {
    runs += 1;
  }, schedule);

  const cleanupFirst = setup();
  cleanupFirst();
  const cleanupSecond = setup();
  for (const callback of callbacks.splice(0)) callback();
  assert.equal(runs, 1);

  const cleanupUnmounted = setup();
  cleanupUnmounted();
  for (const callback of callbacks.splice(0)) callback();
  assert.equal(runs, 1);
  cleanupSecond();
});
