import assert from "node:assert/strict";
import test from "node:test";

import { createMutationRequestGate } from "../src/lib/mutation-request-gate.js";
import { createOwnedQueryLoader, scheduleOwnedMicrotask } from "../src/lib/query-lifecycle.js";

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((nextResolve, nextReject) => {
    resolve = nextResolve;
    reject = nextReject;
  });
  return { promise, resolve, reject };
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

test("owned query cancellation aborts the request body owner", async () => {
  const gate = createMutationRequestGate();
  const pending = deferred();
  let requestSignal;
  const loader = createOwnedQueryLoader({
    gate,
    request: (signal) => {
      requestSignal = signal;
      return pending.promise;
    },
  });

  const run = loader.run();
  assert.ok(requestSignal instanceof AbortSignal);
  loader.cancel();
  assert.equal(requestSignal.aborted, true);
  pending.resolve("late");
  await run;
});

test("owned query cancellation suppresses a late request error", async () => {
  const gate = createMutationRequestGate();
  const pending = deferred();
  let errors = 0;
  const loader = createOwnedQueryLoader({
    gate,
    request: () => pending.promise,
    onError: () => {
      errors += 1;
    },
  });

  const run = loader.run();
  loader.cancel();
  pending.reject(new Error("late cancellation error"));
  await run;
  assert.equal(errors, 0);
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
