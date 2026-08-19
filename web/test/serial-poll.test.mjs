import assert from "node:assert/strict";
import test from "node:test";

import { createCancelableProgress, createSerialPoller, isProgressTerminal } from "../src/lib/serial-poll.js";
import { createEditableTaskPollingLifecycle } from "../src/lib/editable-task-polling.js";

function deferred() {
  let resolve;
  const promise = new Promise((nextResolve) => {
    resolve = nextResolve;
  });
  return { promise, resolve };
}

function manualScheduler() {
  const pending = [];
  return {
    schedule(callback) {
      pending.push(callback);
      return callback;
    },
    clear(handle) {
      const index = pending.indexOf(handle);
      if (index >= 0) pending.splice(index, 1);
    },
    runNext() {
      const callback = pending.shift();
      assert.ok(callback);
      callback();
    },
    size() {
      return pending.length;
    },
  };
}

test("serial polling waits for settle before scheduling the next request", async () => {
  const scheduler = manualScheduler();
  const first = deferred();
  const second = deferred();
  let calls = 0;
  const runner = createSerialPoller({
    intervalMs: 10,
    schedule: scheduler.schedule,
    clear: scheduler.clear,
    poll: () => {
      calls += 1;
      return calls === 1 ? first.promise : second.promise;
    },
    isDone: () => false,
  });

  const result = runner.start();
  assert.equal(calls, 1);
  first.resolve({ done: false });
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(calls, 1);
  assert.equal(scheduler.size(), 1);

  scheduler.runNext();
  await Promise.resolve();
  assert.equal(calls, 2);

  runner.stop();
  assert.equal((await result).status, "stopped");
  second.resolve({ done: false });
});

test("stop during an in-flight request settles and suppresses the late result", async () => {
  const request = deferred();
  let requestSignal;
  let progressCalls = 0;
  const runner = createSerialPoller({
    intervalMs: 10,
    poll: (signal) => {
      requestSignal = signal;
      return request.promise;
    },
    isDone: () => false,
    onProgress: () => {
      progressCalls += 1;
    },
  });

  const result = runner.start();
  runner.stop();
  assert.equal(requestSignal.aborted, true);
  assert.equal((await result).status, "stopped");

  request.resolve({ done: true });
  await Promise.resolve();
  assert.equal(progressCalls, 0);
});

test("poller stop aborts the pending request owner before a late result can publish", async () => {
  const request = deferred();
  let requestSignal;
  let published = false;
  const runner = createSerialPoller({
    poll: (pollSignal) => {
      requestSignal = pollSignal;
      return request.promise.then((value) => {
        if (!pollSignal.aborted) published = true;
        return value;
      });
    },
    isDone: () => false,
    onProgress: () => { published = true; },
  });

  void runner.start();
  await new Promise((resolve) => queueMicrotask(resolve));
  assert.ok(requestSignal);
  runner.stop();
  request.resolve({ done: false });
  await new Promise((resolve) => queueMicrotask(resolve));
  assert.equal(requestSignal.aborted, true);
  assert.equal(published, false);
});

test("editable task production lifecycle aborts pending fetchTasks on replace and dispose", async () => {
  for (const cleanup of ["runningIds effect cleanup", "component root cleanup"]) {
    let requestSignal;
    const published = [];
    const pollingStates = [];
    const request = deferred();
    const scheduled = [];
    const lifecycle = createEditableTaskPollingLifecycle({
      fetchTasks: (ids, signal) => {
        requestSignal = signal;
        return request.promise.then((value) => {
          if (!signal.aborted) published.push({ ids, value });
          return value;
        });
      },
      onPollingChange: (nextPolling) => pollingStates.push(nextPolling),
      schedule: (callback) => {
        scheduled.push(callback);
        return callback;
      },
      clear: (callback) => {
        const index = scheduled.indexOf(callback);
        if (index >= 0) scheduled.splice(index, 1);
      },
    });

    lifecycle.replace(["running-task"]);
    assert.equal(scheduled.length, 1, `${cleanup} schedules the production five-second poll`);
    scheduled.shift()();
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.ok(requestSignal, `${cleanup} entered fetchTasks through the production lifecycle`);
    if (cleanup === "runningIds effect cleanup") lifecycle.replace([]);
    else lifecycle.dispose();
    if (cleanup === "runningIds effect cleanup") assert.deepEqual(pollingStates, [true, false]);
    else assert.deepEqual(pollingStates, [true]);
    request.resolve({ done: false });
    await new Promise((resolve) => queueMicrotask(resolve));
    assert.equal(requestSignal.aborted, true, `${cleanup} aborts the request signal`);
    assert.deepEqual(published, [], `${cleanup} ignores the late request result`);
  }
});

test("initial polling delay is cancellable", async () => {
  const scheduler = manualScheduler();
  let calls = 0;
  const runner = createSerialPoller({
    initialDelayMs: 10,
    schedule: scheduler.schedule,
    clear: scheduler.clear,
    poll: () => {
      calls += 1;
      return Promise.resolve({ done: false });
    },
    isDone: () => false,
  });

  const result = runner.start();
  assert.equal(calls, 0);
  assert.equal(scheduler.size(), 1);
  runner.stop();
  assert.equal((await result).status, "stopped");
  assert.equal(calls, 0);
  assert.equal(scheduler.size(), 0);
});

test("cancelable progress lets completion win and clears its timeout", async () => {
  const scheduler = manualScheduler();
  const progress = createCancelableProgress({
    total: 1,
    intervalMs: 10,
    timeoutMs: 20,
    schedule: scheduler.schedule,
    clear: scheduler.clear,
  });

  const result = progress.start();
  scheduler.runNext();
  const completionResult = await result;
  assert.deepEqual(completionResult, { status: "done", current: 1 });
  assert.equal(isProgressTerminal(completionResult.status), true);
  assert.equal(scheduler.size(), 0);
});

test("cancelable progress timeout stops later ticks and stop suppresses callbacks", async () => {
  const scheduler = manualScheduler();
  let progressCalls = 0;
  const progress = createCancelableProgress({
    total: 20,
    intervalMs: 10,
    timeoutMs: 20,
    onProgress: () => {
      progressCalls += 1;
    },
    schedule: scheduler.schedule,
    clear: scheduler.clear,
  });

  const timedOut = progress.start();
  scheduler.runNext();
  scheduler.runNext();
  const timeoutResult = await timedOut;
  assert.deepEqual(timeoutResult, { status: "timeout", current: 1 });
  assert.equal(isProgressTerminal(timeoutResult.status), true);
  assert.equal(isProgressTerminal("stopped"), false);
  assert.equal(scheduler.size(), 0);

  progressCalls = 0;
  const stopped = createCancelableProgress({
    total: 20,
    schedule: scheduler.schedule,
    clear: scheduler.clear,
    onProgress: () => {
      progressCalls += 1;
    },
  });
  const stoppedResult = stopped.start();
  stopped.stop();
  assert.deepEqual(await stoppedResult, { status: "stopped", current: 0 });
  assert.equal(scheduler.size(), 0);
  assert.equal(progressCalls, 0);
});
