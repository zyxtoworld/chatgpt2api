export function createSerialPoller({
  poll,
  isDone,
  onProgress,
  intervalMs = 500,
  initialDelayMs = 0,
  schedule = globalThis.setTimeout,
  clear = globalThis.clearTimeout,
}) {
  let started = false;
  let stopped = false;
  let settled = false;
  let timer = null;
  let inFlight = false;
  let resolveResult;
  let rejectResult;

  const result = new Promise((resolve, reject) => {
    resolveResult = resolve;
    rejectResult = reject;
  });

  const settleDone = (value) => {
    if (settled) return;
    settled = true;
    resolveResult({ status: "done", value });
  };

  const settleStopped = () => {
    if (settled) return;
    settled = true;
    resolveResult({ status: "stopped" });
  };

  const settleError = (error) => {
    if (settled) return;
    settled = true;
    rejectResult(error);
  };

  const tick = async () => {
    if (stopped || settled || inFlight) return;

    inFlight = true;
    let scheduleNext = false;
    try {
      const value = await poll();
      if (stopped || settled) return;
      if (isDone(value)) {
        settleDone(value);
      } else {
        onProgress?.(value);
        scheduleNext = true;
      }
    } catch (error) {
      if (!stopped && !settled) {
        settleError(error);
      }
    } finally {
      inFlight = false;
      if (scheduleNext && !stopped && !settled) {
        timer = schedule(() => {
          timer = null;
          void tick();
        }, intervalMs);
      }
    }
  };

  return {
    start() {
      if (!started) {
        started = true;
        if (initialDelayMs > 0) {
          timer = schedule(() => {
            timer = null;
            void tick();
          }, initialDelayMs);
        } else {
          void tick();
        }
      }
      return result;
    },
    stop() {
      if (stopped || settled) return;
      stopped = true;
      if (timer !== null) {
        clear(timer);
        timer = null;
      }
      settleStopped();
    },
  };
}

export function createCancelableProgress({
  total,
  intervalMs = 150,
  timeoutMs = 2000,
  onProgress,
  schedule = globalThis.setTimeout,
  clear = globalThis.clearTimeout,
}) {
  let started = false;
  let settled = false;
  let stopped = false;
  let current = 0;
  let tickTimer = null;
  let timeoutTimer = null;
  let resolveResult;

  const result = new Promise((resolve) => {
    resolveResult = resolve;
  });

  const clearTimers = () => {
    if (tickTimer !== null) {
      clear(tickTimer);
      tickTimer = null;
    }
    if (timeoutTimer !== null) {
      clear(timeoutTimer);
      timeoutTimer = null;
    }
  };

  const settle = (status) => {
    if (settled) return;
    settled = true;
    clearTimers();
    resolveResult({ status, current });
  };

  const scheduleTick = () => {
    if (stopped || settled) return;
    tickTimer = schedule(() => {
      tickTimer = null;
      if (stopped || settled) return;
      current += 1;
      if (current >= total) {
        settle("done");
        return;
      }
      onProgress?.(current);
      scheduleTick();
    }, intervalMs);
  };

  return {
    start() {
      if (!started) {
        started = true;
        if (total <= 0) {
          settle("done");
        } else {
          scheduleTick();
          timeoutTimer = schedule(() => settle("timeout"), timeoutMs);
        }
      }
      return result;
    },
    stop() {
      if (stopped || settled) return;
      stopped = true;
      settle("stopped");
    },
  };
}

export function isProgressTerminal(status) {
  return status === "done" || status === "timeout";
}
