import { createSerialPoller } from "./serial-poll.js";

export function createEditableTaskPollingLifecycle({
  fetchTasks,
  onError,
  onPollingChange,
  schedule = globalThis.setTimeout,
  clear = globalThis.clearTimeout,
}) {
  let currentPoller = null;

  const stopCurrent = () => {
    currentPoller?.stop();
    currentPoller = null;
  };

  return {
    replace(ids) {
      stopCurrent();
      const taskIds = Array.from(new Set(ids.filter(Boolean)));
      onPollingChange?.(taskIds.length > 0);
      if (!taskIds.length) return;

      const poller = createSerialPoller({
        intervalMs: 5000,
        initialDelayMs: 5000,
        schedule,
        clear,
        poll: (signal) => fetchTasks(taskIds, signal),
        isDone: () => false,
        onProgress: () => undefined,
      });
      currentPoller = poller;
      void poller.start().catch((error) => onError?.(error));
    },
    dispose() {
      stopCurrent();
    },
  };
}
