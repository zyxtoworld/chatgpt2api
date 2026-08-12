export function createOwnedQueryLoader({
  gate,
  domain = "default",
  request,
  onStart,
  onCommit,
  onError,
  onFinish,
}) {
  let loadingOwner = null;

  const run = async () => {
    const queryOwner = gate.beginQuery(domain);
    if (!queryOwner.allowed) {
      return;
    }

    loadingOwner = queryOwner;
    onStart?.();
    try {
      const value = await request();
      if (gate.acceptsQuery(queryOwner)) {
        onCommit?.(value);
      }
    } catch (error) {
      if (gate.acceptsQuery(queryOwner)) {
        onError?.(error);
      }
    } finally {
      if (loadingOwner === queryOwner) {
        loadingOwner = null;
        onFinish?.();
      }
    }
  };

  return {
    run,
    clearLoadingForMutation() {
      if (loadingOwner === null) {
        return;
      }
      loadingOwner = null;
      onFinish?.();
    },
    cancel() {
      loadingOwner = null;
    },
  };
}

export function scheduleOwnedMicrotask(task, schedule = globalThis.queueMicrotask) {
  let active = true;
  schedule(() => {
    if (active) {
      void task();
    }
  });
  return () => {
    active = false;
  };
}
