export function createLifecycleActionOwner() {
  let epoch = 0;
  let nextToken = 0;
  let active = true;

  return {
    begin() {
      return { epoch, token: ++nextToken };
    },

    accepts(action) {
      return Boolean(active && action && action.epoch === epoch);
    },

    invalidate() {
      epoch += 1;
    },

    cancel() {
      active = false;
      epoch += 1;
    },

    activate() {
      active = true;
      epoch += 1;
    },
  };
}

export async function observeLifecycleAction(owner, operation, handlers = {}) {
  const action = owner.begin();
  try {
    const value = await operation();
    if (owner.accepts(action)) {
      handlers.onSuccess?.(value);
    }
  } catch (error) {
    if (owner.accepts(action)) {
      handlers.onError?.(error);
    }
  }
}
