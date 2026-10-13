// @ts-nocheck
export interface QueryGate {
  beginQuery(domain: string): { allowed: boolean };
  acceptsQuery(owner: unknown): boolean;
}

export interface OwnedQueryLoaderOptions<T> {
  gate: QueryGate;
  domain?: string;
  request: (signal: AbortSignal) => Promise<T>;
  onStart?: () => void;
  onCommit?: (value: T) => void;
  onError?: (error: unknown) => void;
  onFinish?: () => void;
}

export interface OwnedQueryLoader {
  run(): Promise<void>;
  clearLoadingForMutation(): void;
  cancel(): void;
}

export function createOwnedQueryLoader<T>(options: OwnedQueryLoaderOptions<T>): OwnedQueryLoader {
  const { gate, domain = "default", request, onStart, onCommit, onError, onFinish } = options;
  let loadingOwner = null;
  let requestController = null;

  const run = async () => {
    const queryOwner = gate.beginQuery(domain);
    if (!queryOwner.allowed) {
      return;
    }

    requestController?.abort();
    const controller = new AbortController();
    requestController = controller;
    loadingOwner = queryOwner;
    const ownsQuery = () => loadingOwner === queryOwner;
    onStart?.();
    try {
      const value = await request(controller.signal);
      if (ownsQuery() && gate.acceptsQuery(queryOwner)) {
        onCommit?.(value);
      }
    } catch (error) {
      if (ownsQuery() && gate.acceptsQuery(queryOwner)) {
        onError?.(error);
      }
    } finally {
      if (loadingOwner === queryOwner) {
        loadingOwner = null;
        onFinish?.();
      }
      if (requestController === controller) {
        requestController = null;
      }
    }
  };

  return {
    run,
    clearLoadingForMutation() {
      requestController?.abort();
      if (loadingOwner === null) {
        return;
      }
      loadingOwner = null;
      onFinish?.();
    },
    cancel() {
      requestController?.abort();
      requestController = null;
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
