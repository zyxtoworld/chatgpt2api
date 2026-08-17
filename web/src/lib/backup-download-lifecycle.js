import { createDownloadAbortRegistry } from "./download-lifecycle.js";
import { createLifecycleActionOwner } from "./lifecycle-action-owner.js";

export function createAbortableLifecycleOwner() {
  const lifecycleOwner = createLifecycleActionOwner();
  const abortRegistry = createDownloadAbortRegistry();

  return {
    activate() {
      lifecycleOwner.activate();
      abortRegistry.activate();
    },

    begin() {
      return {
        action: lifecycleOwner.begin(),
        controller: abortRegistry.begin(),
      };
    },

    accepts(download) {
      return lifecycleOwner.accepts(download?.action);
    },

    finish(download) {
      abortRegistry.finish(download?.controller);
    },

    cancel() {
      lifecycleOwner.cancel();
      abortRegistry.cancel();
    },

    activeCount() {
      return abortRegistry.activeCount();
    },
  };
}

export async function runBackupDownload({
  owner,
  getAuthKey,
  fetchImpl = globalThis.fetch,
  url,
  fallbackName,
  filenameFromContentDisposition,
  requestErrorMessage,
  onDownload,
  onSuccess,
  onError,
}) {
  const downloadOwner = owner.begin();
  try {
    const authKey = await getAuthKey();
    if (!owner.accepts(downloadOwner)) {
      return;
    }
    if (!authKey) {
      throw new Error("当前登录态已失效，请重新登录后再下载");
    }

    const response = await fetchImpl(url, {
      headers: { Authorization: `Bearer ${authKey}` },
      signal: downloadOwner.controller.signal,
    });
    if (!owner.accepts(downloadOwner)) {
      await releaseStaleResponse(response);
      return;
    }

    if (!response.ok) {
      let message = "下载备份失败";
      try {
        const data = await response.json();
        if (!owner.accepts(downloadOwner)) {
          return;
        }
        message = requestErrorMessage({ status: response.status, payload: data });
      } catch {
        if (!owner.accepts(downloadOwner)) {
          return;
        }
        message = response.status === 401 ? "登录已失效，请重新登录后再试" : message;
      }
      throw new Error(message);
    }

    const downloadName = filenameFromContentDisposition(response.headers.get("Content-Disposition")) || fallbackName || "backup.bin";
    const blob = await response.blob();
    if (!owner.accepts(downloadOwner)) {
      return;
    }
    onDownload(blob, downloadName);
    if (owner.accepts(downloadOwner)) {
      onSuccess();
    }
  } catch (error) {
    if (owner.accepts(downloadOwner)) {
      onError(error);
    }
  } finally {
    owner.finish(downloadOwner);
  }
}

export async function releaseStaleResponse(response) {
  const body = response?.body;
  if (!body || typeof body.cancel !== "function") {
    return;
  }
  try {
    await body.cancel();
  } catch {
    // The owner is already stale; there is no live UI action to report this cleanup failure to.
  }
}
