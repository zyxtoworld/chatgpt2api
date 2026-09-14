async function cancelResponseBody(response) {
  const cancel = response?.body?.cancel;
  if (typeof cancel !== "function") return;
  try {
    await cancel.call(response.body);
  } catch {
    // The request is already being discarded; preserve the original error.
  }
}

function createAbortError() {
  if (typeof DOMException === "function") {
    return new DOMException("The operation was aborted", "AbortError");
  }
  const error = new Error("The operation was aborted");
  error.name = "AbortError";
  return error;
}

export async function fetchImageAsFile(url, fileName, signal) {
  const response = await fetch(url, { signal });
  let aborted = Boolean(signal?.aborted);
  let cancelPromise;
  const cancel = () => {
    cancelPromise ||= cancelResponseBody(response);
    return cancelPromise;
  };
  const onAbort = () => {
    aborted = true;
    void cancel();
  };
  signal?.addEventListener("abort", onAbort, { once: true });
  if (signal?.aborted && !aborted) {
    onAbort();
  }
  try {
    if (aborted) {
      await cancel();
      throw createAbortError();
    }
    if (!response.ok) {
      await cancel();
      throw new Error("读取结果图失败");
    }
    const blob = await response.blob();
    if (aborted || signal?.aborted) {
      await cancel();
      throw createAbortError();
    }
    return new File([blob], fileName, { type: blob.type || "image/png" });
  } finally {
    signal?.removeEventListener("abort", onAbort);
  }
}
