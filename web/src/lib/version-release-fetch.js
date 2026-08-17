function createAbortError() {
  if (typeof DOMException === "function") {
    return new DOMException("The operation was aborted", "AbortError");
  }
  const error = new Error("The operation was aborted");
  error.name = "AbortError";
  return error;
}

async function cancelResponseBody(response) {
  const cancel = response?.body?.cancel;
  if (typeof cancel !== "function") return;
  try {
    await cancel.call(response.body);
  } catch {
    // The response is already being discarded; preserve the original error.
  }
}

/**
 * @param {Response} response
 * @param {number} maxBytes
 * @param {AbortSignal | undefined} signal
 */
async function readBoundedText(response, maxBytes, signal) {
  const rawLength = response?.headers?.get?.("content-length");
  if (rawLength != null) {
    const normalizedLength = String(rawLength).trim();
    if (!/^\d+$/.test(normalizedLength) || Number(normalizedLength) > maxBytes) {
      await cancelResponseBody(response);
      throw new Error("version response too large");
    }
  }

  const reader = response?.body?.getReader?.();
  if (!reader) {
    await cancelResponseBody(response);
    throw new Error("version response is not streamable");
  }

  let cancelled = Boolean(signal?.aborted);
  let cancelPromise;
  const cancel = () => {
    cancelPromise ||= Promise.resolve(reader.cancel?.()).catch(() => undefined);
    return cancelPromise;
  };
  const onAbort = () => {
    cancelled = true;
    void cancel();
  };
  signal?.addEventListener("abort", onAbort, { once: true });

  try {
    if (cancelled) {
      await cancel();
      throw createAbortError();
    }
    const decoder = new TextDecoder();
    let bytes = 0;
    let text = "";
    while (true) {
      const { done, value } = await reader.read();
      if (cancelled || signal?.aborted) {
        await cancel();
        throw createAbortError();
      }
      if (done) break;
      if (!(value instanceof Uint8Array)) {
        await cancel();
        throw new Error("version response is invalid");
      }
      bytes += value.byteLength;
      if (bytes > maxBytes) {
        await cancel();
        throw new Error("version response too large");
      }
      text += decoder.decode(value, { stream: true });
    }
    return text + decoder.decode();
  } catch (error) {
    await cancel();
    throw error;
  } finally {
    signal?.removeEventListener("abort", onAbort);
    reader.releaseLock?.();
  }
}

/**
 * @param {string} url
 * @param {{maxBytes: number, signal?: AbortSignal, fetchImpl?: typeof fetch}} options
 */
export async function fetchReleaseText(url, { maxBytes, signal, fetchImpl = fetch } = {}) {
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) {
    throw new Error("version response limit is invalid");
  }
  const response = await fetchImpl(url, { signal });
  if (!response?.ok) {
    await cancelResponseBody(response);
    throw new Error("version request failed");
  }
  return readBoundedText(response, maxBytes, signal);
}
