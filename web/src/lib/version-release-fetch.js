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
  let rejectAbort;
  const abortPromise = signal
    ? new Promise((_, reject) => {
        rejectAbort = reject;
      })
    : null;
  const cancel = () => {
    cancelPromise ||= Promise.resolve(reader.cancel?.()).catch(() => undefined);
    return cancelPromise;
  };
  const onAbort = () => {
    cancelled = true;
    void cancel();
    rejectAbort?.(createAbortError());
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
      const { done, value } = await (
        abortPromise ? Promise.race([reader.read(), abortPromise]) : reader.read()
      );
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
  if (signal?.aborted) throw createAbortError();
  const response = await fetchImpl(url, { signal });
  if (!response?.ok) {
    await cancelResponseBody(response);
    throw new Error("version request failed");
  }
  return readBoundedText(response, maxBytes, signal);
}

/**
 * Fetch the version and changelog as one cancellable operation. If either
 * request fails, the sibling is cancelled before the original error escapes.
 *
 * @param {string} versionUrl
 * @param {string} changelogUrl
 * @param {{versionMaxBytes: number, changelogMaxBytes: number, signal?: AbortSignal, fetchImpl?: typeof fetch}} options
 */
export async function fetchReleaseBundle(
  versionUrl,
  changelogUrl,
  { versionMaxBytes, changelogMaxBytes, signal, fetchImpl = fetch } = {},
) {
  const requestController = new AbortController();
  if (signal?.aborted) throw createAbortError();

  let rejectParentAbort;
  const parentAbortPromise = signal
    ? new Promise((_, reject) => {
        rejectParentAbort = reject;
      })
    : null;
  const forwardAbort = () => {
    requestController.abort();
    rejectParentAbort?.(createAbortError());
  };
  signal?.addEventListener("abort", forwardAbort, { once: true });

  try {
    const requests = Promise.all([
      fetchReleaseText(versionUrl, {
        maxBytes: versionMaxBytes,
        signal: requestController.signal,
        fetchImpl,
      }),
      fetchReleaseText(changelogUrl, {
        maxBytes: changelogMaxBytes,
        signal: requestController.signal,
        fetchImpl,
      }),
    ]);
    return await (parentAbortPromise ? Promise.race([requests, parentAbortPromise]) : requests);
  } catch (error) {
    requestController.abort();
    throw error;
  } finally {
    signal?.removeEventListener("abort", forwardAbort);
  }
}
