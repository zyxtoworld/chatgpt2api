function deriveImageTags(items) {
  const seen = new Set();
  const tags = [];
  for (const item of items) {
    for (const tag of item.tags || []) {
      if (!seen.has(tag)) {
        seen.add(tag);
        tags.push(tag);
      }
    }
  }
  return tags;
}

function createAbortError() {
  if (typeof DOMException === "function") {
    return new DOMException("The operation was aborted", "AbortError");
  }
  const error = new Error("The operation was aborted");
  error.name = "AbortError";
  return error;
}

export async function loadManagedImagesWithTags(fetchImages, fetchTags, signal) {
  const controller = new AbortController();
  const abortFromParent = () => controller.abort();
  if (signal?.aborted) {
    controller.abort();
    throw createAbortError();
  }
  let removeParentAbortWaiter = () => {};
  const parentAbortPromise = signal
    ? new Promise((_, reject) => {
        const rejectOnAbort = () => reject(createAbortError());
        removeParentAbortWaiter = () => signal.removeEventListener("abort", rejectOnAbort);
        signal.addEventListener("abort", rejectOnAbort, { once: true });
      })
    : null;
  signal?.addEventListener("abort", abortFromParent, { once: true });
  try {
    const imagesPromise = fetchImages(controller.signal);
    const tagsPromise = Promise.resolve()
      .then(() => fetchTags(controller.signal))
      .catch(() => null);
    const waitForParentAbort = (promise) => parentAbortPromise
      ? Promise.race([promise, parentAbortPromise])
      : promise;
    let data;
    try {
      data = await waitForParentAbort(imagesPromise);
    } catch (error) {
      controller.abort();
      throw error;
    }
    if (signal?.aborted) {
      controller.abort();
      throw createAbortError();
    }
    const tagsData = await waitForParentAbort(tagsPromise);
    if (signal?.aborted) {
      controller.abort();
      throw createAbortError();
    }
    return {
      data,
      tags: tagsData ? tagsData.tags : deriveImageTags(data.items),
    };
  } catch (error) {
    controller.abort();
    throw error;
  } finally {
    removeParentAbortWaiter();
    signal?.removeEventListener("abort", abortFromParent);
  }
}
