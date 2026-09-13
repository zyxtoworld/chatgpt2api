export function downloadBlobFile(
  blob,
  filename,
  {
    documentRef = globalThis.document,
    urlApi = globalThis.URL,
    schedule = globalThis.setTimeout,
  } = {},
) {
  const url = urlApi.createObjectURL(blob);
  let link;
  try {
    link = documentRef.createElement("a");
    link.href = url;
    link.download = filename;
  } catch (error) {
    schedule(() => urlApi.revokeObjectURL(url), 0);
    throw error;
  }
  try {
    link.click();
  } finally {
    schedule(() => urlApi.revokeObjectURL(url), 0);
  }
}

export function downloadTextFile(
  text,
  filename,
  {
    documentRef = globalThis.document,
    urlApi = globalThis.URL,
    blobCtor = globalThis.Blob,
    mimeType = "text/markdown;charset=utf-8",
    schedule = globalThis.setTimeout,
  } = {},
) {
  return downloadBlobFile(new blobCtor([text], { type: mimeType }), filename, {
    documentRef,
    urlApi,
    schedule,
  });
}
