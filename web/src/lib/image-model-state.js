const IMAGE_MODEL_ID_PATTERN = /image/i;

export function filterImageModels(items) {
  return (Array.isArray(items) ? items : [])
    .map((item) => (typeof item?.id === "string" ? item.id.trim() : ""))
    .filter(
      (id, index, list) =>
        IMAGE_MODEL_ID_PATTERN.test(id) && list.indexOf(id) === index,
    );
}

export function resolveImageModelLoadSuccess(data) {
  const models = filterImageModels(data?.data);
  return {
    status: models.length > 0 ? "ready" : "empty",
    models,
  };
}

export function resolveImageModelLoadError() {
  return {
    status: "error",
    models: [],
  };
}

export function selectImageModel(value, models) {
  const availableModels = Array.isArray(models) ? models : [];
  const normalized = String(value || "").trim();
  return normalized && availableModels.includes(normalized)
    ? normalized
    : availableModels[0] || "";
}

export function canSubmitImage({ prompt, model, models, status }) {
  return (
    status === "ready" &&
    Boolean(String(prompt || "").trim()) &&
    Array.isArray(models) &&
    models.includes(model)
  );
}
