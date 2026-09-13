function uniqueEnabledIds(channels) {
  const seen = new Set();
  const ids = [];
  for (const channel of Array.isArray(channels) ? channels : []) {
    const id = channelIdText(channel?.id);
    if (!id || channel?.enabled !== true || seen.has(id)) {
      continue;
    }
    seen.add(id);
    ids.push(id);
  }
  return ids;
}

function scalarText(value) {
  if (typeof value === "string") return value.trim();
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return "";
}

function channelIdText(value) {
  let text = "";
  if (typeof value === "string") {
    text = value.trim();
  } else if (typeof value === "number" && Number.isSafeInteger(value) && value > 0) {
    text = String(value);
  }
  if (
    text.length === 0
    || text.length > 64
    || !/^[0-9]+$/.test(text)
    || !/[1-9]/.test(text)
  ) {
    return "";
  }
  return text;
}

export function resetCCLoadModelState() {
  return {
    loadingModelIds: [],
    modelLoadErrorIds: [],
  };
}

export function normalizeCCLoadChannels(channels) {
  const seen = new Set();
  return (Array.isArray(channels) ? channels : []).flatMap((channel) => {
    const id = channelIdText(channel?.id);
    if (!id || seen.has(id)) return [];
    seen.add(id);
    const modelIds = [];
    const seenModels = new Set();
    for (const value of Array.isArray(channel?.models) ? channel.models : []) {
      const modelId = typeof value === "string" ? value.trim() : "";
      if (!modelId || seenModels.has(modelId)) continue;
      seenModels.add(modelId);
      modelIds.push(modelId);
    }
    const normalized = {
      id,
      name: scalarText(channel?.name),
      enabled: channel?.enabled === true,
      plan_type: scalarText(channel?.plan_type),
      subscription_active_until: scalarText(channel?.subscription_active_until),
      models: modelIds,
      models_loaded: channel?.models_loaded === true,
    };
    return [normalized];
  });
}

export function getUnloadedCCLoadChannelIds(channels) {
  const ids = [];
  for (const channel of Array.isArray(channels) ? channels : []) {
    const id = channelIdText(channel?.id);
    if (!id || channel?.enabled !== true || channel?.models_loaded === true) continue;
    ids.push(id);
  }
  return ids;
}

export function mergeCCLoadChannelModels(channels, catalogs) {
  const normalizedCatalogs = normalizeCCLoadChannels(catalogs);
  const byId = new Map(normalizedCatalogs.map((channel) => [channel.id, channel]));
  return (Array.isArray(channels) ? channels : []).map((channel) => {
    const id = channelIdText(channel?.id);
    const directCatalog = byId.get(id);
    const catalog = directCatalog;
    if (!catalog) return channel;
    return {
      ...channel,
      models: catalog.models,
      models_loaded: catalog.models_loaded,
    };
  });
}

export function getCCLoadModelErrorIds(catalogs, requestedIds = []) {
  const returnedIds = new Set();
  const failedIds = [];
  for (const catalog of Array.isArray(catalogs) ? catalogs : []) {
    const id = channelIdText(catalog?.id);
    if (!id) continue;
    returnedIds.add(id);
    if (catalog?.models_loaded !== true) failedIds.push(id);
  }
  for (const rawId of Array.isArray(requestedIds) ? requestedIds : []) {
    const id = channelIdText(rawId);
    if (id && !returnedIds.has(id)) failedIds.push(id);
  }
  return [...new Set(failedIds)];
}

export function filterCCLoadChannels(channels, query) {
  const items = Array.isArray(channels) ? channels : [];
  const normalizedQuery = String(query || "").trim().toLowerCase();
  if (!normalizedQuery) {
    return items.slice();
  }
  return items.filter((channel) => {
    const searchable = [
      channel?.id,
      channel?.name,
      channel?.plan_type,
      channel?.subscription_active_until,
      ...(Array.isArray(channel?.models) ? channel.models : []),
    ]
      .map((value) => String(value || "").toLowerCase())
      .join(" ");
    return searchable.includes(normalizedQuery);
  });
}

export function getCCLoadPage(items, page, pageSize) {
  const values = Array.isArray(items) ? items : [];
  const size = Number.isInteger(Number(pageSize)) && Number(pageSize) > 0 ? Number(pageSize) : 1;
  const pageCount = Math.max(1, Math.ceil(values.length / size));
  const safePage = Math.min(Math.max(1, Number(page) || 1), pageCount);
  const startIndex = (safePage - 1) * size;
  return {
    items: values.slice(startIndex, startIndex + size),
    page: safePage,
    pageCount,
    total: values.length,
    start: values.length === 0 ? 0 : startIndex + 1,
    end: Math.min(startIndex + size, values.length),
  };
}

export function getSelectableCCLoadChannelIds(channels) {
  return uniqueEnabledIds(channels);
}

export function areAllCCLoadChannelsSelected(selectedIds, channels) {
  const selectableIds = uniqueEnabledIds(channels);
  if (selectableIds.length === 0) {
    return false;
  }
  const selected = new Set(Array.isArray(selectedIds) ? selectedIds : []);
  return selectableIds.every((id) => selected.has(id));
}

export function toggleAllCCLoadChannels(selectedIds, channels, checked) {
  const selectableIds = uniqueEnabledIds(channels);
  const current = Array.isArray(selectedIds) ? selectedIds : [];
  if (checked) {
    return [...new Set([...current, ...selectableIds])];
  }
  const selectable = new Set(selectableIds);
  return current.filter((id) => !selectable.has(id));
}

export function getValidCCLoadSelectedIds(selectedIds, channels) {
  const selectable = new Set(uniqueEnabledIds(channels));
  const seen = new Set();
  return (Array.isArray(selectedIds) ? selectedIds : []).filter((rawId) => {
    const id = channelIdText(rawId);
    if (!selectable.has(id) || seen.has(id)) {
      return false;
    }
    seen.add(id);
    return true;
  });
}
