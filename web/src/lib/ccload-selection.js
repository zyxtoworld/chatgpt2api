function uniqueEnabledIds(channels) {
  const seen = new Set();
  const ids = [];
  for (const channel of Array.isArray(channels) ? channels : []) {
    const id = String(channel?.id || "").trim();
    if (!id || channel?.enabled !== true || seen.has(id)) {
      continue;
    }
    seen.add(id);
    ids.push(id);
  }
  return ids;
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
    const id = String(rawId || "").trim();
    if (!selectable.has(id) || seen.has(id)) {
      return false;
    }
    seen.add(id);
    return true;
  });
}
