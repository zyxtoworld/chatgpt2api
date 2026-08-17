function scalarText(value) {
  return typeof value === "string" ? value.trim() : "";
}

export function normalizeSub2APIRemoteAccounts(items) {
  const seen = new Set();
  const accounts = [];
  for (const item of Array.isArray(items) ? items : []) {
    const id = scalarText(item?.id);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    accounts.push({
      id,
      name: scalarText(item?.name),
      email: scalarText(item?.email),
      plan_type: scalarText(item?.plan_type),
      status: scalarText(item?.status),
      expires_at: scalarText(item?.expires_at),
      has_refresh_token: item?.has_refresh_token === true,
    });
  }
  return accounts;
}
