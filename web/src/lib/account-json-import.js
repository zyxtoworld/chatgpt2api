function getRecord(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : null;
}

function getString(value) {
  return typeof value === "string" ? value.trim() : "";
}

function getSub2ApiAccount(value) {
  const raw = getRecord(value);
  const credentials = getRecord(raw?.credentials);
  if (!raw || !credentials) {
    return null;
  }

  const platform = getString(raw.platform).toLowerCase();
  if (platform && platform !== "openai") {
    return null;
  }

  const token = getString(credentials.access_token ?? credentials.accessToken);
  if (!token) {
    return null;
  }

  const extra = getRecord(raw.extra);
  const payload = {
    access_token: token,
    source_type: "codex",
  };
  const credentialFields = [
    "refresh_token",
    "id_token",
    "chatgpt_account_id",
    "chatgpt_user_id",
    "organization_id",
    "expires_at",
    "subscription_expires_at",
    "model_mapping",
  ];
  const accountFields = ["concurrency", "priority", "rate_multiplier", "auto_pause_on_expired"];

  for (const field of credentialFields) {
    if (credentials[field] !== undefined) {
      payload[field] = credentials[field];
    }
  }
  for (const field of accountFields) {
    if (raw[field] !== undefined) {
      payload[field] = raw[field];
    }
  }

  const email = getString(credentials.email) || getString(extra?.email) || getString(raw.name);
  if (email) {
    payload.email = email;
  }
  const planType = getString(credentials.plan_type) || getString(raw.plan_type);
  if (planType) {
    payload.type = planType;
  }

  return payload;
}

function getAccountJsonAccount(value) {
  const raw = getRecord(value);
  if (!raw) {
    return null;
  }

  const tokenValue = raw.access_token ?? raw.accessToken;
  const token = typeof tokenValue === "string" ? tokenValue.trim() : "";
  if (!token) {
    return getSub2ApiAccount(raw);
  }

  const payload = {
    ...raw,
    access_token: token,
    source_type: "codex",
  };
  delete payload.accessToken;
  if (payload.type === "codex") {
    payload.export_type = "codex";
    delete payload.type;
  }
  return payload;
}

export function getAccountJsonAccounts(value) {
  if (Array.isArray(value)) {
    return value.map(getAccountJsonAccount).filter(Boolean);
  }

  const singleAccount = getAccountJsonAccount(value);
  if (singleAccount) {
    return [singleAccount];
  }

  const raw = getRecord(value);
  const nested = raw?.accounts ?? raw?.items;
  if (Array.isArray(nested)) {
    return nested.map(getAccountJsonAccount).filter(Boolean);
  }

  return [];
}
