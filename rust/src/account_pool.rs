use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use tokio::sync::Mutex;

use super::{ApiError, AppInitError, read_account_snapshot};

static USAGE_MARK_FAILURES: AtomicUsize = AtomicUsize::new(0);

pub(super) type AccountModelGroup = String;

fn is_request_eligible_status(status: &str) -> bool {
    !matches!(status, "禁用" | "限流" | "异常")
}

#[derive(Clone, Debug)]
pub(super) struct AccountRecord {
    pub(super) token: String,
    pub(super) status: String,
    pub(super) source_type: String,
    pub(super) chatgpt_account_id: Option<String>,
    pub(super) account_type: String,
    pub(super) models: Vec<String>,
    pub(super) raw: serde_json::Value,
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub(super) struct CatalogAccountCandidate {
    pub(super) token: String,
    pub(super) source_type: String,
    pub(super) chatgpt_account_id: Option<String>,
}

pub(super) struct AccountSlot {
    pub(super) record: AccountRecord,
    pub(super) inflight: AtomicUsize,
    last_used_at: RwLock<Option<String>>,
}

#[derive(Clone)]
pub(super) struct AccountSnapshot {
    pub(super) generation: u64,
    pub(super) fingerprint: [u8; 32],
    pub(super) valid: bool,
    pub(super) accounts: Arc<Vec<Arc<AccountSlot>>>,
}

pub(super) struct AccountLease {
    pub(super) slot: Arc<AccountSlot>,
}

impl AccountLease {
    pub(super) fn token(&self) -> &str {
        &self.slot.record.token
    }

    pub(super) fn source_type(&self) -> &str {
        &self.slot.record.source_type
    }

    pub(super) fn account_type(&self) -> &str {
        &self.slot.record.account_type
    }

    pub(super) fn chatgpt_account_id(&self) -> Option<&str> {
        self.slot.record.chatgpt_account_id.as_deref()
    }

    pub(super) fn proxy_url(&self) -> Option<&str> {
        self.slot
            .record
            .raw
            .get("proxy")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }
}

impl Drop for AccountLease {
    fn drop(&mut self) {
        let _ = self
            .slot
            .inflight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_sub(1)
            });
    }
}

#[derive(Clone)]
pub(super) struct AccountStore {
    pub(super) path: Option<Arc<PathBuf>>,
    pub(super) snapshot: Arc<RwLock<AccountSnapshot>>,
    reload_gate: Arc<Mutex<()>>,
    mutation_gate: Arc<Mutex<()>>,
    pub(super) cursor: Arc<AtomicUsize>,
}

impl AccountStore {
    pub(super) fn load(path: Option<&Path>) -> Result<Self, AppInitError> {
        let (records, fingerprint) = if let Some(path) = path {
            read_account_snapshot(path)?
        } else {
            (Vec::new(), [0; 32])
        };
        let accounts = Arc::new(account_slots(records));
        Ok(Self {
            path: path.map(|path| Arc::new(path.to_owned())),
            snapshot: Arc::new(RwLock::new(AccountSnapshot {
                generation: 0,
                fingerprint,
                valid: true,
                accounts,
            })),
            reload_gate: Arc::new(Mutex::new(())),
            mutation_gate: Arc::new(Mutex::new(())),
            cursor: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub(super) async fn reload(&self) -> bool {
        let Some(path) = self.path.clone() else {
            return true;
        };
        let _reload_guard = self.reload_gate.lock().await;
        let result = tokio::task::spawn_blocking(move || read_account_snapshot(&path)).await;
        let mut snapshot = self.snapshot.write().expect("account snapshot lock");
        match result {
            Ok(Ok((accounts, fingerprint))) => {
                if !snapshot.valid || snapshot.fingerprint != fingerprint {
                    snapshot.generation = snapshot.generation.saturating_add(1);
                    snapshot.fingerprint = fingerprint;
                    snapshot.accounts = Arc::new(account_slots_with_runtime_state(
                        accounts,
                        Some(snapshot.accounts.as_ref()),
                    ));
                    snapshot.valid = true;
                }
                true
            }
            _ => {
                snapshot.generation = snapshot.generation.saturating_add(1);
                snapshot.valid = false;
                snapshot.accounts = Arc::new(Vec::new());
                false
            }
        }
    }

    #[cfg(test)]
    pub(super) async fn acquire_excluding_with_type_filter(
        &self,
        model: &str,
        excluded_tokens: &HashSet<String>,
        allowed_groups: Option<&HashSet<AccountModelGroup>>,
    ) -> Option<AccountLease> {
        self.acquire_excluding_with_type_and_source_filter(
            model,
            excluded_tokens,
            allowed_groups,
            None,
        )
        .await
    }

    pub(super) async fn acquire_excluding_with_type_and_source_filter(
        &self,
        model: &str,
        excluded_tokens: &HashSet<String>,
        allowed_groups: Option<&HashSet<AccountModelGroup>>,
        required_source_type: Option<&str>,
    ) -> Option<AccountLease> {
        self.acquire_filtered(
            model,
            excluded_tokens,
            allowed_groups,
            required_source_type,
            None,
        )
        .await
    }

    pub(super) async fn acquire_exact_with_type_and_source_filter(
        &self,
        model: &str,
        token: &str,
        allowed_groups: Option<&HashSet<AccountModelGroup>>,
    ) -> Option<AccountLease> {
        if token.is_empty() || !self.reload().await {
            return None;
        }
        let excluded_tokens = self
            .records()
            .into_iter()
            .map(|record| record.token)
            .filter(|candidate| candidate != token)
            .collect::<HashSet<_>>();
        self.acquire_excluding_with_type_and_source_filter(
            model,
            &excluded_tokens,
            allowed_groups,
            Some("codex"),
        )
        .await
    }

    pub(super) async fn acquire_excluding_with_type_and_capability_filter(
        &self,
        model: &str,
        excluded_tokens: &HashSet<String>,
        allowed_groups: Option<&HashSet<AccountModelGroup>>,
        required_capability: Option<&str>,
    ) -> Option<AccountLease> {
        self.acquire_filtered(
            model,
            excluded_tokens,
            allowed_groups,
            None,
            required_capability,
        )
        .await
    }

    async fn acquire_filtered(
        &self,
        model: &str,
        excluded_tokens: &HashSet<String>,
        allowed_groups: Option<&HashSet<AccountModelGroup>>,
        required_source_type: Option<&str>,
        required_capability: Option<&str>,
    ) -> Option<AccountLease> {
        if !self.reload().await {
            return None;
        }
        let snapshot = self.snapshot.read().expect("account snapshot lock");
        if !snapshot.valid || snapshot.accounts.is_empty() {
            return None;
        }
        let accounts = snapshot.accounts.as_ref();
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        for offset in 0..accounts.len() {
            let slot = accounts[(start.wrapping_add(offset)) % accounts.len()].clone();
            if excluded_tokens.contains(&slot.record.token)
                || required_source_type.is_some_and(|source| slot.record.source_type != source)
                || required_capability.is_some_and(|capability| {
                    let matches_capability = match capability {
                        "codex" => slot.record.source_type == "codex",
                        "web" => matches!(
                            slot.record.source_type.as_str(),
                            "web" | "password" | "password-oauth"
                        ),
                        _ => false,
                    };
                    !matches_capability
                })
                || !is_request_eligible_status(slot.record.status.as_str())
            {
                continue;
            }
            if let Some(allowed_groups) = allowed_groups {
                if !allowed_groups.contains(&slot.record.account_type) {
                    continue;
                }
            } else if model != "auto"
                && !slot.record.models.is_empty()
                && !slot
                    .record
                    .models
                    .iter()
                    .any(|candidate| candidate == model)
            {
                continue;
            }
            slot.inflight.fetch_add(1, Ordering::AcqRel);
            return Some(AccountLease { slot });
        }
        None
    }

    #[cfg(test)]
    pub(super) async fn acquire(&self, model: &str) -> Option<AccountLease> {
        self.acquire_excluding_with_type_filter(model, &HashSet::new(), None)
            .await
    }

    pub(super) async fn active_type_candidates(
        &self,
    ) -> Option<(
        u64,
        HashMap<AccountModelGroup, Vec<CatalogAccountCandidate>>,
    )> {
        if !self.reload().await {
            return None;
        }
        let snapshot = self.snapshot.read().expect("account snapshot lock");
        if !snapshot.valid {
            return None;
        }
        let mut groups = HashMap::<AccountModelGroup, Vec<CatalogAccountCandidate>>::new();
        for slot in snapshot.accounts.iter() {
            if !is_request_eligible_status(slot.record.status.as_str()) {
                continue;
            }
            groups
                .entry(slot.record.account_type.clone())
                .or_default()
                .push(CatalogAccountCandidate {
                    token: slot.record.token.clone(),
                    source_type: slot.record.source_type.clone(),
                    chatgpt_account_id: slot.record.chatgpt_account_id.clone(),
                });
        }
        for candidates in groups.values_mut() {
            let mut seen_tokens = HashSet::new();
            candidates.retain(|candidate| seen_tokens.insert(candidate.token.clone()));
        }
        Some((snapshot.generation, groups))
    }

    pub(super) fn records(&self) -> Vec<AccountRecord> {
        self.snapshot
            .read()
            .expect("account snapshot lock")
            .accounts
            .iter()
            .map(|slot| slot.record.clone())
            .collect()
    }

    pub(super) fn raw_records(&self) -> Vec<serde_json::Value> {
        self.snapshot
            .read()
            .expect("account snapshot lock")
            .accounts
            .iter()
            .map(|slot| slot.record.raw.clone())
            .collect()
    }

    pub(super) fn mark_text_used(&self, token: &str) -> bool {
        let Ok(snapshot) = self.snapshot.read() else {
            return false;
        };
        let Some(slot) = snapshot
            .accounts
            .iter()
            .find(|slot| slot.record.token == token)
        else {
            return false;
        };
        let Ok(mut last_used_at) = slot.last_used_at.write() else {
            return false;
        };
        *last_used_at = Some(current_timestamp());
        true
    }

    pub(super) fn last_used_at(&self, token: &str) -> Option<String> {
        let snapshot = self.snapshot.read().ok()?;
        snapshot
            .accounts
            .iter()
            .find(|slot| slot.record.token == token)
            .and_then(|slot| slot.last_used_at.read().ok()?.clone())
    }

    #[cfg(test)]
    pub(super) fn set_last_used_at_for_test(&self, token: &str, value: Option<&str>) -> bool {
        let Ok(snapshot) = self.snapshot.read() else {
            return false;
        };
        let Some(slot) = snapshot
            .accounts
            .iter()
            .find(|slot| slot.record.token == token)
        else {
            return false;
        };
        let Ok(mut last_used_at) = slot.last_used_at.write() else {
            return false;
        };
        *last_used_at = value.map(ToOwned::to_owned);
        true
    }

    pub(super) fn note_usage_mark_failure() {
        if USAGE_MARK_FAILURES.fetch_add(1, Ordering::Relaxed) < 8 {
            log::warn!("account text usage marker unavailable; terminal response preserved");
        }
    }

    /// Serialize every account read-modify-write under the same file gate.
    /// Callers must not keep a snapshot across network or other await points.
    pub(super) async fn mutate_raw<F, R>(&self, mutator: F) -> Result<R, ApiError>
    where
        F: FnOnce(&mut Vec<serde_json::Value>) -> Result<R, ApiError>,
    {
        let _mutation_guard = self.mutation_gate.lock().await;
        let path = self
            .path
            .as_deref()
            .ok_or_else(ApiError::unsupported_capability)?;
        let _file_lock = super::acquire_path_write_lock(path).await?;
        if !self.reload().await {
            return Err(ApiError::unavailable());
        }
        let expected_fingerprint = self
            .snapshot
            .read()
            .expect("account snapshot lock")
            .fingerprint;
        let mut records = self.raw_records();
        let result = mutator(&mut records)?;
        self.replace_raw_locked(serde_json::Value::Array(records), expected_fingerprint)
            .await
            .map_err(|_| ApiError::unavailable())?;
        Ok(result)
    }

    /// Merge imported accounts against the current on-disk snapshot. Identity
    /// wins over access-token equality so token rotation cannot duplicate an
    /// account or erase a concurrent user edit.
    pub(super) async fn merge_import_records(
        &self,
        incoming: Vec<serde_json::Value>,
    ) -> Result<(usize, usize), ApiError> {
        self.mutate_raw(|records| {
            let mut index_by_key = HashMap::new();
            let mut index_by_token = HashMap::new();
            for (index, record) in records.iter().enumerate() {
                if let Some(token) = account_payload_token(record) {
                    index_by_token.insert(token, index);
                }
                if let Some(key) = account_identity_key(record) {
                    let replace = index_by_key.get(&key).copied().is_none_or(|previous| {
                        token_rank(&account_payload_token(record).unwrap_or_default())
                            >= token_rank(
                                &account_payload_token(&records[previous]).unwrap_or_default(),
                            )
                    });
                    if replace {
                        index_by_key.insert(key, index);
                    }
                }
            }
            let mut added = 0usize;
            let mut skipped = 0usize;
            let mut incoming_keys = HashMap::<String, usize>::new();
            let mut deduped_incoming = Vec::<(String, serde_json::Value)>::new();
            for value in incoming {
                let Some(token) = account_payload_token(&value) else {
                    continue;
                };
                let key = account_identity_key(&value).unwrap_or_else(|| format!("token:{token}"));
                if let Some(index) = incoming_keys.get(&key).copied() {
                    let previous = &deduped_incoming[index].1;
                    let merged = merge_account_values(previous, &value);
                    deduped_incoming[index].1 = merged;
                    skipped += 1;
                    continue;
                }
                incoming_keys.insert(key.clone(), deduped_incoming.len());
                deduped_incoming.push((key, value));
            }
            for (key, value) in deduped_incoming {
                let Some(token) = account_payload_token(&value) else {
                    continue;
                };
                let index = index_by_token
                    .get(&token)
                    .copied()
                    .or_else(|| index_by_key.get(&key).copied());
                if let Some(index) = index {
                    let merged = merge_account_values(&records[index], &value);
                    if let Some(previous) = account_payload_token(&records[index]) {
                        index_by_token.remove(&previous);
                    }
                    if let Some(next) = account_payload_token(&merged) {
                        index_by_token.insert(next, index);
                    }
                    records[index] = merged;
                    skipped += 1;
                } else {
                    let index = records.len();
                    let mut value = value;
                    if let Some(object) = value.as_object_mut() {
                        object.remove("accessToken");
                        object.remove("token");
                        object.insert("access_token".to_owned(), serde_json::json!(token));
                        if !object.contains_key("status") {
                            object.insert("status".to_owned(), serde_json::json!("正常"));
                        }
                    }
                    records.push(value);
                    index_by_key.insert(key, index);
                    index_by_token.insert(token, index);
                    added += 1;
                }
            }
            if records.len() > super::MAX_ACCOUNTS {
                return Err(ApiError::invalid_request());
            }
            Ok((added, skipped))
        })
        .await
    }

    pub(super) async fn update_refreshed_account(
        &self,
        old_token: &str,
        updated: serde_json::Value,
    ) -> Result<bool, ApiError> {
        self.mutate_raw(|records| {
            let Some(target) = records
                .iter_mut()
                .find(|item| account_payload_token(item).as_deref() == Some(old_token))
            else {
                return Ok(false);
            };
            let mut merged = merge_account_values(target, &updated);
            if let Some(updated_object) = updated.as_object()
                && updated_object
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
            {
                if let Some(status) = updated_object.get("status") {
                    merged["status"] = status.clone();
                }
                for key in ["last_refresh_error", "last_refresh_error_at"] {
                    merged
                        .as_object_mut()
                        .expect("merged account object")
                        .remove(key);
                }
            }
            *target = merged;
            Ok(true)
        })
        .await
    }

    pub(super) async fn mark_refresh_failed(
        &self,
        token: &str,
        error: &str,
    ) -> Result<bool, ApiError> {
        self.mutate_raw(|records| {
            let Some(target) = records
                .iter_mut()
                .find(|item| account_payload_token(item).as_deref() == Some(token))
            else {
                return Ok(false);
            };
            let object = target.as_object_mut().ok_or_else(ApiError::unavailable)?;
            object.insert(
                "status".to_owned(),
                serde_json::Value::String("异常".to_owned()),
            );
            object.insert(
                "last_refresh_error".to_owned(),
                serde_json::Value::String(error.to_owned()),
            );
            Ok(true)
        })
        .await
    }

    async fn replace_raw_locked(
        &self,
        value: serde_json::Value,
        expected_fingerprint: [u8; 32],
    ) -> Result<(), ()> {
        let Some(path) = self.path.clone() else {
            return Err(());
        };
        let target = path.as_ref().clone();
        let result = tokio::task::spawn_blocking(move || {
            let (current, _, fingerprint) =
                super::read_account_document(&target).map_err(|_| ())?;
            if fingerprint != expected_fingerprint {
                return Err(());
            }
            let output = match current {
                serde_json::Value::Object(mut object) if object.get("items").is_some() => {
                    object.insert("items".to_owned(), value);
                    serde_json::Value::Object(object)
                }
                _ => value,
            };
            let bytes = serde_json::to_vec(&output).map_err(|_| ())?;
            if bytes.len() as u64 > super::MAX_ACCOUNT_SNAPSHOT_BYTES {
                return Err(());
            }
            super::atomic_replace_checked_with_limit(
                &target,
                &bytes,
                super::MAX_ACCOUNT_SNAPSHOT_BYTES,
                false,
            )
            .map_err(|_| ())?;
            let _ = super::read_account_snapshot(&target).map_err(|_| ())?;
            Ok(())
        })
        .await
        .map_err(|_| ())?;
        result?;
        if self.reload().await { Ok(()) } else { Err(()) }
    }

    #[cfg(test)]
    pub(super) fn inflight(&self) -> usize {
        self.snapshot
            .read()
            .expect("account snapshot lock")
            .accounts
            .iter()
            .map(|slot| slot.inflight.load(Ordering::Acquire))
            .sum()
    }

    #[cfg(test)]
    pub(super) fn poison_usage_marker_lock(&self, token: &str) -> bool {
        let snapshot = self.snapshot.read().expect("account snapshot lock");
        let Some(slot) = snapshot
            .accounts
            .iter()
            .find(|slot| slot.record.token == token)
            .cloned()
        else {
            return false;
        };
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = slot
                .last_used_at
                .write()
                .expect("usage marker lock should initially be healthy");
            panic!("test-only usage marker poison");
        }))
        .is_err()
    }
}

fn account_payload_token(value: &serde_json::Value) -> Option<String> {
    value
        .as_object()
        .and_then(|object| {
            object
                .get("access_token")
                .or_else(|| object.get("accessToken"))
                .or_else(|| object.get("token"))
        })
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|token| !token.is_empty() && token.len() <= super::MAX_ACCOUNT_TOKEN_LENGTH)
        .map(ToOwned::to_owned)
}

fn jwt_payload(token: &str) -> serde_json::Value {
    let Some(encoded) = token.split('.').nth(1) else {
        return serde_json::Value::Null;
    };
    let encoded = encoded.trim_end_matches('=');
    let Ok(bytes) = URL_SAFE_NO_PAD.decode(encoded) else {
        return serde_json::Value::Null;
    };
    serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
}

fn nonempty_field(value: Option<&serde_json::Value>) -> Option<String> {
    value
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn claim_field(payload: &serde_json::Value, path: &[&str]) -> Option<String> {
    let mut current = payload;
    for key in path {
        current = current.get(*key)?;
    }
    nonempty_field(Some(current))
}

fn account_identity_key(value: &serde_json::Value) -> Option<String> {
    let object = value.as_object()?;
    let access = account_payload_token(value).unwrap_or_default();
    let access_payload = jwt_payload(&access);
    let id_payload = object
        .get("id_token")
        .and_then(serde_json::Value::as_str)
        .map(jwt_payload)
        .unwrap_or(serde_json::Value::Null);
    let account_id = claim_field(
        &access_payload,
        &["https://api.openai.com/auth", "chatgpt_account_id"],
    )
    .or_else(|| {
        claim_field(
            &id_payload,
            &["https://api.openai.com/auth", "chatgpt_account_id"],
        )
    })
    .or_else(|| nonempty_field(object.get("account_id")))
    .or_else(|| nonempty_field(object.get("chatgpt_account_id")));
    if let Some(account_id) = account_id {
        return Some(format!("account_id:{account_id}"));
    }
    let subject = claim_field(&access_payload, &["sub"])
        .or_else(|| claim_field(&id_payload, &["sub"]))
        .or_else(|| claim_field(&access_payload, &["https://api.openai.com/auth", "user_id"]))
        .or_else(|| claim_field(&id_payload, &["https://api.openai.com/auth", "user_id"]))
        .or_else(|| nonempty_field(object.get("user_id")));
    if let Some(subject) = subject {
        return Some(format!("subject:{subject}"));
    }
    let email = claim_field(
        &access_payload,
        &["https://api.openai.com/profile", "email"],
    )
    .or_else(|| claim_field(&id_payload, &["https://api.openai.com/profile", "email"]))
    .or_else(|| claim_field(&access_payload, &["email"]))
    .or_else(|| claim_field(&id_payload, &["email"]))
    .or_else(|| nonempty_field(object.get("email")));
    email.map(|email| format!("email:{}", email.to_ascii_lowercase()))
}

fn merge_account_values(
    current: &serde_json::Value,
    incoming: &serde_json::Value,
) -> serde_json::Value {
    let current_token = account_payload_token(current).unwrap_or_default();
    let incoming_token = account_payload_token(incoming).unwrap_or_default();
    let preferred_is_incoming = token_rank(&incoming_token) >= token_rank(&current_token);
    let (preferred, fallback) = if preferred_is_incoming {
        (incoming, current)
    } else {
        (current, incoming)
    };
    let mut merged = fallback.as_object().cloned().unwrap_or_default();
    if let Some(object) = preferred.as_object() {
        for (key, value) in object {
            let useful = !value.is_null()
                && (!value.is_string() || !value.as_str().unwrap_or_default().trim().is_empty());
            if useful || !merged.contains_key(key) {
                merged.insert(key.clone(), value.clone());
            }
        }
    }
    let preferred_token = account_payload_token(preferred).unwrap_or_default();
    merged.remove("accessToken");
    merged.insert(
        "access_token".to_owned(),
        serde_json::Value::String(preferred_token),
    );
    for key in [
        "status",
        "quota",
        "success",
        "fail",
        "invalid_count",
        "limits_progress",
        "last_used_at",
        "last_invalid_at",
        "last_refresh_error",
        "last_refresh_error_at",
        "last_token_refresh_at",
        "last_token_refresh_error",
        "last_token_refresh_error_at",
    ] {
        if let Some(value) = current.as_object().and_then(|object| object.get(key)) {
            merged.insert(key.to_owned(), value.clone());
        }
    }
    let created_at = [current, incoming]
        .iter()
        .filter_map(|value| {
            value
                .as_object()
                .and_then(|object| object.get("created_at"))
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
        })
        .min()
        .map(ToOwned::to_owned);
    if let Some(created_at) = created_at {
        merged.insert(
            "created_at".to_owned(),
            serde_json::Value::String(created_at),
        );
    }
    serde_json::Value::Object(merged)
}

fn token_rank(token: &str) -> (i64, i64) {
    let payload = jwt_payload(token);
    fn numeric(payload: &serde_json::Value, key: &str) -> i64 {
        payload
            .get(key)
            .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
            .unwrap_or_default()
    }
    (numeric(&payload, "exp"), numeric(&payload, "iat"))
}

fn account_slots(records: Vec<AccountRecord>) -> Vec<Arc<AccountSlot>> {
    account_slots_with_runtime_state(records, None)
}

fn remember_runtime_marker(markers: &mut HashMap<String, String>, key: String, value: String) {
    match markers.entry(key) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(value);
        }
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            let previous = entry.get().clone();
            if let Some(latest) = latest_last_used_at(previous, value) {
                entry.insert(latest);
            }
        }
    }
}

#[derive(Clone)]
struct PreviousAccountRuntime {
    identity: Option<String>,
    last_used_at: Option<String>,
}

fn last_used_at_key(value: &str) -> Option<(u32, u32, u32, u32, u32, u32)> {
    let bytes = value.as_bytes();
    if bytes.len() != 19
        || !matches!(bytes[4], b'-')
        || !matches!(bytes[7], b'-')
        || !matches!(bytes[10], b' ')
        || !matches!(bytes[13], b':')
        || !matches!(bytes[16], b':')
    {
        return None;
    }
    let number = |start: usize, end: usize| {
        value
            .get(start..end)
            .filter(|part| part.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|part| part.parse::<u32>().ok())
    };
    let year = number(0, 4)?;
    let month = number(5, 7)?;
    let day = number(8, 10)?;
    let hour = number(11, 13)?;
    let minute = number(14, 16)?;
    let second = number(17, 19)?;
    if year == 0 || !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days_in_month = match month {
        2 if year % 400 == 0 || (year % 4 == 0 && year % 100 != 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if !(1..=days_in_month).contains(&day) {
        return None;
    }
    Some((year, month, day, hour, minute, second))
}

fn valid_last_used_at(value: &str) -> Option<String> {
    last_used_at_key(value).map(|_| value.to_owned())
}

fn latest_last_used_at(left: String, right: String) -> Option<String> {
    match (last_used_at_key(&left), last_used_at_key(&right)) {
        (Some(left_key), Some(right_key)) => Some(if left_key >= right_key { left } else { right }),
        (Some(_), None) => Some(left),
        (None, Some(_)) => Some(right),
        (None, None) => None,
    }
}

fn compatible_identity(previous: Option<&str>, current: Option<&str>) -> bool {
    match (previous, current) {
        (Some(previous), Some(current)) => previous == current,
        (None, None) => true,
        _ => false,
    }
}

fn account_slots_with_runtime_state(
    records: Vec<AccountRecord>,
    previous: Option<&[Arc<AccountSlot>]>,
) -> Vec<Arc<AccountSlot>> {
    let mut previous_by_token = HashMap::<String, PreviousAccountRuntime>::new();
    let mut last_used_by_identity = HashMap::<String, String>::new();
    if let Some(previous) = previous {
        for slot in previous {
            let identity = account_identity_key(&slot.record.raw);
            let last_used_at = slot
                .last_used_at
                .read()
                .ok()
                .and_then(|value| value.as_deref().and_then(valid_last_used_at));
            previous_by_token.insert(
                slot.record.token.clone(),
                PreviousAccountRuntime {
                    identity: identity.clone(),
                    last_used_at: last_used_at.clone(),
                },
            );
            if let (Some(identity), Some(last_used_at)) = (identity, last_used_at) {
                remember_runtime_marker(&mut last_used_by_identity, identity, last_used_at);
            }
        }
    }
    records
        .into_iter()
        .map(|record| {
            let persisted_last_used_at = record
                .raw
                .get("last_used_at")
                .and_then(serde_json::Value::as_str)
                .and_then(valid_last_used_at);
            let identity = account_identity_key(&record.raw);
            let runtime_last_used_at = if let Some(previous) = previous_by_token.get(&record.token)
            {
                compatible_identity(previous.identity.as_deref(), identity.as_deref())
                    .then(|| previous.last_used_at.clone())
                    .flatten()
            } else {
                identity
                    .as_ref()
                    .and_then(|identity| last_used_by_identity.get(identity).cloned())
            };
            let last_used_at = match (runtime_last_used_at, persisted_last_used_at) {
                (Some(runtime), Some(persisted)) => latest_last_used_at(runtime, persisted),
                (Some(runtime), None) | (None, Some(runtime)) => Some(runtime),
                (None, None) => None,
            };
            Arc::new(AccountSlot {
                record,
                inflight: AtomicUsize::new(0),
                last_used_at: RwLock::new(last_used_at),
            })
        })
        .collect()
}

fn current_timestamp() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let days = seconds.div_euclid(86_400);
    let day_seconds = seconds.rem_euclid(86_400);
    let hour = day_seconds / 3_600;
    let minute = day_seconds % 3_600 / 60;
    let second = day_seconds % 60;

    // Civil date conversion from Unix days; this keeps the in-memory marker
    // in the same second-resolution shape as Python's text account marker.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let month_part = (5 * doy + 2) / 153;
    let day = doy - (153 * month_part + 2) / 5 + 1;
    let month = month_part + if month_part < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
}
