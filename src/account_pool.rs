use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use sha2::Digest;
use tokio::sync::Mutex;

use file_identity::FileVersion;

use super::model_pool::WEB_IMAGE_MODELS;
use super::{
    AccountRevisionDecision, ApiError, AppInitError, HealthSnapshotSync, NativeRequestContext,
    account_revision_decision, read_account_snapshot,
    storage::{StorageBackend, StorageError, StorageSnapshot},
    validated_file_version,
};

#[cfg(test)]
use std::sync::atomic::AtomicBool;

static USAGE_MARK_FAILURES: AtomicUsize = AtomicUsize::new(0);

pub(super) type AccountModelGroup = String;

fn is_request_eligible_status(status: &str) -> bool {
    !matches!(status, "禁用" | "异常")
}

fn is_request_eligible_record(record: &AccountRecord) -> bool {
    is_request_eligible_status(record.status.as_str())
}

fn is_codex_image_account_eligible(
    record: &AccountRecord,
    allowed_groups: Option<&HashSet<AccountModelGroup>>,
) -> bool {
    is_image_account_available(record)
        && record.source_type == "codex"
        && allowed_groups.is_none_or(|groups| groups.contains(&record.account_type))
}

fn is_image_account_available(record: &AccountRecord) -> bool {
    !matches!(record.status.as_str(), "禁用" | "限流" | "异常")
        && image_quota(record.raw.get("quota")).is_some()
}

#[derive(Clone, Debug)]
pub(super) struct AccountRecord {
    pub(super) token: String,
    pub(super) created_at: String,
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
    pub(super) proxy_url: Option<String>,
}

pub(super) struct AccountSlot {
    pub(super) record: AccountRecord,
    pub(super) inflight: AtomicUsize,
    image_inflight: Arc<AtomicUsize>,
    last_used_at: RwLock<Option<String>>,
    #[cfg(test)]
    fail_usage_marker: AtomicBool,
}

#[derive(Clone)]
pub(super) struct AccountSnapshot {
    pub(super) generation: u64,
    pub(super) fingerprint: [u8; 32],
    pub(super) file_version: Option<FileVersion>,
    pub(super) valid: bool,
    pub(super) accounts: Arc<Vec<Arc<AccountSlot>>>,
    pub(super) health: AccountHealthStats,
}

#[derive(Clone, Default)]
pub(super) struct AccountHealthStats {
    pub(super) total: u64,
    pub(super) cumulative_total: u64,
    pub(super) active: u64,
    pub(super) limited: u64,
    pub(super) abnormal: u64,
    pub(super) disabled: u64,
    pub(super) total_quota: u64,
    pub(super) total_success: i64,
    pub(super) total_fail: i64,
    pub(super) by_type: BTreeMap<String, u64>,
}

impl AccountHealthStats {
    fn from_records(records: &[AccountRecord], cumulative_total: u64) -> Self {
        let mut health = Self {
            total: records.len() as u64,
            cumulative_total: cumulative_total.max(records.len() as u64),
            ..Self::default()
        };
        for record in records {
            match record.status.as_str() {
                "正常" => {
                    health.active = health.active.saturating_add(1);
                    health.total_quota = health.total_quota.saturating_add(
                        u64::try_from(account_counter(record.raw.get("quota")).max(0))
                            .unwrap_or_default(),
                    );
                }
                "限流" => health.limited = health.limited.saturating_add(1),
                "异常" => health.abnormal = health.abnormal.saturating_add(1),
                "禁用" => health.disabled = health.disabled.saturating_add(1),
                _ => {}
            }
            health.total_success = health
                .total_success
                .saturating_add(account_counter(record.raw.get("success")));
            health.total_fail = health
                .total_fail
                .saturating_add(account_counter(record.raw.get("fail")));
            let account_type = record
                .raw
                .get("type")
                .filter(|value| account_value_truthy(Some(value)))
                .map(python_account_value_string)
                .unwrap_or_else(|| "free".to_owned());
            let count = health.by_type.entry(account_type).or_default();
            *count = count.saturating_add(1);
        }
        health
    }
}

fn account_counter(value: Option<&serde_json::Value>) -> i64 {
    value
        .and_then(super::python_integer_value)
        .unwrap_or_default()
}

pub(super) fn account_value_truthy(value: Option<&serde_json::Value>) -> bool {
    match value {
        None | Some(serde_json::Value::Null) => false,
        Some(serde_json::Value::Bool(value)) => *value,
        Some(serde_json::Value::Number(value)) => value.as_f64().is_some_and(|value| value != 0.0),
        Some(serde_json::Value::String(value)) => !value.is_empty(),
        Some(serde_json::Value::Array(value)) => !value.is_empty(),
        Some(serde_json::Value::Object(value)) => !value.is_empty(),
    }
}

pub(super) fn account_invalid_count(value: Option<&serde_json::Value>) -> i64 {
    value
        .and_then(super::python_integer_value)
        .unwrap_or_default()
}

fn image_quota(value: Option<&serde_json::Value>) -> Option<u64> {
    let value = value?;
    let quota = match value {
        serde_json::Value::Bool(value) => i64::from(*value),
        serde_json::Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_f64().map(|value| value.trunc() as i64))?,
        serde_json::Value::String(text) => text.trim().parse::<i64>().ok()?,
        _ => return None,
    };
    u64::try_from(quota).ok().filter(|quota| *quota > 0)
}

fn has_verified_web_image_capability(record: &AccountRecord) -> bool {
    if record.status != "正常"
        || !is_request_eligible_record(record)
        || record
            .raw
            .get("_verified_image_capability")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        || image_quota(record.raw.get("quota")).is_none()
    {
        return false;
    }
    record.source_type != "codex"
        || record
            .raw
            .as_object()
            .is_some_and(super::account_model_source_proof)
}

pub(super) struct AccountLease {
    pub(super) slot: Arc<AccountSlot>,
    image: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ImageResultUpdate {
    Updated,
    RemovedRateLimited,
    NotFound,
}

impl AccountLease {
    fn new(slot: Arc<AccountSlot>, image: bool) -> Self {
        Self { slot, image }
    }

    pub(super) fn token(&self) -> &str {
        &self.slot.record.token
    }

    pub(super) fn source_type(&self) -> &str {
        &self.slot.record.source_type
    }

    pub(super) fn account_type(&self) -> &str {
        &self.slot.record.account_type
    }

    pub(super) fn email(&self) -> Option<&str> {
        self.slot
            .record
            .raw
            .get("email")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
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
    pub(super) fn native_request_context(&self) -> NativeRequestContext {
        NativeRequestContext::for_account(&self.slot.record.raw)
    }
    pub(super) fn tls_emulation(&self) -> wreq_util::Profile {
        self.native_request_context().tls_emulation()
    }
}

impl Drop for AccountLease {
    fn drop(&mut self) {
        let counter = if self.image {
            &self.slot.image_inflight
        } else {
            &self.slot.inflight
        };
        let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
            value.checked_sub(1)
        });
    }
}

#[derive(Clone)]
pub(super) struct AccountStore {
    pub(super) path: Option<Arc<PathBuf>>,
    backend: Option<Arc<StorageBackend>>,
    pub(super) snapshot: Arc<RwLock<AccountSnapshot>>,
    reload_gate: Arc<Mutex<()>>,
    mutation_gate: Arc<Mutex<()>>,
    pub(super) cursor: Arc<AtomicUsize>,
    health_snapshot_sync: Arc<RwLock<Option<HealthSnapshotSync>>>,
}

impl AccountStore {
    pub(super) fn load(path: Option<&Path>) -> Result<Self, AppInitError> {
        let (records, fingerprint, file_version, cumulative_total) = if let Some(path) = path {
            let (value, records, fingerprint, file_version) = super::read_account_document(path)?;
            let canonical = super::canonicalize_account_document_value(&value)?;
            if canonical != value && super::account_snapshot_requires_rewrite(&value, &canonical) {
                let bytes =
                    serde_json::to_vec(&canonical).map_err(|_| AppInitError::AccountSnapshot)?;
                super::atomic_replace_checked_with_limit(
                    path,
                    &bytes,
                    super::MAX_ACCOUNT_SNAPSHOT_BYTES,
                    false,
                )
                .map_err(|_| AppInitError::AccountSnapshot)?;
                let canonical_fingerprint = sha2::Sha256::digest(&bytes).into();
                let canonical_version = super::validated_file_version(path)
                    .map_err(|_| AppInitError::AccountSnapshot)?;
                let cumulative_total = value
                    .get("cumulative_total")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(records.len() as u64);
                (
                    records,
                    canonical_fingerprint,
                    Some(canonical_version),
                    cumulative_total,
                )
            } else {
                let cumulative_total = value
                    .get("cumulative_total")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(records.len() as u64);
                (records, fingerprint, Some(file_version), cumulative_total)
            }
        } else {
            (Vec::new(), [0; 32], None, 0)
        };
        let health = AccountHealthStats::from_records(&records, cumulative_total);
        let accounts = Arc::new(account_slots(records.clone()));
        Ok(Self {
            path: path.map(|path| Arc::new(path.to_owned())),
            backend: None,
            snapshot: Arc::new(RwLock::new(AccountSnapshot {
                generation: 0,
                fingerprint,
                file_version,
                valid: true,
                accounts,
                health,
            })),
            reload_gate: Arc::new(Mutex::new(())),
            mutation_gate: Arc::new(Mutex::new(())),
            cursor: Arc::new(AtomicUsize::new(0)),
            health_snapshot_sync: Arc::new(RwLock::new(None)),
        })
    }

    pub(super) async fn load_backend(backend: Arc<StorageBackend>) -> Result<Self, AppInitError> {
        let loaded = backend
            .load_accounts()
            .await
            .map_err(|_| AppInitError::StorageBackend)?;
        let cumulative_total = loaded
            .cumulative_total
            .unwrap_or(loaded.records.len() as u64);
        let canonical_records = loaded
            .records
            .iter()
            .map(super::canonicalize_account_item)
            .collect::<Result<Vec<_>, _>>()?;
        let loaded = if canonical_records != loaded.records {
            backend
                .save_accounts(loaded.revision, canonical_records, cumulative_total)
                .await
                .map_err(|_| AppInitError::StorageBackend)?
        } else {
            loaded
        };
        let (records, fingerprint, cumulative_total) = parse_backend_snapshot(loaded)?;
        let health = AccountHealthStats::from_records(&records, cumulative_total);
        Ok(Self {
            path: None,
            backend: Some(backend),
            snapshot: Arc::new(RwLock::new(AccountSnapshot {
                generation: 0,
                fingerprint,
                file_version: None,
                valid: true,
                accounts: Arc::new(account_slots(records)),
                health,
            })),
            reload_gate: Arc::new(Mutex::new(())),
            mutation_gate: Arc::new(Mutex::new(())),
            cursor: Arc::new(AtomicUsize::new(0)),
            health_snapshot_sync: Arc::new(RwLock::new(None)),
        })
    }

    pub(super) fn install_health_snapshot_sync(&self, sync: HealthSnapshotSync) {
        *self
            .health_snapshot_sync
            .write()
            .expect("account health snapshot sync lock") = Some(sync);
    }

    fn notify_health_snapshot_sync(&self) {
        let sync = self
            .health_snapshot_sync
            .read()
            .expect("account health snapshot sync lock")
            .clone();
        if let Some(sync) = sync {
            sync();
        }
    }

    pub(super) async fn reload(&self) -> bool {
        if self.backend.is_some() {
            return self.snapshot.read().expect("account snapshot lock").valid;
        }
        let Some(path) = self.path.clone() else {
            return true;
        };
        let _reload_guard = self.reload_gate.lock().await;
        let version_path = path.clone();
        let version =
            tokio::task::spawn_blocking(move || validated_file_version(&version_path)).await;
        let Ok(Ok(version)) = version else {
            self.invalidate();
            return false;
        };
        {
            let snapshot = self.snapshot.read().expect("account snapshot lock");
            // File-backed owners publish through the checked atomic-replace
            // path, so FileVersion is the bounded O(1) request-path revision
            // key. In-place writers are outside that contract and must use
            // the explicit invalidation/reload boundary instead of forcing a
            // content scan here.
            if snapshot.valid && snapshot.file_version == Some(version) {
                return true;
            }
        }
        let result = tokio::task::spawn_blocking(move || read_account_snapshot(&path)).await;
        let valid = {
            let mut snapshot = self.snapshot.write().expect("account snapshot lock");
            match result {
                Ok(Ok((accounts, fingerprint, file_version, cumulative_total))) => {
                    if account_revision_decision(
                        snapshot.valid,
                        snapshot.file_version,
                        snapshot.fingerprint,
                        file_version,
                        fingerprint,
                    ) == AccountRevisionDecision::Reload
                    {
                        let health = AccountHealthStats::from_records(&accounts, cumulative_total);
                        snapshot.generation = snapshot.generation.saturating_add(1);
                        snapshot.fingerprint = fingerprint;
                        snapshot.file_version = Some(file_version);
                        snapshot.accounts = Arc::new(account_slots_with_runtime_state(
                            accounts,
                            Some(snapshot.accounts.as_ref()),
                        ));
                        snapshot.health = health;
                        snapshot.valid = true;
                    }
                    true
                }
                _ => {
                    snapshot.generation = snapshot.generation.saturating_add(1);
                    snapshot.valid = false;
                    snapshot.accounts = Arc::new(Vec::new());
                    snapshot.health = AccountHealthStats::default();
                    false
                }
            }
        };
        self.notify_health_snapshot_sync();
        valid
    }

    /// Refresh a backend-backed snapshot after a failed CAS.  The ordinary
    /// backend `reload` path is deliberately cache-only so health and request
    /// reads never turn into an implicit remote/database scan.  A mutation
    /// conflict is the explicit exception: the next CAS attempt must be built
    /// from the winning persisted revision.
    async fn reload_backend_fresh(&self) -> bool {
        let Some(backend) = self.backend.clone() else {
            return self.reload().await;
        };
        match backend.load_accounts().await {
            Ok(loaded) => self.publish_backend_snapshot(loaded),
            Err(_) => {
                self.invalidate();
                false
            }
        }
    }

    pub(super) fn invalidate(&self) {
        self.invalidate_silent();
        self.notify_health_snapshot_sync();
    }

    pub(super) fn invalidate_silent(&self) {
        {
            let mut snapshot = self.snapshot.write().expect("account snapshot lock");
            snapshot.generation = snapshot.generation.saturating_add(1);
            snapshot.valid = false;
            snapshot.accounts = Arc::new(Vec::new());
            snapshot.health = AccountHealthStats::default();
        }
    }

    pub(super) fn health_validated(&self) -> bool {
        if self.backend.is_some() {
            return self.snapshot.read().expect("account snapshot lock").valid;
        }
        let Some(path) = self.path.as_deref() else {
            return self.snapshot.read().expect("account snapshot lock").valid;
        };
        let expected = {
            let snapshot = self.snapshot.read().expect("account snapshot lock");
            if !snapshot.valid {
                return false;
            }
            snapshot.file_version
        };
        expected.is_some_and(|expected| validated_file_version(path).ok() == Some(expected))
    }

    pub(super) fn health_stats(&self) -> AccountHealthStats {
        self.snapshot
            .read()
            .expect("account snapshot lock")
            .health
            .clone()
    }

    pub(super) async fn lock_reload_gate(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.reload_gate.clone().lock_owned().await
    }

    pub(super) fn publish_backend_snapshot(&self, loaded: StorageSnapshot) -> bool {
        let Ok((accounts, fingerprint, cumulative_total)) = parse_backend_snapshot(loaded) else {
            self.invalidate();
            return false;
        };
        self.publish_validated_backend_snapshot(accounts, fingerprint, cumulative_total);
        true
    }

    pub(super) fn publish_validated_backend_snapshot(
        &self,
        accounts: Vec<AccountRecord>,
        fingerprint: [u8; 32],
        cumulative_total: u64,
    ) {
        self.publish_validated_backend_snapshot_silent(accounts, fingerprint, cumulative_total);
        self.notify_health_snapshot_sync();
    }

    pub(super) fn publish_validated_backend_snapshot_silent(
        &self,
        accounts: Vec<AccountRecord>,
        fingerprint: [u8; 32],
        cumulative_total: u64,
    ) {
        let mut snapshot = self.snapshot.write().expect("account snapshot lock");
        if !snapshot.valid || snapshot.fingerprint != fingerprint {
            let health = AccountHealthStats::from_records(&accounts, cumulative_total);
            snapshot.generation = snapshot.generation.saturating_add(1);
            snapshot.fingerprint = fingerprint;
            snapshot.file_version = None;
            snapshot.accounts = Arc::new(account_slots_with_runtime_state(
                accounts,
                Some(snapshot.accounts.as_ref()),
            ));
            snapshot.health = health;
            snapshot.valid = true;
        }
    }

    #[cfg(test)]
    pub(super) async fn hold_reload_gate_for_test(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.lock_reload_gate().await
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

    pub(super) async fn acquire_least_recently_used_with_types(
        &self,
        allowed_groups: &HashSet<AccountModelGroup>,
    ) -> Option<AccountLease> {
        if !self.reload().await {
            return None;
        }
        let snapshot = self.snapshot.read().expect("account snapshot lock");
        if !snapshot.valid {
            return None;
        }
        let mut candidates = snapshot
            .accounts
            .iter()
            .enumerate()
            .filter(|(_, slot)| {
                is_request_eligible_record(&slot.record)
                    && allowed_groups.contains(&slot.record.account_type)
            })
            .map(|(index, slot)| {
                let last_used_at = slot
                    .last_used_at
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone()
                    .unwrap_or_default();
                (last_used_at, index, slot.clone())
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
        let slot = candidates.into_iter().next()?.2;
        slot.inflight.fetch_add(1, Ordering::AcqRel);
        Some(AccountLease::new(slot, false))
    }

    pub(super) async fn acquire_image_lease(
        &self,
        excluded_tokens: &HashSet<String>,
    ) -> Option<AccountLease> {
        self.acquire_image_lease_with_limit(excluded_tokens, usize::MAX)
            .await
    }

    pub(super) async fn acquire_image_lease_with_limit(
        &self,
        excluded_tokens: &HashSet<String>,
        max_inflight_per_account: usize,
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
        let max_inflight = u64::try_from(max_inflight_per_account).unwrap_or(u64::MAX);
        for offset in 0..accounts.len() {
            let slot = accounts[(start.wrapping_add(offset)) % accounts.len()].clone();
            if image_quota(slot.record.raw.get("quota")).is_none() {
                continue;
            }
            if excluded_tokens.contains(&slot.record.token)
                || !is_image_account_available(&slot.record)
            {
                continue;
            }
            let reserved =
                slot.image_inflight
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |inflight| {
                        (u64::try_from(inflight)
                            .ok()
                            .is_some_and(|inflight| inflight < max_inflight))
                        .then(|| inflight.checked_add(1))
                        .flatten()
                    });
            if reserved.is_ok() {
                return Some(AccountLease::new(slot, true));
            }
        }
        None
    }

    pub(super) async fn acquire_codex_image_lease_with_limit(
        &self,
        excluded_tokens: &HashSet<String>,
        allowed_groups: Option<&HashSet<AccountModelGroup>>,
        max_inflight_per_account: usize,
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
        let max_inflight = u64::try_from(max_inflight_per_account).unwrap_or(u64::MAX);
        for offset in 0..accounts.len() {
            let slot = accounts[(start.wrapping_add(offset)) % accounts.len()].clone();
            if excluded_tokens.contains(&slot.record.token)
                || !is_codex_image_account_eligible(&slot.record, allowed_groups)
            {
                continue;
            }
            let reserved =
                slot.image_inflight
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |inflight| {
                        (u64::try_from(inflight)
                            .ok()
                            .is_some_and(|inflight| inflight < max_inflight))
                        .then(|| inflight.checked_add(1))
                        .flatten()
                    });
            if reserved.is_ok() {
                return Some(AccountLease::new(slot, true));
            }
        }
        None
    }

    pub(super) async fn codex_image_has_candidate(
        &self,
        excluded_tokens: &HashSet<String>,
        allowed_groups: Option<&HashSet<AccountModelGroup>>,
    ) -> bool {
        if !self.reload().await {
            return false;
        }
        let snapshot = self.snapshot.read().expect("account snapshot lock");
        snapshot.valid
            && snapshot.accounts.iter().any(|slot| {
                !excluded_tokens.contains(&slot.record.token)
                    && is_codex_image_account_eligible(&slot.record, allowed_groups)
            })
    }

    pub(super) async fn codex_image_token_is_eligible(
        &self,
        token: &str,
        allowed_groups: Option<&HashSet<AccountModelGroup>>,
    ) -> bool {
        if token.is_empty() || !self.reload().await {
            return false;
        }
        let snapshot = self.snapshot.read().expect("account snapshot lock");
        snapshot
            .accounts
            .iter()
            .find(|slot| slot.record.token == token)
            .is_some_and(|slot| is_codex_image_account_eligible(&slot.record, allowed_groups))
    }

    pub(super) async fn image_token_is_eligible(&self, token: &str) -> bool {
        if token.is_empty() || !self.reload().await {
            return false;
        }
        let snapshot = self.snapshot.read().expect("account snapshot lock");
        snapshot
            .accounts
            .iter()
            .find(|slot| slot.record.token == token)
            .is_some_and(|slot| is_image_account_available(&slot.record))
    }

    pub(super) async fn image_has_eligible_account(
        &self,
        excluded_tokens: &HashSet<String>,
    ) -> bool {
        if !self.reload().await {
            return false;
        }
        let snapshot = self.snapshot.read().expect("account snapshot lock");
        snapshot.accounts.iter().any(|slot| {
            !excluded_tokens.contains(&slot.record.token)
                && is_image_account_available(&slot.record)
        })
    }

    /// Persist image quota accounting for the account that owned the lease.
    pub(super) async fn mark_image_result(
        &self,
        token: &str,
        success: bool,
        auto_remove_rate_limited: bool,
    ) -> Result<ImageResultUpdate, ApiError> {
        if token.is_empty() {
            return Ok(ImageResultUpdate::NotFound);
        }
        let mut result = ImageResultUpdate::NotFound;
        self.mutate_raw(|records| {
            let Some(index) = records
                .iter_mut()
                .position(|value| account_payload_token(value).as_deref() == Some(token))
            else {
                return Ok(());
            };
            let limited = {
                let object = records[index]
                    .as_object_mut()
                    .ok_or_else(ApiError::unavailable)?;
                object.insert(
                    "last_used_at".to_owned(),
                    serde_json::Value::String(current_local_timestamp()),
                );
                if success {
                    let quota = account_counter(object.get("quota"))
                        .saturating_sub(1)
                        .max(0);
                    object.insert("quota".to_owned(), serde_json::json!(quota));
                    object.insert(
                        "success".to_owned(),
                        serde_json::json!(account_counter(object.get("success")).saturating_add(1)),
                    );
                    if quota == 0 {
                        object.insert(
                            "status".to_owned(),
                            serde_json::Value::String("限流".to_owned()),
                        );
                        let restore_at = object
                            .get("restore_at")
                            .filter(|value| account_value_truthy(Some(value)))
                            .cloned();
                        object.insert(
                            "restore_at".to_owned(),
                            restore_at.unwrap_or(serde_json::Value::Null),
                        );
                    } else if object.get("status").and_then(serde_json::Value::as_str)
                        == Some("限流")
                    {
                        object.insert(
                            "status".to_owned(),
                            serde_json::Value::String("正常".to_owned()),
                        );
                    }
                } else {
                    object.insert(
                        "fail".to_owned(),
                        serde_json::json!(account_counter(object.get("fail")).saturating_add(1)),
                    );
                }
                object.get("status").and_then(serde_json::Value::as_str) == Some("限流")
            };
            if limited && auto_remove_rate_limited {
                records.remove(index);
                result = ImageResultUpdate::RemovedRateLimited;
            } else {
                result = ImageResultUpdate::Updated;
            }
            Ok(())
        })
        .await?;
        Ok(result)
    }

    async fn acquire_filtered(
        &self,
        _model: &str,
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
        let eligible = snapshot
            .accounts
            .iter()
            .filter(|slot| {
                !excluded_tokens.contains(&slot.record.token)
                    && required_source_type.is_none_or(|source| slot.record.source_type == source)
                    && required_capability.is_none_or(|capability| match capability {
                        "codex" => slot.record.source_type == "codex",
                        "web" => matches!(
                            slot.record.source_type.as_str(),
                            "web" | "password" | "password-oauth"
                        ),
                        _ => false,
                    })
                    && is_request_eligible_record(&slot.record)
                    && allowed_groups
                        .is_none_or(|groups| groups.contains(&slot.record.account_type))
            })
            .cloned()
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            return None;
        }
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % eligible.len();
        let slot = eligible[start].clone();
        slot.inflight.fetch_add(1, Ordering::AcqRel);
        Some(AccountLease::new(slot, false))
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
            if matches!(slot.record.status.as_str(), "禁用" | "异常") {
                continue;
            }
            groups
                .entry(slot.record.account_type.clone())
                .or_default()
                .push(CatalogAccountCandidate {
                    token: slot.record.token.clone(),
                    source_type: slot.record.source_type.clone(),
                    chatgpt_account_id: slot.record.chatgpt_account_id.clone(),
                    proxy_url: slot
                        .record
                        .raw
                        .get("proxy")
                        .and_then(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned),
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

    pub(super) fn image_models_for_account(
        &self,
        token: &str,
        account_id: Option<&str>,
    ) -> Vec<String> {
        let account_id = account_id.map(str::trim).filter(|value| !value.is_empty());
        self.snapshot
            .read()
            .expect("account snapshot lock")
            .accounts
            .iter()
            .find(|slot| {
                has_verified_web_image_capability(&slot.record)
                    && slot
                        .record
                        .raw
                        .get("_verified_image_capability")
                        .and_then(serde_json::Value::as_bool)
                        == Some(true)
                    && slot
                        .record
                        .raw
                        .get("quota")
                        .is_some_and(|value| image_quota(Some(value)).is_some())
                    && (slot.record.token == token
                        || account_id.is_some_and(|value| {
                            slot.record.chatgpt_account_id.as_deref() == Some(value)
                        }))
            })
            .map(|_| WEB_IMAGE_MODELS.iter().map(|id| (*id).to_owned()).collect())
            .unwrap_or_default()
    }

    /// Match the original image-account contract without tying discovery to
    /// the import source: healthy accounts with a positive integer quota are
    /// eligible for the ChatGPT Web image model.
    pub(super) fn image_capable_account_types(&self) -> HashSet<AccountModelGroup> {
        self.snapshot
            .read()
            .expect("account snapshot lock")
            .accounts
            .iter()
            .filter(|slot| {
                has_verified_web_image_capability(&slot.record)
                    && slot
                        .record
                        .raw
                        .get("_verified_image_capability")
                        .and_then(serde_json::Value::as_bool)
                        == Some(true)
                    && slot
                        .record
                        .raw
                        .get("quota")
                        .is_some_and(|value| image_quota(Some(value)).is_some())
            })
            .map(|slot| slot.record.account_type.to_ascii_lowercase())
            .collect()
    }

    pub(super) fn image_models_by_account_type(
        &self,
    ) -> HashMap<AccountModelGroup, HashSet<String>> {
        let mut result = HashMap::new();
        let snapshot = self.snapshot.read().expect("account snapshot lock");
        for slot in snapshot.accounts.iter().filter(|slot| {
            has_verified_web_image_capability(&slot.record)
                && slot
                    .record
                    .raw
                    .get("_verified_image_capability")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
                && slot
                    .record
                    .raw
                    .get("quota")
                    .is_some_and(|value| image_quota(Some(value)).is_some())
        }) {
            result
                .entry(slot.record.account_type.to_ascii_lowercase())
                .or_insert_with(HashSet::new)
                .extend(WEB_IMAGE_MODELS.iter().map(|id| (*id).to_owned()));
        }
        result
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

    pub(super) async fn mark_text_used(&self, token: &str) -> bool {
        if token.is_empty() {
            return false;
        }
        #[cfg(test)]
        if self
            .snapshot
            .read()
            .ok()
            .and_then(|snapshot| {
                snapshot
                    .accounts
                    .iter()
                    .find(|slot| slot.record.token == token)
                    .map(|slot| slot.fail_usage_marker.load(Ordering::Acquire))
            })
            .unwrap_or(false)
        {
            return false;
        }
        let token = token.to_owned();
        let timestamp = current_local_timestamp();
        self.mutate_raw(|records| {
            let Some(account) = records
                .iter_mut()
                .find(|account| account_payload_token(account).as_deref() == Some(token.as_str()))
            else {
                return Ok(false);
            };
            let Some(object) = account.as_object_mut() else {
                return Ok(false);
            };
            object.insert(
                "last_used_at".to_owned(),
                serde_json::Value::String(timestamp),
            );
            Ok(true)
        })
        .await
        .unwrap_or(false)
    }

    pub(super) fn last_used_at(&self, token: &str) -> Option<String> {
        let snapshot = self.snapshot.read().ok()?;
        snapshot
            .accounts
            .iter()
            .find(|slot| slot.record.token == token)
            .and_then(|slot| slot.last_used_at.read().ok()?.clone())
    }

    pub(super) fn image_inflight(&self, token: &str) -> usize {
        self.snapshot
            .read()
            .ok()
            .and_then(|snapshot| {
                snapshot
                    .accounts
                    .iter()
                    .find(|slot| slot.record.token == token)
                    .map(|slot| slot.image_inflight.load(Ordering::Acquire))
            })
            .unwrap_or_default()
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
        if self.backend.is_some() {
            let _reload_guard = self.reload_gate.lock().await;
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
            self.replace_raw_locked(
                serde_json::Value::Array(records),
                expected_fingerprint,
                None,
            )
            .await
            .map_err(|_| ApiError::unavailable())?;
            return Ok(result);
        }
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
        self.replace_raw_locked(
            serde_json::Value::Array(records),
            expected_fingerprint,
            None,
        )
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
        let _mutation_guard = self.mutation_gate.lock().await;
        if self.backend.is_some() {
            let _reload_guard = self.reload_gate.lock().await;
            return self.merge_import_records_locked(&incoming, 8).await;
        }
        let path = self
            .path
            .as_deref()
            .ok_or_else(ApiError::unsupported_capability)?;
        let _file_lock = super::acquire_path_write_lock(path).await?;
        self.merge_import_records_locked(&incoming, 1).await
    }

    async fn merge_import_records_locked(
        &self,
        incoming: &[serde_json::Value],
        attempts: usize,
    ) -> Result<(usize, usize), ApiError> {
        for attempt in 0..attempts {
            if !self.reload().await {
                return Err(ApiError::unavailable());
            }
            let (expected_fingerprint, cumulative_total) = {
                let snapshot = self.snapshot.read().expect("account snapshot lock");
                (snapshot.fingerprint, snapshot.health.cumulative_total)
            };
            let mut records = self.raw_records();
            let runtime_created_at = self
                .records()
                .into_iter()
                .map(|record| (record.token, record.created_at))
                .collect::<HashMap<_, _>>();
            for record in &mut records {
                if record
                    .get("created_at")
                    .is_none_or(|value| !account_value_truthy(Some(value)))
                    && let Some(token) = account_payload_token(record)
                    && let Some(created_at) = runtime_created_at.get(&token)
                    && let Some(object) = record.as_object_mut()
                {
                    object.insert(
                        "created_at".to_owned(),
                        serde_json::Value::String(created_at.clone()),
                    );
                }
            }
            let result = merge_import_records_in_place(&mut records, incoming)?;
            let next_cumulative_total = cumulative_total
                .checked_add(u64::try_from(result.0).map_err(|_| ApiError::invalid_request())?)
                .ok_or_else(ApiError::invalid_request)?;
            match self
                .replace_raw_locked(
                    serde_json::Value::Array(records),
                    expected_fingerprint,
                    Some(next_cumulative_total),
                )
                .await
            {
                Ok(()) => return Ok(result),
                Err(StorageError::Conflict) if attempt + 1 < attempts => {
                    if !self.reload_backend_fresh().await {
                        return Err(ApiError::unavailable());
                    }
                    continue;
                }
                Err(_) => return Err(ApiError::unavailable()),
            }
        }
        Err(ApiError::unavailable())
    }

    pub(super) async fn update_refreshed_account(
        &self,
        old_token: &str,
        mut updated: serde_json::Value,
    ) -> Result<bool, ApiError> {
        self.mutate_raw(|records| {
            let Some(target) = records
                .iter_mut()
                .find(|item| account_payload_token(item).as_deref() == Some(old_token))
            else {
                return Ok(false);
            };
            if let Some(object) = updated.as_object_mut() {
                object.remove("success");
                object.remove("fail");
            }
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

    pub(super) async fn record_invalid_token(
        &self,
        token: &str,
        _error: &str,
        remove_invalid: bool,
    ) -> Result<bool, ApiError> {
        if token.is_empty() {
            return Ok(false);
        }
        self.mutate_raw(|records| {
            let Some(index) = records
                .iter()
                .position(|item| account_payload_token(item).as_deref() == Some(token))
            else {
                return Ok(false);
            };
            if remove_invalid {
                records.remove(index);
            } else if let Some(object) = records[index].as_object_mut() {
                object.insert(
                    "status".to_owned(),
                    serde_json::Value::String("异常".to_owned()),
                );
                object.insert("quota".to_owned(), serde_json::Value::from(0));
            }
            Ok(true)
        })
        .await
    }

    async fn replace_raw_locked(
        &self,
        value: serde_json::Value,
        expected_fingerprint: [u8; 32],
        next_cumulative_total: Option<u64>,
    ) -> Result<(), StorageError> {
        if let Some(backend) = self.backend.clone() {
            let records = value.as_array().cloned().ok_or(StorageError::Invalid)?;
            let cumulative_total = match next_cumulative_total {
                Some(value) => value,
                None => {
                    self.snapshot
                        .read()
                        .map_err(|_| StorageError::Unavailable)?
                        .health
                        .cumulative_total
                }
            };
            let saved = match backend
                .save_accounts(expected_fingerprint, records, cumulative_total)
                .await
            {
                Ok(saved) => saved,
                Err(StorageError::Conflict) => return Err(StorageError::Conflict),
                Err(error) => {
                    self.invalidate();
                    return Err(error);
                }
            };
            let (accounts, fingerprint, cumulative_total) =
                parse_backend_snapshot(saved).map_err(|_| {
                    self.invalidate();
                    StorageError::Invalid
                })?;
            let health = AccountHealthStats::from_records(&accounts, cumulative_total);
            let mut snapshot = self.snapshot.write().expect("account snapshot lock");
            snapshot.generation = snapshot.generation.saturating_add(1);
            snapshot.fingerprint = fingerprint;
            snapshot.file_version = None;
            snapshot.accounts = Arc::new(account_slots_with_runtime_state(
                accounts,
                Some(snapshot.accounts.as_ref()),
            ));
            snapshot.health = health;
            snapshot.valid = true;
            drop(snapshot);
            self.notify_health_snapshot_sync();
            return Ok(());
        }
        let Some(path) = self.path.clone() else {
            return Err(StorageError::Unsupported);
        };
        let target = path.as_ref().clone();
        let result = tokio::task::spawn_blocking(move || {
            let (current, _, fingerprint, _version) =
                super::read_account_document(&target).map_err(|_| StorageError::Unavailable)?;
            if fingerprint != expected_fingerprint {
                return Err(StorageError::Conflict);
            }
            let value = if next_cumulative_total.is_none() {
                compact_account_records_for_write(&current, &value)?
            } else {
                value
            };
            let output = match current {
                serde_json::Value::Object(mut object) if object.get("items").is_some() => {
                    object.insert("items".to_owned(), value);
                    if let Some(cumulative_total) = next_cumulative_total {
                        object.insert(
                            "cumulative_total".to_owned(),
                            serde_json::json!(cumulative_total),
                        );
                    }
                    serde_json::Value::Object(object)
                }
                _ if next_cumulative_total.is_some() => serde_json::json!({
                    "items": value,
                    "cumulative_total": next_cumulative_total.expect("checked cumulative total"),
                }),
                _ => value,
            };
            let bytes = serde_json::to_vec(&output).map_err(|_| StorageError::Invalid)?;
            super::account_snapshot::validate_bytes(&bytes).map_err(|_| StorageError::Invalid)?;
            super::atomic_replace_checked_with_limit(
                &target,
                &bytes,
                super::MAX_ACCOUNT_SNAPSHOT_BYTES,
                false,
            )
            .map_err(|_| StorageError::Unavailable)?;
            super::read_account_snapshot(&target).map_err(|_| StorageError::Unavailable)
        })
        .await;
        let (accounts, fingerprint, file_version, cumulative_total) = match result {
            Ok(Ok(committed)) => committed,
            Ok(Err(StorageError::Conflict)) => return Err(StorageError::Conflict),
            Ok(Err(error)) => {
                self.invalidate();
                return Err(error);
            }
            Err(_) => {
                self.invalidate();
                return Err(StorageError::Unavailable);
            }
        };
        let health = AccountHealthStats::from_records(&accounts, cumulative_total);
        let mut snapshot = self.snapshot.write().expect("account snapshot lock");
        snapshot.generation = snapshot.generation.saturating_add(1);
        snapshot.fingerprint = fingerprint;
        snapshot.file_version = Some(file_version);
        snapshot.accounts = Arc::new(account_slots_with_runtime_state(
            accounts,
            Some(snapshot.accounts.as_ref()),
        ));
        snapshot.health = health;
        snapshot.valid = true;
        drop(snapshot);
        self.notify_health_snapshot_sync();
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn inflight(&self) -> usize {
        self.snapshot
            .read()
            .expect("account snapshot lock")
            .accounts
            .iter()
            .map(|slot| {
                slot.inflight.load(Ordering::Acquire) + slot.image_inflight.load(Ordering::Acquire)
            })
            .sum()
    }

    #[cfg(test)]
    pub(super) fn force_usage_marker_failure_for_test(&self, token: &str) -> bool {
        let snapshot = self.snapshot.read().expect("account snapshot lock");
        let Some(slot) = snapshot
            .accounts
            .iter()
            .find(|slot| slot.record.token == token)
            .cloned()
        else {
            return false;
        };
        !slot.fail_usage_marker.swap(true, Ordering::AcqRel)
    }
}

fn compact_account_records_for_write(
    original: &serde_json::Value,
    updated: &serde_json::Value,
) -> Result<serde_json::Value, StorageError> {
    let original_items = match original {
        serde_json::Value::Array(items) => items,
        serde_json::Value::Object(object) => object
            .get("items")
            .and_then(serde_json::Value::as_array)
            .ok_or(StorageError::Invalid)?,
        _ => return Err(StorageError::Invalid),
    };
    let updated_items = updated.as_array().ok_or(StorageError::Invalid)?;
    let mut updated_by_token = updated_items
        .iter()
        .filter_map(|item| account_payload_token(item).map(|token| (token, item.clone())))
        .collect::<HashMap<_, _>>();
    let mut compacted = Vec::with_capacity(updated_items.len());

    for original_item in original_items {
        let token = original_item
            .as_object()
            .and_then(|object| {
                object
                    .get("access_token")
                    .or_else(|| object.get("accessToken"))
            })
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .map(ToOwned::to_owned)
            .ok_or(StorageError::Invalid)?;
        let Some(updated_item) = updated_by_token.remove(&token) else {
            continue;
        };
        let original_object = original_item.as_object().ok_or(StorageError::Invalid)?;
        let updated_object = updated_item.as_object().ok_or(StorageError::Invalid)?;
        let normalized_original =
            super::canonicalize_account_item(original_item).map_err(|_| StorageError::Invalid)?;
        let normalized_original = normalized_original
            .as_object()
            .ok_or(StorageError::Invalid)?;
        let mut item = original_object.clone();

        item.retain(|key, _| key == "accessToken" || updated_object.contains_key(key.as_str()));
        for (key, value) in updated_object {
            if key == "access_token" {
                item.insert(key.clone(), value.clone());
                continue;
            }
            if normalized_original.get(key) == Some(value) {
                if let Some(original_value) = original_object.get(key) {
                    item.insert(key.clone(), original_value.clone());
                } else {
                    item.remove(key);
                }
            } else {
                item.insert(key.clone(), value.clone());
            }
        }
        for key in [
            "accessToken",
            "token",
            "refresh_token",
            "id_token",
            "_refresh_token",
            "_id_token",
        ] {
            item.remove(key);
        }
        compacted.push(serde_json::Value::Object(item));
    }

    for updated_item in updated_items {
        if let Some(token) = account_payload_token(updated_item)
            && let Some(item) = updated_by_token.remove(&token)
        {
            compacted.push(item);
        }
    }
    Ok(serde_json::Value::Array(compacted))
}

fn merge_import_records_in_place(
    records: &mut Vec<serde_json::Value>,
    incoming: &[serde_json::Value],
) -> Result<(usize, usize), ApiError> {
    let mut index_by_token = HashMap::new();
    for (index, record) in records.iter().enumerate() {
        if let Some(token) = account_payload_token(record) {
            index_by_token.insert(token, index);
        }
    }
    let mut added = 0usize;
    let mut skipped = 0usize;
    let mut incoming_keys = HashMap::<String, usize>::new();
    let mut deduped_incoming = Vec::<(String, serde_json::Value)>::new();
    for value in incoming {
        let value = prepare_import_account_value(value);
        let Some(token) = account_payload_token(&value) else {
            continue;
        };
        if let Some(index) = incoming_keys.get(&token).copied() {
            let previous = &deduped_incoming[index].1;
            let merged = merge_import_payload_values(previous, &value);
            deduped_incoming[index].1 = merged;
            continue;
        }
        incoming_keys.insert(token, deduped_incoming.len());
        deduped_incoming.push((String::new(), value));
    }
    for (_, value) in deduped_incoming {
        let Some(token) = account_payload_token(&value) else {
            continue;
        };
        if let Some(index) = index_by_token.get(&token).copied() {
            let merged = merge_import_account_values(&records[index], &value);
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
            let value = merge_import_account_values(&serde_json::json!({}), &value);
            records.push(value);
            index_by_token.insert(token, index);
            added += 1;
        }
    }
    if records.len() > super::MAX_ACCOUNTS {
        return Err(ApiError::invalid_request());
    }
    Ok((added, skipped))
}

pub(super) fn python_account_value_string(value: &serde_json::Value) -> String {
    fn python_string_repr(value: &str) -> String {
        let quote = if !value.contains('\'') {
            '\''
        } else if !value.contains('"') {
            '"'
        } else {
            '\''
        };
        let mut output = String::with_capacity(value.len() + 2);
        output.push(quote);
        for character in value.chars() {
            match character {
                '\\' => output.push_str("\\\\"),
                '\n' => output.push_str("\\n"),
                '\r' => output.push_str("\\r"),
                '\t' => output.push_str("\\t"),
                character if character == quote => {
                    output.push('\\');
                    output.push(character);
                }
                character => output.push(character),
            }
        }
        output.push(quote);
        output
    }
    fn python_repr(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::Null => "None".to_owned(),
            serde_json::Value::Bool(true) => "True".to_owned(),
            serde_json::Value::Bool(false) => "False".to_owned(),
            serde_json::Value::Number(value) => super::native_turnstile_json_number(value),
            serde_json::Value::String(value) => python_string_repr(value),
            serde_json::Value::Array(items) => format!(
                "[{}]",
                items.iter().map(python_repr).collect::<Vec<_>>().join(", ")
            ),
            serde_json::Value::Object(items) => format!(
                "{{{}}}",
                items
                    .iter()
                    .map(|(key, value)| {
                        format!("{}: {}", python_string_repr(key), python_repr(value))
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
    match value {
        serde_json::Value::String(value) => value.clone(),
        value => python_repr(value),
    }
}

fn prepare_import_account_value(value: &serde_json::Value) -> serde_json::Value {
    let Some(mut object) = value.as_object().cloned() else {
        return value.clone();
    };
    if !object.contains_key("access_token")
        && let Some(token) = object.remove("accessToken")
    {
        object.insert("access_token".to_owned(), token);
    }
    if let Some(token) = object
        .get("access_token")
        .filter(|value| !value.is_null())
        .map(python_account_value_string)
    {
        object.insert(
            "access_token".to_owned(),
            serde_json::Value::String(token.trim().to_owned()),
        );
    }
    let codex_type = object
        .get("type")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("codex"));
    if codex_type {
        object.insert("export_type".to_owned(), serde_json::json!("codex"));
        object.insert("source_type".to_owned(), serde_json::json!("codex"));
        object.remove("type");
    }
    if object
        .get("export_type")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("codex"))
    {
        object.insert("source_type".to_owned(), serde_json::json!("codex"));
    }
    if object
        .get("plan_type")
        .is_some_and(|value| account_value_truthy(Some(value)))
        && !object
            .get("type")
            .is_some_and(|value| account_value_truthy(Some(value)))
    {
        let plan_type = object
            .get("plan_type")
            .map(python_account_value_string)
            .unwrap_or_default()
            .trim()
            .to_owned();
        object.insert("type".to_owned(), serde_json::Value::String(plan_type));
    } else if let Some(account_type) = object
        .get("type")
        .filter(|value| account_value_truthy(Some(value)))
        .map(python_account_value_string)
    {
        object.insert("type".to_owned(), serde_json::Value::String(account_type));
    }
    serde_json::Value::Object(object)
}

fn merge_import_account_values(
    current: &serde_json::Value,
    incoming: &serde_json::Value,
) -> serde_json::Value {
    let mut merged = current.as_object().cloned().unwrap_or_default();
    if let Some(object) = incoming.as_object() {
        for (key, value) in object {
            if key == "created_at" && !account_value_truthy(Some(value)) {
                continue;
            }
            merged.insert(key.clone(), value.clone());
        }
    }
    for key in [
        "accessToken",
        "token",
        "refresh_token",
        "id_token",
        "_refresh_token",
        "_id_token",
    ] {
        merged.remove(key);
    }
    let token = account_payload_token(incoming)
        .or_else(|| account_payload_token(current))
        .unwrap_or_default();
    merged.insert("access_token".to_owned(), serde_json::Value::String(token));
    let account_type = incoming
        .get("type")
        .filter(|value| account_value_truthy(Some(value)))
        .or_else(|| {
            current
                .get("type")
                .filter(|value| account_value_truthy(Some(value)))
        });
    let account_type = match account_type {
        Some(serde_json::Value::Null) | None => "free".to_owned(),
        Some(value) => python_account_value_string(value),
    };
    merged.insert("type".to_owned(), serde_json::Value::String(account_type));
    if merged
        .get("status")
        .is_none_or(|value| !account_value_truthy(Some(value)))
    {
        merged.insert("status".to_owned(), serde_json::json!("正常"));
    }
    if merged
        .get("type")
        .and_then(serde_json::Value::as_str)
        .is_none_or(|value| value.trim().is_empty())
    {
        merged.insert("type".to_owned(), serde_json::json!("free"));
    }
    if merged
        .get("source_type")
        .and_then(serde_json::Value::as_str)
        .is_none_or(|value| value.trim().is_empty())
    {
        merged.insert("source_type".to_owned(), serde_json::json!("web"));
    }
    if merged
        .get("created_at")
        .and_then(serde_json::Value::as_str)
        .is_none_or(|value| value.trim().is_empty())
    {
        merged.insert(
            "created_at".to_owned(),
            serde_json::Value::String(super::current_timestamp()),
        );
    }
    serde_json::Value::Object(merged)
}

fn merge_import_payload_values(
    current: &serde_json::Value,
    incoming: &serde_json::Value,
) -> serde_json::Value {
    let mut merged = current.as_object().cloned().unwrap_or_default();
    if let Some(object) = incoming.as_object() {
        for (key, value) in object {
            merged.insert(key.clone(), value.clone());
        }
    }
    serde_json::Value::Object(merged)
}

pub(super) fn parse_backend_snapshot(
    snapshot: StorageSnapshot,
) -> Result<(Vec<AccountRecord>, [u8; 32], u64), AppInitError> {
    let cumulative_total = snapshot
        .cumulative_total
        .unwrap_or(snapshot.records.len() as u64);
    let value = if snapshot.cumulative_total.is_some() {
        serde_json::json!({
            "items": snapshot.records,
            "cumulative_total": cumulative_total,
        })
    } else {
        serde_json::Value::Array(snapshot.records)
    };
    let bytes = serde_json::to_vec(&value).map_err(|_| AppInitError::AccountSnapshot)?;
    let (_, records, _) = super::parse_account_document_bytes(&bytes)?;
    Ok((records, snapshot.revision, cumulative_total))
}

fn account_payload_token(value: &serde_json::Value) -> Option<String> {
    value
        .as_object()
        .and_then(|object| object.get("access_token"))
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
    let id_payload = serde_json::Value::Null;
    let account_id = claim_field(
        &access_payload,
        &["https://api.openai.com/auth", "chatgpt_account_id"],
    )
    .or_else(|| nonempty_field(object.get("account_id")))
    .or_else(|| nonempty_field(object.get("chatgpt_account_id")));
    if let Some(account_id) = account_id {
        return Some(format!("account_id:{account_id}"));
    }
    let subject = claim_field(&access_payload, &["sub"])
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
    for key in [
        "accessToken",
        "token",
        "refresh_token",
        "id_token",
        "_refresh_token",
        "_id_token",
    ] {
        merged.remove(key);
    }
    merged.insert(
        "access_token".to_owned(),
        serde_json::Value::String(preferred_token),
    );
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
    image_inflight: Arc<AtomicUsize>,
    last_used_at: Option<String>,
}

fn python_clean_last_used_at(value: Option<&serde_json::Value>) -> Option<String> {
    value
        .filter(|value| account_value_truthy(Some(value)))
        .map(python_account_value_string)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn latest_last_used_at(left: String, right: String) -> Option<String> {
    Some(if left >= right { left } else { right })
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
    let mut image_inflight_by_identity = HashMap::<String, Arc<AtomicUsize>>::new();
    let mut last_used_by_identity = HashMap::<String, String>::new();
    if let Some(previous) = previous {
        for slot in previous {
            let identity = account_identity_key(&slot.record.raw);
            if let Some(identity) = identity.as_ref() {
                image_inflight_by_identity
                    .entry(identity.clone())
                    .or_insert_with(|| slot.image_inflight.clone());
            }
            let last_used_at = slot.last_used_at.read().ok().and_then(|value| {
                value
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned)
            });
            previous_by_token.insert(
                slot.record.token.clone(),
                PreviousAccountRuntime {
                    identity: identity.clone(),
                    image_inflight: slot.image_inflight.clone(),
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
                .and_then(|value| python_clean_last_used_at(Some(value)));
            let identity = account_identity_key(&record.raw);
            let image_inflight = previous_by_token
                .get(&record.token)
                .map(|previous| previous.image_inflight.clone())
                .or_else(|| {
                    identity
                        .as_ref()
                        .and_then(|identity| image_inflight_by_identity.get(identity).cloned())
                })
                .unwrap_or_else(|| Arc::new(AtomicUsize::new(0)));
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
                image_inflight,
                last_used_at: RwLock::new(last_used_at),
                #[cfg(test)]
                fail_usage_marker: AtomicBool::new(false),
            })
        })
        .collect()
}

pub(super) fn current_timestamp() -> String {
    format_account_timestamp(time::OffsetDateTime::now_utc())
}

pub(super) fn current_local_timestamp() -> String {
    format_account_timestamp(
        time::OffsetDateTime::now_local().unwrap_or_else(|_| time::OffsetDateTime::now_utc()),
    )
}

fn format_account_timestamp(now: time::OffsetDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        now.year(),
        now.month() as u8,
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        AccountHealthStats, AccountModelGroup, AccountRecord, AccountStore,
        current_local_timestamp, current_timestamp, has_verified_web_image_capability,
        is_codex_image_account_eligible, is_image_account_available, is_request_eligible_record,
    };
    use serde_json::json;
    use std::{collections::HashSet, fs};

    #[test]
    fn account_timestamp_uses_utc_like_python() {
        let format = time::format_description::parse_borrowed::<1>(
            "[year]-[month]-[day] [hour]:[minute]:[second]",
        )
        .expect("timestamp format");
        let before = time::OffsetDateTime::now_utc().unix_timestamp();
        let timestamp = current_timestamp();
        let after = time::OffsetDateTime::now_utc().unix_timestamp();
        let parsed = time::PrimitiveDateTime::parse(&timestamp, &format)
            .expect("naive account timestamp")
            .assume_utc()
            .unix_timestamp();
        assert!((before..=after).contains(&parsed));
    }
    #[test]
    fn account_value_string_matches_python_str_for_containers() {
        assert_eq!(
            super::python_account_value_string(&json!(["web", true, null, {"nested": "x"}])),
            "['web', True, None, {'nested': 'x'}]"
        );
        assert_eq!(
            super::python_account_value_string(&json!({"proxy": "http://example.test"})),
            "{'proxy': 'http://example.test'}"
        );
        assert_eq!(
            super::python_account_value_string(&json!("  raw value  ")),
            "  raw value  "
        );
        assert_eq!(super::python_account_value_string(&json!(1e-7)), "1e-07");
        assert_eq!(super::python_account_value_string(&json!(1e20)), "1e+20");
    }
    #[test]
    fn imported_type_merge_uses_python_repr_for_containers() {
        let merged = super::merge_import_account_values(
            &json!({"access_token":"token","type":"free"}),
            &json!({"access_token":"token","type":["Team", true]}),
        );
        assert_eq!(merged["type"], "['Team', True]");
    }

    #[test]
    fn imported_access_token_uses_python_string_coercion() {
        let mut records = Vec::new();
        let (added, skipped) =
            super::merge_import_records_in_place(&mut records, &[json!({"access_token": 42})])
                .expect("numeric access token is coerced like Python");
        assert_eq!((added, skipped), (1, 0));
        assert_eq!(records[0]["access_token"], "42");
    }

    #[test]
    fn last_used_timestamp_uses_local_time_like_python() {
        let format = time::format_description::parse_borrowed::<1>(
            "[year]-[month]-[day] [hour]:[minute]:[second]",
        )
        .expect("timestamp format");
        let offset = time::UtcOffset::current_local_offset().unwrap_or(time::UtcOffset::UTC);
        let before = time::OffsetDateTime::now_utc();
        let timestamp = current_local_timestamp();
        let after = time::OffsetDateTime::now_utc();
        let parsed = time::PrimitiveDateTime::parse(&timestamp, &format)
            .expect("naive local timestamp")
            .assume_offset(offset);
        assert!(
            (before.unix_timestamp()..=after.unix_timestamp()).contains(&parsed.unix_timestamp())
        );
    }

    fn record(status: &str, invalid_count: u64) -> AccountRecord {
        AccountRecord {
            token: "token".to_owned(),
            created_at: String::new(),
            status: status.to_owned(),
            source_type: "web".to_owned(),
            chatgpt_account_id: None,
            account_type: "free".to_owned(),
            models: Vec::new(),
            raw: json!({
                "access_token": "token",
                "invalid_count": invalid_count,
                "quota": 1,
                "_verified_image_capability": true
            }),
        }
    }

    #[test]
    fn text_pool_keeps_limited_and_deferred_invalid_accounts_eligible() {
        let limited = record("限流", 0);
        let deferred_invalid = record("正常", 1);

        assert!(is_request_eligible_record(&limited));
        assert!(is_request_eligible_record(&deferred_invalid));
        assert!(!has_verified_web_image_capability(&limited));
        assert!(has_verified_web_image_capability(&deferred_invalid));
        assert!(!is_request_eligible_record(&record("禁用", 0)));
        assert!(!is_request_eligible_record(&record("异常", 0)));
    }

    #[test]
    fn image_pool_excludes_only_python_blocked_statuses() {
        for status in ["正常", "unknown", " 异常 "] {
            assert!(
                is_image_account_available(&record(status, 0)),
                "Python accepts image status {status:?} when quota is positive"
            );
        }
        for status in ["禁用", "限流", "异常"] {
            assert!(
                !is_image_account_available(&record(status, 0)),
                "Python blocks image status {status:?}"
            );
        }

        let mut codex = record("unknown", 0);
        codex.source_type = "codex".to_owned();
        codex.account_type = "pro".to_owned();
        let allowed = HashSet::from(["pro".to_owned()]);
        assert!(is_codex_image_account_eligible(&codex, Some(&allowed)));
        codex.status = "异常".to_owned();
        assert!(!is_codex_image_account_eligible(&codex, Some(&allowed)));
    }

    #[test]
    fn health_type_counts_match_python_stored_type_labels() {
        let make_record = |token: &str, account_type: &str| AccountRecord {
            token: token.to_owned(),
            created_at: String::new(),
            status: "正常".to_owned(),
            source_type: "web".to_owned(),
            chatgpt_account_id: None,
            account_type: account_type.to_ascii_lowercase(),
            models: Vec::new(),
            raw: json!({
                "access_token": token,
                "type": account_type,
                "status": "正常"
            }),
        };
        let health = AccountHealthStats::from_records(
            &[
                make_record("plus-upper", "Plus"),
                make_record("plus-lower", "plus"),
                make_record("team-alias", "Business"),
            ],
            3,
        );
        assert_eq!(health.by_type.get("Plus"), Some(&1));
        assert_eq!(health.by_type.get("plus"), Some(&1));
        assert_eq!(health.by_type.get("Business"), Some(&1));
        assert_eq!(health.by_type.get("other"), None);
    }

    #[tokio::test]
    async fn editable_account_selection_is_oldest_last_used_and_includes_codex() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-editable-lru-{}-{nonce}.json",
            std::process::id()
        ));
        fs::write(
            &path,
            r#"[
                {"access_token":"first","status":"正常","type":"Plus","source_type":"web","last_used_at":"2026-09-20 10:00:00"},
                {"access_token":"codex-oldest","status":"正常","type":"Plus","source_type":"codex","last_used_at":"2026-09-19 10:00:00"},
                {"access_token":"disabled","status":"禁用","type":"Plus","last_used_at":""},
                {"access_token":"free","status":"正常","type":"free","last_used_at":""}
            ]"#
                .as_bytes(),
        )
        .expect("account snapshot");
        let store = AccountStore::load(Some(&path)).expect("account store");
        let groups = HashSet::<AccountModelGroup>::from([
            "plus".to_owned(),
            "team".to_owned(),
            "pro".to_owned(),
            "enterprise".to_owned(),
        ]);

        let lease = store
            .acquire_least_recently_used_with_types(&groups)
            .await
            .expect("editable account");
        assert_eq!(lease.token(), "codex-oldest");
        assert_eq!(lease.source_type(), "codex");
        drop(lease);
        assert_eq!(store.inflight(), 0);
        fs::remove_file(path).expect("cleanup");
    }

    #[tokio::test]
    async fn text_account_round_robin_counts_only_eligible_accounts() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-text-round-robin-{}-{nonce}.json",
            std::process::id()
        ));
        fs::write(
            &path,
            r#"[
                {"access_token":"disabled","status":"禁用"},
                {"access_token":"first","status":"正常"},
                {"access_token":"abnormal","status":"异常"},
                {"access_token":"second","status":"正常"}
            ]"#,
        )
        .expect("account snapshot");
        let store = AccountStore::load(Some(&path)).expect("account store");
        let mut selected = Vec::new();
        for _ in 0..4 {
            let lease = store
                .acquire_excluding_with_type_and_source_filter("auto", &HashSet::new(), None, None)
                .await
                .expect("eligible text account");
            selected.push(lease.token().to_owned());
            drop(lease);
        }
        assert_eq!(selected, ["first", "second", "first", "second"]);
        fs::remove_file(path).expect("cleanup");
    }
}
