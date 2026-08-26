use std::{
    collections::{HashMap, HashSet},
    fs,
    future::Future,
    path::{Component, Path, PathBuf},
    pin::Pin,
    process::Stdio,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;
#[cfg(test)]
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{
    Any, AnyConnection, AnyPool, Executor, Row, Transaction,
    any::{AnyPoolOptions, AnyRow, install_default_drivers},
    pool::PoolConnection,
};
use tokio::io::AsyncReadExt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StorageSnapshot {
    pub(super) records: Vec<Value>,
    pub(super) revision: [u8; 32],
    pub(super) cumulative_total: Option<u64>,
}

pub(super) type HealthRefreshPermit = Box<dyn Send>;
pub(super) type HealthRefreshCoordinator =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = HealthRefreshPermit> + Send>> + Send + Sync>;
pub(super) type HealthSnapshotPublisher = Arc<
    dyn Fn(StorageSnapshot, StorageSnapshot) -> Pin<Box<dyn Future<Output = bool> + Send>>
        + Send
        + Sync,
>;
pub(super) type HealthSnapshotValidator =
    Arc<dyn Fn(&StorageSnapshot, &StorageSnapshot) -> bool + Send + Sync>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StorageError {
    Conflict,
    Invalid,
    Unavailable,
    Unsupported,
}

#[derive(Clone)]
pub(super) enum StorageBackend {
    Json(Arc<JsonStorage>),
    Database(Arc<DatabaseStorage>),
    Git(Arc<GitStorage>),
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct JsonFileHealth {
    valid: bool,
    version: Option<super::FileVersion>,
}

struct JsonHealthState {
    accounts: JsonFileHealth,
    auth_keys: JsonFileHealth,
    account_count: usize,
    auth_key_count: usize,
}

pub(super) struct JsonStorage {
    accounts_path: PathBuf,
    auth_keys_path: PathBuf,
    operation_gate: tokio::sync::Mutex<()>,
    health_state: StdMutex<JsonHealthState>,
    #[cfg(test)]
    cas_read_hook: StdMutex<Option<JsonCasReadHook>>,
}

#[cfg(test)]
#[derive(Clone)]
struct JsonCasReadHook {
    after_read: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    pause: Arc<AtomicBool>,
}

impl JsonStorage {
    fn new(accounts_path: &Path, auth_keys_path: &Path) -> Result<Self, StorageError> {
        let accounts_path = accounts_path.to_owned();
        let auth_keys_path = auth_keys_path.to_owned();
        for path in [&accounts_path, &auth_keys_path] {
            let parent = path.parent().ok_or(StorageError::Invalid)?;
            fs::create_dir_all(parent).map_err(|_| StorageError::Unavailable)?;
            let metadata = fs::symlink_metadata(parent).map_err(|_| StorageError::Unavailable)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(StorageError::Invalid);
            }
            if let Ok(metadata) = fs::symlink_metadata(path)
                && (metadata.file_type().is_symlink() || !metadata.is_file())
            {
                return Err(StorageError::Invalid);
            }
        }
        Ok(Self {
            accounts_path,
            auth_keys_path,
            operation_gate: tokio::sync::Mutex::new(()),
            health_state: StdMutex::new(JsonHealthState {
                accounts: JsonFileHealth {
                    valid: false,
                    version: None,
                },
                auth_keys: JsonFileHealth {
                    valid: false,
                    version: None,
                },
                account_count: 0,
                auth_key_count: 0,
            }),
            #[cfg(test)]
            cas_read_hook: StdMutex::new(None),
        })
    }

    #[cfg(test)]
    fn set_cas_read_hook(&self, hook: Option<JsonCasReadHook>) {
        *self
            .cas_read_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = hook;
    }

    #[cfg(test)]
    async fn pause_after_cas_read(&self) {
        let hook = self
            .cas_read_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(hook) = hook
            && hook.pause.swap(false, Ordering::SeqCst)
        {
            hook.after_read.notify_one();
            hook.release.notified().await;
        }
    }

    fn missing_snapshot(
        collection: Collection,
    ) -> Result<(StorageSnapshot, Option<super::FileVersion>), StorageError> {
        let cumulative_total = (collection == Collection::Accounts).then_some(0);
        let revision = canonical_revision(&[])?;
        Ok((
            StorageSnapshot {
                records: Vec::new(),
                revision,
                cumulative_total,
            },
            None,
        ))
    }

    fn accounts_from_file(
        path: &Path,
    ) -> Result<(StorageSnapshot, Option<super::FileVersion>), StorageError> {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(StorageError::Invalid);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Self::missing_snapshot(Collection::Accounts);
            }
            Err(_) => return Err(StorageError::Unavailable),
            Ok(_) => {}
        }
        let (value, _parsed, _fingerprint, version) =
            super::read_account_document(path).map_err(|_| StorageError::Invalid)?;
        let records = match &value {
            Value::Array(records) => records.clone(),
            Value::Object(object) => object
                .get("items")
                .and_then(Value::as_array)
                .cloned()
                .ok_or(StorageError::Invalid)?,
            _ => return Err(StorageError::Invalid),
        };
        let total = records.len() as u64;
        let cumulative_total = value
            .as_object()
            .and_then(|object| object.get("cumulative_total"))
            .and_then(Value::as_u64)
            .unwrap_or(total)
            .max(total);
        let revision = canonical_revision(&records)?;
        Ok((
            StorageSnapshot {
                records,
                revision,
                cumulative_total: Some(cumulative_total),
            },
            Some(version),
        ))
    }

    fn auth_keys_from_file(
        path: &Path,
    ) -> Result<(StorageSnapshot, Option<super::FileVersion>), StorageError> {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(StorageError::Invalid);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Self::missing_snapshot(Collection::AuthKeys);
            }
            Err(_) => return Err(StorageError::Unavailable),
            Ok(_) => {}
        }
        let (value, _parsed, _fingerprint, version) =
            super::read_auth_document(path).map_err(|_| StorageError::Invalid)?;
        let records = value
            .as_object()
            .and_then(|object| object.get("items"))
            .and_then(Value::as_array)
            .cloned()
            .ok_or(StorageError::Invalid)?;
        let revision = canonical_revision(&records)?;
        Ok((
            StorageSnapshot {
                records,
                revision,
                cumulative_total: None,
            },
            Some(version),
        ))
    }

    fn publish_account_health(
        &self,
        snapshot: &StorageSnapshot,
        version: Option<super::FileVersion>,
    ) {
        let mut state = self
            .health_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.accounts = JsonFileHealth {
            valid: true,
            version,
        };
        state.account_count = snapshot.records.len();
    }

    fn publish_auth_health(&self, snapshot: &StorageSnapshot, version: Option<super::FileVersion>) {
        let mut state = self
            .health_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.auth_keys = JsonFileHealth {
            valid: true,
            version,
        };
        state.auth_key_count = snapshot.records.len();
    }

    fn invalidate_health(&self) {
        let mut state = self
            .health_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.accounts.valid = false;
        state.auth_keys.valid = false;
        state.account_count = 0;
        state.auth_key_count = 0;
    }

    fn current_version(path: &Path) -> Result<Option<super::FileVersion>, StorageError> {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                Err(StorageError::Invalid)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(StorageError::Unavailable),
            Ok(_) => super::validated_file_version(path)
                .map(Some)
                .map_err(|_| StorageError::Unavailable),
        }
    }

    fn health_is_current(&self, local_snapshot_available: bool) -> Option<Value> {
        if !local_snapshot_available {
            return None;
        }
        let state = self
            .health_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.accounts.valid || !state.auth_keys.valid {
            return None;
        }
        let accounts_current = Self::current_version(&self.accounts_path)
            .ok()
            .is_some_and(|version| version == state.accounts.version);
        let auth_current = Self::current_version(&self.auth_keys_path)
            .ok()
            .is_some_and(|version| version == state.auth_keys.version);
        if !accounts_current || !auth_current {
            return None;
        }
        Some(serde_json::json!({
            "status": "healthy",
            "backend": "json",
            "description": "本地 JSON 存储",
            "account_count": state.account_count,
            "auth_key_count": state.auth_key_count,
        }))
    }

    async fn load_accounts(&self) -> Result<StorageSnapshot, StorageError> {
        let _gate = self.operation_gate.lock().await;
        let path = self.accounts_path.clone();
        let loaded = tokio::task::spawn_blocking(move || Self::accounts_from_file(&path))
            .await
            .map_err(|_| StorageError::Unavailable)??;
        self.publish_account_health(&loaded.0, loaded.1);
        Ok(loaded.0)
    }

    async fn load_auth_keys(&self) -> Result<StorageSnapshot, StorageError> {
        let _gate = self.operation_gate.lock().await;
        let path = self.auth_keys_path.clone();
        let loaded = tokio::task::spawn_blocking(move || Self::auth_keys_from_file(&path))
            .await
            .map_err(|_| StorageError::Unavailable)??;
        self.publish_auth_health(&loaded.0, loaded.1);
        Ok(loaded.0)
    }

    async fn save_accounts(
        &self,
        expected_revision: [u8; 32],
        records: Vec<Value>,
        cumulative_total: u64,
    ) -> Result<StorageSnapshot, StorageError> {
        let next = make_snapshot(Collection::Accounts, records, Some(cumulative_total))?;
        let _gate = self.operation_gate.lock().await;
        let _file_lock = super::acquire_path_write_lock(&self.accounts_path)
            .await
            .map_err(|_| StorageError::Unavailable)?;
        let current = self.load_accounts_locked().await?;
        #[cfg(test)]
        self.pause_after_cas_read().await;
        if current.revision != expected_revision
            || current
                .cumulative_total
                .is_some_and(|value| value > cumulative_total)
        {
            return Err(StorageError::Conflict);
        }
        let bytes = serde_json::to_vec(&serde_json::json!({
            "items": next.records.clone(),
            "cumulative_total": cumulative_total,
        }))
        .map_err(|_| StorageError::Invalid)?;
        self.write_locked(
            &self.accounts_path,
            bytes,
            super::MAX_ACCOUNT_SNAPSHOT_BYTES,
        )
        .await?;
        let committed = self.load_accounts_locked().await?;
        if committed.revision != next.revision
            || committed.cumulative_total != Some(cumulative_total)
        {
            self.invalidate_health();
            return Err(StorageError::Invalid);
        }
        Ok(committed)
    }

    async fn save_auth_keys(
        &self,
        expected_revision: [u8; 32],
        records: Vec<Value>,
    ) -> Result<StorageSnapshot, StorageError> {
        let next = make_snapshot(Collection::AuthKeys, records, None)?;
        let _gate = self.operation_gate.lock().await;
        let _file_lock = super::acquire_path_write_lock(&self.auth_keys_path)
            .await
            .map_err(|_| StorageError::Unavailable)?;
        let current = self.load_auth_keys_locked().await?;
        #[cfg(test)]
        self.pause_after_cas_read().await;
        if current.revision != expected_revision {
            return Err(StorageError::Conflict);
        }
        let bytes = serde_json::to_vec(&serde_json::json!({
            "items": next.records.clone(),
        }))
        .map_err(|_| StorageError::Invalid)?;
        self.write_locked(&self.auth_keys_path, bytes, super::MAX_AUTH_KEYS_BYTES)
            .await?;
        let committed = self.load_auth_keys_locked().await?;
        if committed.revision != next.revision {
            self.invalidate_health();
            return Err(StorageError::Invalid);
        }
        Ok(committed)
    }

    async fn load_accounts_locked(&self) -> Result<StorageSnapshot, StorageError> {
        let path = self.accounts_path.clone();
        let loaded = tokio::task::spawn_blocking(move || Self::accounts_from_file(&path))
            .await
            .map_err(|_| StorageError::Unavailable)??;
        self.publish_account_health(&loaded.0, loaded.1);
        Ok(loaded.0)
    }

    async fn load_auth_keys_locked(&self) -> Result<StorageSnapshot, StorageError> {
        let path = self.auth_keys_path.clone();
        let loaded = tokio::task::spawn_blocking(move || Self::auth_keys_from_file(&path))
            .await
            .map_err(|_| StorageError::Unavailable)??;
        self.publish_auth_health(&loaded.0, loaded.1);
        Ok(loaded.0)
    }

    async fn load_health_snapshots(
        &self,
    ) -> Result<(StorageSnapshot, StorageSnapshot, Value), StorageError> {
        let _gate = self.operation_gate.lock().await;
        let accounts = self.load_accounts_locked().await;
        let auth_keys = self.load_auth_keys_locked().await;
        match (accounts, auth_keys) {
            (Ok(accounts), Ok(auth_keys)) => Ok((
                accounts.clone(),
                auth_keys.clone(),
                serde_json::json!({
                    "status": "healthy",
                    "backend": "json",
                    "description": "本地 JSON 存储",
                    "account_count": accounts.records.len(),
                    "auth_key_count": auth_keys.records.len(),
                }),
            )),
            _ => {
                self.invalidate_health();
                Err(StorageError::Invalid)
            }
        }
    }

    async fn write_locked(
        &self,
        path: &Path,
        bytes: Vec<u8>,
        limit: u64,
    ) -> Result<(), StorageError> {
        if bytes.len() as u64 > limit {
            return Err(StorageError::Invalid);
        }
        let target = path.to_owned();
        let written = tokio::task::spawn_blocking(move || {
            super::atomic_replace_checked_with_limit(&target, &bytes, limit, false).is_ok()
        })
        .await
        .map_err(|_| StorageError::Unavailable)?;
        if !written {
            return Err(StorageError::Unavailable);
        }
        Ok(())
    }

    fn info(&self) -> Value {
        serde_json::json!({
            "type": "json",
            "description": "本地 JSON 存储",
            "accounts_path": self.accounts_path.to_string_lossy(),
            "auth_keys_path": self.auth_keys_path.to_string_lossy(),
        })
    }
}

impl StorageBackend {
    pub(super) async fn connect_configured(
        backend_type: &str,
        database_url: Option<&str>,
        data_dir: &Path,
    ) -> Result<Option<Arc<Self>>, StorageError> {
        match backend_type.trim().to_ascii_lowercase().as_str() {
            "" | "json" => {
                let accounts_path = data_dir.join("accounts.json");
                let auth_keys_path = data_dir.join("auth_keys.json");
                Ok(Some(Arc::new(Self::Json(Arc::new(JsonStorage::new(
                    &accounts_path,
                    &auth_keys_path,
                )?)))))
            }
            "sqlite" | "postgres" | "postgresql" | "mysql" | "database" => {
                let default_url = sqlite_url(&data_dir.join("accounts.db"));
                let database_url = database_url
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .unwrap_or(&default_url);
                Ok(Some(Arc::new(Self::connect_database(database_url).await?)))
            }
            _ => Err(StorageError::Unsupported),
        }
    }

    pub(super) fn connect_json(
        accounts_path: &Path,
        auth_keys_path: &Path,
    ) -> Result<Self, StorageError> {
        Ok(Self::Json(Arc::new(JsonStorage::new(
            accounts_path,
            auth_keys_path,
        )?)))
    }

    pub(super) async fn connect_database(database_url: &str) -> Result<Self, StorageError> {
        Ok(Self::Database(Arc::new(
            DatabaseStorage::connect(database_url).await?,
        )))
    }

    pub(super) async fn connect_git(
        repo_url: &str,
        token: &str,
        branch: &str,
        file_path: &str,
        auth_keys_file_path: &str,
        cache_dir: &Path,
    ) -> Result<Self, StorageError> {
        Ok(Self::Git(Arc::new(
            GitStorage::connect(
                repo_url,
                token,
                branch,
                file_path,
                auth_keys_file_path,
                cache_dir,
            )
            .await?,
        )))
    }

    pub(super) async fn load_accounts(&self) -> Result<StorageSnapshot, StorageError> {
        match self {
            Self::Json(backend) => backend.load_accounts().await,
            Self::Database(backend) => backend.load_accounts().await,
            Self::Git(backend) => backend.load_accounts().await,
        }
    }

    pub(super) async fn save_accounts(
        &self,
        expected_revision: [u8; 32],
        records: Vec<Value>,
        cumulative_total: u64,
    ) -> Result<StorageSnapshot, StorageError> {
        match self {
            Self::Json(backend) => {
                backend
                    .save_accounts(expected_revision, records, cumulative_total)
                    .await
            }
            Self::Database(backend) => {
                backend
                    .save_accounts(expected_revision, records, cumulative_total)
                    .await
            }
            Self::Git(backend) => {
                backend
                    .save_accounts(expected_revision, records, cumulative_total)
                    .await
            }
        }
    }

    pub(super) async fn load_auth_keys(&self) -> Result<StorageSnapshot, StorageError> {
        match self {
            Self::Json(backend) => backend.load_auth_keys().await,
            Self::Database(backend) => backend.load_auth_keys().await,
            Self::Git(backend) => backend.load_auth_keys().await,
        }
    }

    pub(super) async fn save_auth_keys(
        &self,
        expected_revision: [u8; 32],
        records: Vec<Value>,
    ) -> Result<StorageSnapshot, StorageError> {
        match self {
            Self::Json(backend) => backend.save_auth_keys(expected_revision, records).await,
            Self::Database(backend) => backend.save_auth_keys(expected_revision, records).await,
            Self::Git(backend) => backend.save_auth_keys(expected_revision, records).await,
        }
    }

    pub(super) async fn load_health_snapshots(
        &self,
    ) -> Result<(StorageSnapshot, StorageSnapshot, Value), StorageError> {
        match self {
            Self::Json(backend) => backend.load_health_snapshots().await,
            Self::Database(backend) => backend.load_health_snapshots().await,
            Self::Git(backend) => backend.load_health_snapshots().await,
        }
    }

    pub(super) fn cached_health(self: &Arc<Self>, local_snapshot_available: bool) -> Option<Value> {
        match self.as_ref() {
            Self::Json(backend) => backend.health_is_current(local_snapshot_available),
            Self::Database(_) => None,
            Self::Git(backend) => Some(backend.cached_health(local_snapshot_available)),
        }
    }

    pub(super) fn install_health_refresh_coordinator(&self, coordinator: HealthRefreshCoordinator) {
        if let Self::Git(backend) = self {
            backend.install_health_refresh_coordinator(coordinator);
        }
    }

    pub(super) fn install_health_snapshot_publisher(&self, publisher: HealthSnapshotPublisher) {
        if let Self::Git(backend) = self {
            backend.install_health_snapshot_publisher(publisher);
        }
    }

    pub(super) fn install_health_snapshot_validator(&self, validator: HealthSnapshotValidator) {
        if let Self::Git(backend) = self {
            backend.install_health_snapshot_validator(validator);
        }
    }

    pub(super) fn info(&self) -> Value {
        match self {
            Self::Json(backend) => backend.info(),
            Self::Database(backend) => backend.info(),
            Self::Git(backend) => backend.info(),
        }
    }

    pub(super) async fn close(&self) {
        match self {
            Self::Json(_) => {}
            Self::Database(backend) => quiescent_close_database_pool(&backend.pool).await,
            Self::Git(backend) => backend.close().await,
        }
    }
}

const GIT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const GIT_HEALTH_REFRESH_AFTER: Duration = Duration::from_secs(30);
const GIT_HEALTH_RETRY_AFTER: Duration = Duration::from_secs(5);
const GIT_HEALTH_STALE_AFTER: Duration = Duration::from_secs(120);
const GIT_HEALTH_CANDIDATE_NAMESPACE: &str = "refs/chatgpt2api/health-refresh/";
const MAX_GIT_HEALTH_CANDIDATE_REF_BYTES: usize = 64 * 1024;
const MAX_GIT_HEALTH_CANDIDATE_REFS: usize = 256;
const GIT_PENDING_PUSH: &str = ".pending-push";
const SCHEMA_LOCK_NAMESPACE: &[u8] = b"chatgpt2api-storage-schema-v1";
const SERVER_SCHEMA_LOCK_WAIT: Duration = Duration::from_secs(10);
const SERVER_SCHEMA_LOCK_POLL: Duration = Duration::from_millis(50);
const MYSQL_SCHEMA_LOCK_PREFIX: &str = "chatgpt2api-schema-v1-";

fn invalid_database_stage(_stage: &'static str) -> StorageError {
    StorageError::Invalid
}

fn database_row_text(
    row: &AnyRow,
    column: &str,
    kind: DatabaseKind,
    invalid_stage: &'static str,
) -> Result<String, StorageError> {
    if let Ok(value) = row.try_get::<String, _>(column) {
        return Ok(value);
    }
    if kind != DatabaseKind::MySql {
        return Err(invalid_database_stage(invalid_stage));
    }
    let bytes = row
        .try_get::<Vec<u8>, _>(column)
        .map_err(|_| invalid_database_stage(invalid_stage))?;
    String::from_utf8(bytes).map_err(|_| invalid_database_stage(invalid_stage))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GitSyncStatus {
    Fresh,
    Refreshing,
    Retrying,
    Stale,
    Error,
    Closed,
}

struct GitHealthState {
    status: GitSyncStatus,
    last_commit: Option<String>,
    last_sync_unix_ms: Option<u64>,
    last_verified_at: Option<Instant>,
    last_verified_unix_ms: Option<u64>,
    last_attempt_at: Option<Instant>,
}

impl GitHealthState {
    fn uninitialized() -> Self {
        Self {
            status: GitSyncStatus::Error,
            last_commit: None,
            last_sync_unix_ms: None,
            last_verified_at: None,
            last_verified_unix_ms: None,
            last_attempt_at: None,
        }
    }
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or_default()
}

fn parse_git_commit(bytes: &[u8]) -> Result<String, StorageError> {
    let commit = std::str::from_utf8(bytes)
        .map_err(|_| StorageError::Invalid)?
        .trim();
    if !matches!(commit.len(), 40 | 64) || !commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(StorageError::Invalid);
    }
    Ok(commit.to_ascii_lowercase())
}

fn short_git_commit(commit: &str) -> &str {
    commit.get(..8).unwrap_or(commit)
}

fn is_health_candidate_ref(value: &str) -> bool {
    let Some(suffix) = value.strip_prefix(GIT_HEALTH_CANDIDATE_NAMESPACE) else {
        return false;
    };
    let Some((pid, sequence)) = suffix.split_once('-') else {
        return false;
    };
    !pid.is_empty()
        && !sequence.is_empty()
        && pid.bytes().all(|byte| byte.is_ascii_digit())
        && sequence.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
#[derive(Clone)]
struct GitHealthRefreshTestHook {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    starts: Arc<std::sync::atomic::AtomicUsize>,
    fail: Arc<AtomicBool>,
    pause_before_prepare: Arc<AtomicBool>,
    after_fetch: Arc<tokio::sync::Notify>,
    release_after_fetch: Arc<tokio::sync::Notify>,
    pause_after_fetch: Arc<AtomicBool>,
    after_prepare: Arc<tokio::sync::Notify>,
    release_before_commit: Arc<tokio::sync::Notify>,
    pause_after_prepare: Arc<AtomicBool>,
}

struct GitHealthRefreshCandidate {
    accounts: StorageSnapshot,
    auth_keys: StorageSnapshot,
    commit: String,
    canonical_head: String,
    staging_ref: String,
}

pub(super) struct GitStorage {
    repo_url: String,
    redacted_url: String,
    token: String,
    branch: String,
    file_path: String,
    auth_keys_file_path: String,
    cache_dir: PathBuf,
    askpass_path: PathBuf,
    operation_gate: tokio::sync::Mutex<()>,
    health_state: StdMutex<GitHealthState>,
    health_refresh_running: AtomicBool,
    health_refresh_task: StdMutex<Option<tokio::task::JoinHandle<()>>>,
    health_shutdown: AtomicBool,
    health_refresh_coordinator: StdMutex<Option<HealthRefreshCoordinator>>,
    health_snapshot_publisher: StdMutex<Option<HealthSnapshotPublisher>>,
    health_snapshot_validator: StdMutex<Option<HealthSnapshotValidator>>,
    health_candidate_ref: StdMutex<Option<String>>,
    health_candidate_sequence: AtomicU64,
    health_candidate_cleanup_pending: AtomicBool,
    #[cfg(test)]
    health_refresh_test_hook: StdMutex<Option<GitHealthRefreshTestHook>>,
    #[cfg(test)]
    health_candidate_cleanup_fail: AtomicBool,
}

impl GitStorage {
    async fn connect(
        repo_url: &str,
        token: &str,
        branch: &str,
        file_path: &str,
        auth_keys_file_path: &str,
        cache_dir: &Path,
    ) -> Result<Self, StorageError> {
        let repo_url = repo_url.trim();
        if repo_url.is_empty() || repo_url.len() > 8192 {
            return Err(StorageError::Invalid);
        }
        let branch = validate_git_branch(branch)?;
        let file_path = validate_git_relative_path(file_path)?;
        let auth_keys_file_path = validate_git_relative_path(auth_keys_file_path)?;
        if file_path == auth_keys_file_path {
            return Err(StorageError::Invalid);
        }
        let token = token.trim().to_owned();
        let (repo_url, embedded_token) = git_execution_url(repo_url)?;
        let token = if token.is_empty() {
            embedded_token.unwrap_or_default()
        } else {
            token
        };
        let redacted_url = redact_url(repo_url.as_str());
        let cache_dir = cache_dir.to_owned();
        let cache_dir = tokio::task::spawn_blocking(move || prepare_git_cache(&cache_dir))
            .await
            .map_err(|_| StorageError::Unavailable)??;
        let askpass_path = install_git_askpass(&cache_dir)?;
        let backend = Self {
            repo_url,
            redacted_url,
            token,
            branch,
            file_path,
            auth_keys_file_path,
            cache_dir,
            askpass_path,
            operation_gate: tokio::sync::Mutex::new(()),
            health_state: StdMutex::new(GitHealthState::uninitialized()),
            health_refresh_running: AtomicBool::new(false),
            health_refresh_task: StdMutex::new(None),
            health_shutdown: AtomicBool::new(false),
            health_refresh_coordinator: StdMutex::new(None),
            health_snapshot_publisher: StdMutex::new(None),
            health_snapshot_validator: StdMutex::new(None),
            health_candidate_ref: StdMutex::new(None),
            health_candidate_sequence: AtomicU64::new(0),
            health_candidate_cleanup_pending: AtomicBool::new(false),
            #[cfg(test)]
            health_refresh_test_hook: StdMutex::new(None),
            #[cfg(test)]
            health_candidate_cleanup_fail: AtomicBool::new(false),
        };
        {
            let _gate = backend.operation_gate.lock().await;
            let _file_lock = backend.repository_file_lock().await?;
            let repo = backend.sync_repo_locked().await?;
            backend.cleanup_health_candidate_refs_locked(&repo).await?;
            backend.load_accounts_from_repo(&repo)?;
            backend.load_auth_keys_from_repo(&repo)?;
            let commit = backend.current_head(&repo).await?;
            backend.record_sync_success(commit);
        }
        Ok(backend)
    }

    async fn repository_file_lock(&self) -> Result<fs::File, StorageError> {
        super::acquire_path_write_lock(&self.cache_dir.join("repository-owner"))
            .await
            .map_err(|_| StorageError::Unavailable)
    }

    async fn git_output(
        &self,
        current_dir: &Path,
        arguments: &[&str],
    ) -> Result<std::process::Output, StorageError> {
        let mut command = tokio::process::Command::new("git");
        command
            .args(arguments)
            .current_dir(current_dir)
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_ASKPASS", &self.askpass_path)
            .env("GIT_USERNAME", "x-access-token")
            .env("CHATGPT2API_GIT_TOKEN", &self.token)
            .env("GIT_AUTHOR_NAME", "chatgpt2api")
            .env("GIT_AUTHOR_EMAIL", "chatgpt2api@localhost")
            .env("GIT_COMMITTER_NAME", "chatgpt2api")
            .env("GIT_COMMITTER_EMAIL", "chatgpt2api@localhost")
            .env("LC_ALL", "C")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        tokio::time::timeout(GIT_OPERATION_TIMEOUT, command.output())
            .await
            .map_err(|_| StorageError::Unavailable)?
            .map_err(|_| StorageError::Unavailable)
    }

    async fn git_expect(&self, current_dir: &Path, arguments: &[&str]) -> Result<(), StorageError> {
        let output = self.git_output(current_dir, arguments).await?;
        if output.status.success() {
            Ok(())
        } else {
            Err(StorageError::Unavailable)
        }
    }

    async fn git_blob(
        &self,
        repo: &Path,
        commit: &str,
        relative: &str,
        limit: u64,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let spec = format!("{commit}:{relative}");
        let kind = self.git_output(repo, &["cat-file", "-t", &spec]).await?;
        if !kind.status.success() {
            return Ok(None);
        }
        if std::str::from_utf8(&kind.stdout)
            .map_err(|_| StorageError::Invalid)?
            .trim()
            != "blob"
        {
            return Err(StorageError::Invalid);
        }

        let max = usize::try_from(limit).map_err(|_| StorageError::Invalid)?;
        let mut command = tokio::process::Command::new("git");
        command
            .args(["show", "--no-ext-diff", "--no-textconv", &spec])
            .current_dir(repo)
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_ASKPASS", &self.askpass_path)
            .env("GIT_USERNAME", "x-access-token")
            .env("CHATGPT2API_GIT_TOKEN", &self.token)
            .env("LC_ALL", "C")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|_| StorageError::Unavailable)?;
        let mut stdout = child.stdout.take().ok_or(StorageError::Unavailable)?;
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 8192];
        loop {
            let read = tokio::time::timeout(GIT_OPERATION_TIMEOUT, stdout.read(&mut buffer))
                .await
                .map_err(|_| StorageError::Unavailable)?
                .map_err(|_| StorageError::Unavailable)?;
            if read == 0 {
                break;
            }
            if bytes
                .len()
                .checked_add(read)
                .is_none_or(|length| length > max)
            {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return Err(StorageError::Invalid);
            }
            bytes.extend_from_slice(&buffer[..read]);
        }
        let status = tokio::time::timeout(GIT_OPERATION_TIMEOUT, child.wait())
            .await
            .map_err(|_| StorageError::Unavailable)?
            .map_err(|_| StorageError::Unavailable)?;
        if !status.success() {
            return Err(StorageError::Unavailable);
        }
        Ok(Some(bytes))
    }

    async fn current_head(&self, repo: &Path) -> Result<String, StorageError> {
        let output = self.git_output(repo, &["rev-parse", "HEAD"]).await?;
        if !output.status.success() {
            return Err(StorageError::Unavailable);
        }
        parse_git_commit(&output.stdout)
    }

    fn record_sync_success(&self, commit: String) {
        let now = Instant::now();
        let unix_ms = unix_time_millis();
        let mut state = self
            .health_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.status == GitSyncStatus::Closed {
            return;
        }
        state.status = GitSyncStatus::Fresh;
        state.last_commit = Some(commit);
        state.last_sync_unix_ms = Some(unix_ms);
        state.last_verified_at = Some(now);
        state.last_verified_unix_ms = Some(unix_ms);
        state.last_attempt_at = Some(now);
    }

    fn record_sync_failure(&self) {
        let mut state = self
            .health_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.status != GitSyncStatus::Closed {
            state.status = GitSyncStatus::Error;
            state.last_attempt_at = Some(Instant::now());
        }
    }

    fn install_health_refresh_coordinator(&self, coordinator: HealthRefreshCoordinator) {
        *self
            .health_refresh_coordinator
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(coordinator);
    }

    fn install_health_snapshot_publisher(&self, publisher: HealthSnapshotPublisher) {
        *self
            .health_snapshot_publisher
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(publisher);
    }

    fn install_health_snapshot_validator(&self, validator: HealthSnapshotValidator) {
        *self
            .health_snapshot_validator
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(validator);
    }

    fn cached_health(self: &Arc<Self>, local_snapshot_available: bool) -> Value {
        self.start_health_refresh_if_due();
        let state = self
            .health_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let stale_by_age = state
            .last_verified_at
            .is_none_or(|verified| verified.elapsed() > GIT_HEALTH_STALE_AFTER);
        let effective_status = if stale_by_age
            && matches!(
                state.status,
                GitSyncStatus::Fresh | GitSyncStatus::Refreshing | GitSyncStatus::Retrying
            ) {
            GitSyncStatus::Stale
        } else {
            state.status
        };
        let healthy = local_snapshot_available
            && matches!(
                effective_status,
                GitSyncStatus::Fresh | GitSyncStatus::Refreshing
            );
        let sync_status = match effective_status {
            GitSyncStatus::Fresh => "fresh",
            GitSyncStatus::Refreshing => "refreshing",
            GitSyncStatus::Retrying => "refreshing",
            GitSyncStatus::Stale => "stale",
            GitSyncStatus::Error => "error",
            GitSyncStatus::Closed => "closed",
        };
        let mut health = serde_json::json!({
            "status": if healthy { "healthy" } else { "unhealthy" },
            "backend": "git",
            "repo_url": self.redacted_url,
            "branch": self.branch,
            "file_path": self.file_path,
            "auth_keys_file_path": self.auth_keys_file_path,
            "local_snapshot_status": if local_snapshot_available { "available" } else { "unavailable" },
            "sync_status": sync_status,
            "stale": effective_status == GitSyncStatus::Stale,
            "last_sync_unix_ms": state.last_sync_unix_ms,
            "last_verified_unix_ms": state.last_verified_unix_ms,
            "last_commit": state.last_commit.as_deref().map(short_git_commit),
        });
        if !healthy {
            health["error"] = Value::String("存储后端健康检查失败".to_owned());
        }
        health
    }

    fn start_health_refresh_if_due(self: &Arc<Self>) {
        if self.health_shutdown.load(Ordering::Acquire) {
            return;
        }
        let due = {
            let state = self
                .health_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match state.status {
                GitSyncStatus::Closed | GitSyncStatus::Refreshing | GitSyncStatus::Retrying => {
                    false
                }
                GitSyncStatus::Fresh => state
                    .last_verified_at
                    .is_none_or(|verified| verified.elapsed() >= GIT_HEALTH_REFRESH_AFTER),
                GitSyncStatus::Stale | GitSyncStatus::Error => state
                    .last_attempt_at
                    .is_none_or(|attempt| attempt.elapsed() >= GIT_HEALTH_RETRY_AFTER),
            }
        };
        if !due {
            return;
        }

        let mut task_slot = self
            .health_refresh_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if task_slot.as_ref().is_some_and(|task| !task.is_finished()) {
            return;
        }
        let _ = task_slot.take();
        if self.health_shutdown.load(Ordering::Acquire) {
            return;
        }
        {
            let mut state = self
                .health_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.status == GitSyncStatus::Closed {
                return;
            }
            state.status = if matches!(state.status, GitSyncStatus::Stale | GitSyncStatus::Error) {
                GitSyncStatus::Retrying
            } else {
                GitSyncStatus::Refreshing
            };
            state.last_attempt_at = Some(Instant::now());
        }
        self.health_refresh_running.store(true, Ordering::Release);
        let backend = self.clone();
        *task_slot = Some(tokio::spawn(async move {
            backend.run_health_refresh().await;
        }));
    }

    async fn run_health_refresh(self: Arc<Self>) {
        let result = async {
            let candidate = self.prepare_health_refresh_candidate().await?;

            #[cfg(test)]
            {
                let test_hook = self
                    .health_refresh_test_hook
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                if let Some(hook) = test_hook
                    && hook.pause_after_prepare.swap(false, Ordering::SeqCst)
                {
                    hook.after_prepare.notify_one();
                    hook.release_before_commit.notified().await;
                }
            }

            let validator = self
                .health_snapshot_validator
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
                .ok_or(StorageError::Unavailable)?;
            if !(validator)(&candidate.accounts, &candidate.auth_keys) {
                return Err(StorageError::Invalid);
            }

            let _refresh_permit = self.health_refresh_permit().await;
            self.commit_health_refresh_candidate(&candidate).await?;
            let publisher = self
                .health_snapshot_publisher
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
                .ok_or(StorageError::Unavailable)?;
            if !(publisher)(candidate.accounts, candidate.auth_keys).await {
                return Err(StorageError::Invalid);
            }
            Ok(candidate.commit)
        }
        .await;
        self.cleanup_health_refresh_candidate().await;
        if !self.health_shutdown.load(Ordering::Acquire) {
            match result {
                Ok(commit) => self.record_sync_success(commit),
                Err(_) => self.record_sync_failure(),
            }
        }
        self.health_refresh_running.store(false, Ordering::Release);
    }

    async fn health_refresh_permit(&self) -> HealthRefreshPermit {
        let coordinator = self
            .health_refresh_coordinator
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        match coordinator {
            Some(coordinator) => coordinator().await,
            None => Box::new(()),
        }
    }

    async fn close(&self) {
        self.health_shutdown.store(true, Ordering::Release);
        {
            let mut state = self
                .health_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.status = GitSyncStatus::Closed;
        }
        let task = self
            .health_refresh_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        self.cleanup_health_refresh_candidate().await;
        self.health_refresh_running.store(false, Ordering::Release);
    }

    #[cfg(test)]
    fn set_health_refresh_test_hook(&self, hook: Option<GitHealthRefreshTestHook>) {
        *self
            .health_refresh_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = hook;
    }

    #[cfg(test)]
    fn force_health_refresh_due_for_test(&self, stale: bool) {
        let age = if stale {
            GIT_HEALTH_STALE_AFTER + Duration::from_secs(1)
        } else {
            GIT_HEALTH_REFRESH_AFTER + Duration::from_secs(1)
        };
        let past = Instant::now().checked_sub(age).expect("health test age");
        let mut state = self
            .health_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.last_verified_at = Some(past);
        state.last_attempt_at = Some(past);
    }

    #[cfg(test)]
    fn force_health_stale_for_test(&self) {
        let past = Instant::now()
            .checked_sub(GIT_HEALTH_STALE_AFTER + Duration::from_secs(1))
            .expect("health test stale age");
        self.health_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .last_verified_at = Some(past);
    }

    #[cfg(test)]
    fn health_refresh_running_for_test(&self) -> bool {
        self.health_refresh_running.load(Ordering::Acquire)
    }

    #[cfg(test)]
    async fn health_candidate_ref_count_for_test(&self) -> usize {
        let _gate = self.operation_gate.lock().await;
        let _file_lock = self
            .repository_file_lock()
            .await
            .expect("health candidate test file lock");
        let repo = self
            .validate_repo_path()
            .expect("health candidate test repo");
        let output = self
            .git_output(
                &repo,
                &[
                    "for-each-ref",
                    "--format=%(refname)%00%(objectname)",
                    GIT_HEALTH_CANDIDATE_NAMESPACE,
                ],
            )
            .await
            .expect("health candidate test refs");
        assert!(output.status.success());
        output
            .stdout
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .count()
    }

    #[cfg(test)]
    fn health_candidate_cleanup_pending_for_test(&self) -> bool {
        self.health_candidate_cleanup_pending
            .load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn fail_health_candidate_cleanup_for_test(&self, fail: bool) {
        self.health_candidate_cleanup_fail
            .store(fail, Ordering::Release);
    }

    fn repo_path(&self) -> PathBuf {
        self.cache_dir.join("repo")
    }

    fn pending_path(&self) -> PathBuf {
        self.cache_dir.join(GIT_PENDING_PUSH)
    }

    fn validate_pending_marker(&self) -> Result<bool, StorageError> {
        match fs::symlink_metadata(self.pending_path()) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                Err(StorageError::Invalid)
            }
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(_) => Err(StorageError::Unavailable),
        }
    }

    fn validate_repo_path(&self) -> Result<PathBuf, StorageError> {
        let repo = self.repo_path();
        for path in [&repo, &repo.join(".git")] {
            let metadata = fs::symlink_metadata(path).map_err(|_| StorageError::Invalid)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(StorageError::Invalid);
            }
        }
        let canonical = portable_canonicalize(&repo)?;
        if canonical.parent() != Some(self.cache_dir.as_path()) {
            return Err(StorageError::Invalid);
        }
        Ok(canonical)
    }

    async fn fetch_validate_and_promote_remote_locked(
        &self,
        repo: &Path,
    ) -> Result<(StorageSnapshot, StorageSnapshot, String), StorageError> {
        self.git_expect(repo, &["fetch", "--prune", "origin", &self.branch])
            .await?;
        let output = self
            .git_output(repo, &["rev-parse", "FETCH_HEAD^{commit}"])
            .await?;
        if !output.status.success() {
            return Err(StorageError::Unavailable);
        }
        let commit = parse_git_commit(&output.stdout)?;
        let status = self
            .git_output(
                repo,
                &[
                    "status",
                    "--porcelain=v1",
                    "--untracked-files=all",
                    "--",
                    self.file_path.as_str(),
                    self.auth_keys_file_path.as_str(),
                ],
            )
            .await?;
        if !status.status.success() {
            return Err(StorageError::Unavailable);
        }
        if !status.stdout.is_empty() {
            return Err(StorageError::Invalid);
        }
        let accounts = self
            .load_remote_json_value(
                repo,
                &commit,
                &self.file_path,
                super::MAX_ACCOUNT_SNAPSHOT_BYTES,
            )
            .await?;
        let auth_keys = self
            .load_remote_json_value(
                repo,
                &commit,
                &self.auth_keys_file_path,
                super::MAX_AUTH_KEYS_BYTES,
            )
            .await?;
        let accounts = self.accounts_from_value(accounts)?;
        let auth_keys = self.auth_keys_from_value(auth_keys)?;
        self.git_expect(repo, &["reset", "--hard", &commit]).await?;
        Ok((accounts, auth_keys, commit))
    }

    async fn sync_repo_locked(&self) -> Result<PathBuf, StorageError> {
        let pending = self.validate_pending_marker()?;
        let repo_path = self.repo_path();
        if repo_path.exists() {
            let repo = self.validate_repo_path()?;
            if pending {
                self.recover_repo_locked(&repo).await?;
                remove_regular_file(&self.pending_path())?;
            } else {
                self.fetch_validate_and_promote_remote_locked(&repo).await?;
            }
            return Ok(repo);
        }
        if pending {
            return Err(StorageError::Invalid);
        }
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| StorageError::Unavailable)?
            .as_nanos();
        let clone_path = self
            .cache_dir
            .join(format!(".repo.clone.{}.{nonce}.tmp", std::process::id()));
        if clone_path.exists() {
            return Err(StorageError::Invalid);
        }
        let clone_path_text = clone_path.to_string_lossy().into_owned();
        let output = self
            .git_output(
                &self.cache_dir,
                &[
                    "clone",
                    "--branch",
                    &self.branch,
                    "--single-branch",
                    "--",
                    &self.repo_url,
                    &clone_path_text,
                ],
            )
            .await?;
        if !output.status.success() {
            remove_git_clone_temp(&self.cache_dir, &clone_path)?;
            return Err(StorageError::Unavailable);
        }
        if let Err(error) = fs::rename(&clone_path, &repo_path) {
            remove_git_clone_temp(&self.cache_dir, &clone_path)?;
            let _ = error;
            return Err(StorageError::Unavailable);
        }
        self.validate_repo_path()
    }

    async fn recover_repo_locked(&self, repo: &Path) -> Result<(), StorageError> {
        self.fetch_validate_and_promote_remote_locked(repo).await?;
        self.git_expect(
            repo,
            &[
                "clean",
                "-fd",
                "--",
                &self.file_path,
                &self.auth_keys_file_path,
            ],
        )
        .await
    }

    fn load_json_value(
        &self,
        repo: &Path,
        relative: &str,
        limit: u64,
    ) -> Result<Option<Value>, StorageError> {
        let path = repo.join(relative_path_buf(relative)?);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(StorageError::Invalid);
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(StorageError::Unavailable),
        }
        let (bytes, _) =
            super::read_bounded_validated_file(&path, limit).map_err(|_| StorageError::Invalid)?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|_| StorageError::Invalid)
    }

    fn load_json_value_from_bytes(
        &self,
        bytes: Option<Vec<u8>>,
        limit: u64,
    ) -> Result<Option<Value>, StorageError> {
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        if bytes.len() as u64 > limit {
            return Err(StorageError::Invalid);
        }
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|_| StorageError::Invalid)
    }

    async fn load_remote_json_value(
        &self,
        repo: &Path,
        commit: &str,
        relative: &str,
        limit: u64,
    ) -> Result<Option<Value>, StorageError> {
        let bytes = self.git_blob(repo, commit, relative, limit).await?;
        self.load_json_value_from_bytes(bytes, limit)
    }

    fn accounts_from_value(&self, value: Option<Value>) -> Result<StorageSnapshot, StorageError> {
        let (records, cumulative_total) = match value {
            None => (Vec::new(), None),
            Some(Value::Array(records)) => (records, None),
            Some(Value::Object(mut object)) => {
                let cumulative_total = object
                    .remove("cumulative_total")
                    .and_then(|value| value.as_u64())
                    .ok_or(StorageError::Invalid)?;
                let records = object
                    .remove("items")
                    .and_then(|value| value.as_array().cloned())
                    .ok_or(StorageError::Invalid)?;
                (records, Some(cumulative_total))
            }
            Some(_) => return Err(StorageError::Invalid),
        };
        make_snapshot(Collection::Accounts, records, cumulative_total)
    }

    fn auth_keys_from_value(&self, value: Option<Value>) -> Result<StorageSnapshot, StorageError> {
        let records = match value {
            None => Vec::new(),
            Some(Value::Array(records)) => records,
            Some(Value::Object(mut object)) => object
                .remove("items")
                .and_then(|value| value.as_array().cloned())
                .ok_or(StorageError::Invalid)?,
            Some(_) => return Err(StorageError::Invalid),
        };
        make_snapshot(Collection::AuthKeys, records, None)
    }

    fn load_accounts_from_repo(&self, repo: &Path) -> Result<StorageSnapshot, StorageError> {
        let value =
            self.load_json_value(repo, &self.file_path, super::MAX_ACCOUNT_SNAPSHOT_BYTES)?;
        self.accounts_from_value(value)
    }

    fn load_auth_keys_from_repo(&self, repo: &Path) -> Result<StorageSnapshot, StorageError> {
        let value =
            self.load_json_value(repo, &self.auth_keys_file_path, super::MAX_AUTH_KEYS_BYTES)?;
        self.auth_keys_from_value(value)
    }

    async fn prepare_health_refresh_candidate(
        &self,
    ) -> Result<GitHealthRefreshCandidate, StorageError> {
        let _gate = self.operation_gate.lock().await;
        let _file_lock = self.repository_file_lock().await?;
        if self.validate_pending_marker()? {
            return Err(StorageError::Unavailable);
        }
        if self
            .health_candidate_ref
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
        {
            return Err(StorageError::Conflict);
        }

        let repo = self.validate_repo_path()?;
        let canonical_head = self.current_head(&repo).await?;
        #[cfg(test)]
        {
            let test_hook = self
                .health_refresh_test_hook
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            if let Some(hook) = test_hook {
                hook.starts.fetch_add(1, Ordering::SeqCst);
                if hook.pause_before_prepare.load(Ordering::SeqCst) {
                    hook.started.notify_one();
                    hook.release.notified().await;
                }
                if hook.fail.load(Ordering::SeqCst) {
                    return Err(StorageError::Unavailable);
                }
            }
        }
        self.git_expect(&repo, &["fetch", "--prune", "origin", &self.branch])
            .await?;
        #[cfg(test)]
        {
            let test_hook = self
                .health_refresh_test_hook
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            if let Some(hook) = test_hook
                && hook.pause_after_fetch.swap(false, Ordering::SeqCst)
            {
                hook.after_fetch.notify_one();
                hook.release_after_fetch.notified().await;
            }
        }
        let output = self
            .git_output(&repo, &["rev-parse", "FETCH_HEAD^{commit}"])
            .await?;
        if !output.status.success() {
            return Err(StorageError::Unavailable);
        }
        let commit = parse_git_commit(&output.stdout)?;
        let accounts = self
            .load_remote_json_value(
                &repo,
                &commit,
                &self.file_path,
                super::MAX_ACCOUNT_SNAPSHOT_BYTES,
            )
            .await?;
        let auth_keys = self
            .load_remote_json_value(
                &repo,
                &commit,
                &self.auth_keys_file_path,
                super::MAX_AUTH_KEYS_BYTES,
            )
            .await?;
        let accounts = self.accounts_from_value(accounts)?;
        let auth_keys = self.auth_keys_from_value(auth_keys)?;
        let sequence = self
            .health_candidate_sequence
            .fetch_add(1, Ordering::Relaxed);
        let staging_ref = format!(
            "refs/chatgpt2api/health-refresh/{}-{}",
            std::process::id(),
            sequence
        );
        self.git_expect(&repo, &["update-ref", &staging_ref, &commit, ""])
            .await?;
        *self
            .health_candidate_ref
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(staging_ref.clone());
        Ok(GitHealthRefreshCandidate {
            accounts,
            auth_keys,
            commit,
            canonical_head,
            staging_ref,
        })
    }

    async fn commit_health_refresh_candidate(
        &self,
        candidate: &GitHealthRefreshCandidate,
    ) -> Result<(), StorageError> {
        let _gate = self.operation_gate.lock().await;
        let _file_lock = self.repository_file_lock().await?;
        if self.validate_pending_marker()? {
            return Err(StorageError::Unavailable);
        }
        let repo = self.validate_repo_path()?;
        if self.current_head(&repo).await? != candidate.canonical_head {
            return Err(StorageError::Conflict);
        }
        let staging_spec = format!("{}^{{commit}}", candidate.staging_ref);
        let staging = self
            .git_output(&repo, &["rev-parse", &staging_spec])
            .await?;
        if !staging.status.success() || parse_git_commit(&staging.stdout)? != candidate.commit {
            return Err(StorageError::Conflict);
        }
        let remote_ref = format!("refs/remotes/origin/{}", self.branch);
        let remote_spec = format!("{remote_ref}^{{commit}}");
        let remote = self.git_output(&repo, &["rev-parse", &remote_spec]).await?;
        if !remote.status.success() || parse_git_commit(&remote.stdout)? != candidate.commit {
            return Err(StorageError::Conflict);
        }
        let status = self
            .git_output(
                &repo,
                &[
                    "status",
                    "--porcelain=v1",
                    "--untracked-files=all",
                    "--",
                    self.file_path.as_str(),
                    self.auth_keys_file_path.as_str(),
                ],
            )
            .await?;
        if !status.status.success() {
            return Err(StorageError::Unavailable);
        }
        if !status.stdout.is_empty() {
            return Err(StorageError::Conflict);
        }
        self.git_expect(&repo, &["reset", "--hard", &candidate.commit])
            .await?;
        if self.current_head(&repo).await? != candidate.commit {
            return Err(StorageError::Unavailable);
        }
        Ok(())
    }

    async fn cleanup_health_candidate_refs_locked(&self, repo: &Path) -> Result<(), StorageError> {
        #[cfg(test)]
        if self.health_candidate_cleanup_fail.load(Ordering::SeqCst) {
            return Err(StorageError::Unavailable);
        }
        let output = self
            .git_output(
                repo,
                &[
                    "for-each-ref",
                    "--format=%(refname)%00%(objectname)",
                    GIT_HEALTH_CANDIDATE_NAMESPACE,
                ],
            )
            .await?;
        if !output.status.success() || output.stdout.len() > MAX_GIT_HEALTH_CANDIDATE_REF_BYTES {
            return Err(StorageError::Unavailable);
        }
        let text = std::str::from_utf8(&output.stdout).map_err(|_| StorageError::Invalid)?;
        let mut count = 0usize;
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            count = count.saturating_add(1);
            if count > MAX_GIT_HEALTH_CANDIDATE_REFS {
                return Err(StorageError::Invalid);
            }
            let (name, object) = line.split_once('\0').ok_or(StorageError::Invalid)?;
            if object.contains('\0') || !is_health_candidate_ref(name) {
                return Err(StorageError::Invalid);
            }
            let object = parse_git_commit(object.as_bytes())?;
            let kind = self.git_output(repo, &["cat-file", "-t", &object]).await?;
            if !kind.status.success()
                || std::str::from_utf8(&kind.stdout)
                    .map_err(|_| StorageError::Invalid)?
                    .trim()
                    != "commit"
            {
                return Err(StorageError::Invalid);
            }
            self.git_expect(repo, &["update-ref", "-d", name, &object])
                .await?;
        }
        Ok(())
    }

    async fn cleanup_health_refresh_candidate(&self) {
        let _gate = self.operation_gate.lock().await;
        let result = async {
            let _file_lock = self.repository_file_lock().await?;
            let repo = self.validate_repo_path()?;
            self.cleanup_health_candidate_refs_locked(&repo).await
        }
        .await;
        if result.is_ok() {
            let mut candidate = self
                .health_candidate_ref
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *candidate = None;
            self.health_candidate_cleanup_pending
                .store(false, Ordering::Release);
        } else {
            self.health_candidate_cleanup_pending
                .store(true, Ordering::Release);
        }
    }

    async fn load_health_snapshots_consistent(
        &self,
    ) -> Result<(StorageSnapshot, StorageSnapshot, String), StorageError> {
        let _gate = self.operation_gate.lock().await;
        let _file_lock = self.repository_file_lock().await?;
        let repo_path = self.repo_path();
        if repo_path.exists() && !self.validate_pending_marker()? {
            let repo = self.validate_repo_path()?;
            return self.fetch_validate_and_promote_remote_locked(&repo).await;
        }
        let repo = self.sync_repo_locked().await?;
        let accounts = self.load_accounts_from_repo(&repo)?;
        let auth_keys = self.load_auth_keys_from_repo(&repo)?;
        let commit = self.current_head(&repo).await?;
        Ok((accounts, auth_keys, commit))
    }

    async fn load_accounts(&self) -> Result<StorageSnapshot, StorageError> {
        let result = async {
            let _gate = self.operation_gate.lock().await;
            let _file_lock = self.repository_file_lock().await?;
            let repo = self.sync_repo_locked().await?;
            let snapshot = self.load_accounts_from_repo(&repo)?;
            let commit = self.current_head(&repo).await?;
            Ok((snapshot, commit))
        }
        .await;
        match result {
            Ok((snapshot, commit)) => {
                self.record_sync_success(commit);
                Ok(snapshot)
            }
            Err(error) => {
                self.record_sync_failure();
                Err(error)
            }
        }
    }

    async fn load_auth_keys(&self) -> Result<StorageSnapshot, StorageError> {
        let result = async {
            let _gate = self.operation_gate.lock().await;
            let _file_lock = self.repository_file_lock().await?;
            let repo = self.sync_repo_locked().await?;
            let snapshot = self.load_auth_keys_from_repo(&repo)?;
            let commit = self.current_head(&repo).await?;
            Ok((snapshot, commit))
        }
        .await;
        match result {
            Ok((snapshot, commit)) => {
                self.record_sync_success(commit);
                Ok(snapshot)
            }
            Err(error) => {
                self.record_sync_failure();
                Err(error)
            }
        }
    }

    async fn save_accounts(
        &self,
        expected_revision: [u8; 32],
        records: Vec<Value>,
        cumulative_total: u64,
    ) -> Result<StorageSnapshot, StorageError> {
        let result = async {
            let next = make_snapshot(Collection::Accounts, records, Some(cumulative_total))?;
            let _gate = self.operation_gate.lock().await;
            let _file_lock = self.repository_file_lock().await?;
            let repo = self.sync_repo_locked().await?;
            let current = self.load_accounts_from_repo(&repo)?;
            if current.revision != expected_revision
                || current
                    .cumulative_total
                    .is_some_and(|value| value > cumulative_total)
            {
                return Err(StorageError::Conflict);
            }
            let value = serde_json::json!({
                "items": next.records.clone(),
                "cumulative_total": cumulative_total,
            });
            if let Err(error) = self
                .publish_value_locked(
                    &repo,
                    &self.file_path,
                    &value,
                    super::MAX_ACCOUNT_SNAPSHOT_BYTES,
                )
                .await
            {
                if !self.validate_pending_marker()? {
                    let recovered = self.load_accounts_from_repo(&repo)?;
                    if recovered.revision != expected_revision
                        || recovered
                            .cumulative_total
                            .is_some_and(|value| value > cumulative_total)
                    {
                        return Err(StorageError::Conflict);
                    }
                }
                return Err(error);
            }
            let commit = self.current_head(&repo).await.ok();
            Ok((next, commit))
        }
        .await;
        match result {
            Ok((next, commit)) => {
                match commit {
                    Some(commit) => self.record_sync_success(commit),
                    None => self.record_sync_failure(),
                }
                Ok(next)
            }
            Err(error) => {
                self.record_sync_failure();
                Err(error)
            }
        }
    }

    async fn save_auth_keys(
        &self,
        expected_revision: [u8; 32],
        records: Vec<Value>,
    ) -> Result<StorageSnapshot, StorageError> {
        let result = async {
            let next = make_snapshot(Collection::AuthKeys, records, None)?;
            let _gate = self.operation_gate.lock().await;
            let _file_lock = self.repository_file_lock().await?;
            let repo = self.sync_repo_locked().await?;
            if self.load_auth_keys_from_repo(&repo)?.revision != expected_revision {
                return Err(StorageError::Conflict);
            }
            let value = serde_json::json!({"items": next.records.clone()});
            if let Err(error) = self
                .publish_value_locked(
                    &repo,
                    &self.auth_keys_file_path,
                    &value,
                    super::MAX_AUTH_KEYS_BYTES,
                )
                .await
            {
                if !self.validate_pending_marker()?
                    && self.load_auth_keys_from_repo(&repo)?.revision != expected_revision
                {
                    return Err(StorageError::Conflict);
                }
                return Err(error);
            }
            let commit = self.current_head(&repo).await.ok();
            Ok((next, commit))
        }
        .await;
        match result {
            Ok((next, commit)) => {
                match commit {
                    Some(commit) => self.record_sync_success(commit),
                    None => self.record_sync_failure(),
                }
                Ok(next)
            }
            Err(error) => {
                self.record_sync_failure();
                Err(error)
            }
        }
    }

    async fn publish_value_locked(
        &self,
        repo: &Path,
        relative: &str,
        value: &Value,
        limit: u64,
    ) -> Result<(), StorageError> {
        let mut bytes = serde_json::to_vec_pretty(value).map_err(|_| StorageError::Invalid)?;
        bytes.push(b'\n');
        if bytes.len() as u64 > limit {
            return Err(StorageError::Invalid);
        }
        super::atomic_replace_checked_with_limit(&self.pending_path(), b"pending\n", 64, false)
            .map_err(|_| StorageError::Unavailable)?;
        let publish = self
            .write_commit_and_push(repo, relative, &bytes, limit)
            .await;
        match publish {
            Ok(()) => {
                remove_regular_file(&self.pending_path())?;
                Ok(())
            }
            Err(error) => {
                let recovered = self.recover_repo_locked(repo).await;
                if recovered.is_ok() {
                    remove_regular_file(&self.pending_path())?;
                }
                if recovered.is_err() {
                    return Err(StorageError::Unavailable);
                }
                Err(error)
            }
        }
    }

    async fn write_commit_and_push(
        &self,
        repo: &Path,
        relative: &str,
        bytes: &[u8],
        limit: u64,
    ) -> Result<(), StorageError> {
        let path = prepare_git_snapshot_parent(repo, relative)?;
        super::atomic_replace_checked_with_limit(&path, bytes, limit, false)
            .map_err(|_| StorageError::Unavailable)?;
        self.git_expect(repo, &["add", "--", relative]).await?;
        let diff = self
            .git_output(repo, &["diff", "--cached", "--quiet", "--", relative])
            .await?;
        if diff.status.success() {
            return Ok(());
        }
        if diff.status.code() != Some(1) {
            return Err(StorageError::Unavailable);
        }
        self.git_expect(
            repo,
            &["commit", "-m", "Update storage snapshot", "--", relative],
        )
        .await?;
        let refspec = format!("HEAD:refs/heads/{}", self.branch);
        let output = self
            .git_output(repo, &["push", "--porcelain", "origin", &refspec])
            .await?;
        if output.status.success() {
            Ok(())
        } else {
            Err(StorageError::Unavailable)
        }
    }

    async fn load_health_snapshots(
        &self,
    ) -> Result<(StorageSnapshot, StorageSnapshot, Value), StorageError> {
        let (accounts, auth_keys, commit) = self.load_health_snapshots_consistent().await?;
        let health = serde_json::json!({
            "status":"healthy",
            "backend":"git",
            "repo_url":self.redacted_url,
            "branch":self.branch,
            "file_path":self.file_path,
            "auth_keys_file_path":self.auth_keys_file_path,
            "account_count":accounts.records.len(),
            "auth_key_count":auth_keys.records.len(),
            "last_commit":short_git_commit(&commit),
        });
        self.record_sync_success(commit);
        Ok((accounts, auth_keys, health))
    }

    fn info(&self) -> Value {
        serde_json::json!({
            "type":"git",
            "description":"Git 私有仓库存储",
            "repo_url":self.redacted_url,
            "branch":self.branch,
            "file_path":self.file_path,
            "auth_keys_file_path":self.auth_keys_file_path,
        })
    }
}

fn validate_git_relative_path(value: &str) -> Result<String, StorageError> {
    let normalized = value.trim().replace('\\', "/");
    if normalized.is_empty()
        || normalized.len() > 1024
        || normalized.starts_with('/')
        || normalized
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
        || Path::new(value).is_absolute()
        || Path::new(value).components().any(|component| {
            matches!(
                component,
                Component::Prefix(_) | Component::RootDir | Component::ParentDir
            )
        })
    {
        return Err(StorageError::Invalid);
    }
    Ok(normalized)
}

fn relative_path_buf(value: &str) -> Result<PathBuf, StorageError> {
    let normalized = validate_git_relative_path(value)?;
    Ok(normalized.split('/').collect())
}

fn validate_git_branch(value: &str) -> Result<String, StorageError> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 255
        || value.starts_with('-')
        || value.starts_with('.')
        || value.ends_with('.')
        || value.ends_with('/')
        || value.ends_with(".lock")
        || value.contains("..")
        || value.contains("@{")
        || value.bytes().any(|byte| {
            byte.is_ascii_control()
                || matches!(byte, b' ' | b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\')
        })
        || value
            .split('/')
            .any(|part| part.is_empty() || part.starts_with('.') || part.ends_with('.'))
    {
        return Err(StorageError::Invalid);
    }
    Ok(value.to_owned())
}

fn git_execution_url(value: &str) -> Result<(String, Option<String>), StorageError> {
    let Ok(mut parsed) = url::Url::parse(value) else {
        return Ok((value.to_owned(), None));
    };
    if !matches!(parsed.scheme(), "http" | "https") {
        return Ok((value.to_owned(), None));
    }
    let embedded = parsed
        .password()
        .map(ToOwned::to_owned)
        .or_else(|| (!parsed.username().is_empty()).then(|| parsed.username().to_owned()));
    parsed.set_username("").map_err(|_| StorageError::Invalid)?;
    parsed
        .set_password(None)
        .map_err(|_| StorageError::Invalid)?;
    parsed.set_query(None);
    parsed.set_fragment(None);
    Ok((parsed.to_string(), embedded))
}

fn prepare_git_cache(path: &Path) -> Result<PathBuf, StorageError> {
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()
            .map_err(|_| StorageError::Unavailable)?
            .join(path)
    };
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                current.push(component.as_os_str());
            }
            Component::CurDir => continue,
            Component::ParentDir => return Err(StorageError::Invalid),
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(StorageError::Invalid);
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current).map_err(|_| StorageError::Unavailable)?;
                let metadata =
                    fs::symlink_metadata(&current).map_err(|_| StorageError::Unavailable)?;
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(StorageError::Invalid);
                }
            }
            Err(_) => return Err(StorageError::Unavailable),
        }
    }
    portable_canonicalize(&path)
}

fn portable_canonicalize(path: &Path) -> Result<PathBuf, StorageError> {
    let canonical = fs::canonicalize(path).map_err(|_| StorageError::Unavailable)?;
    #[cfg(windows)]
    {
        let text = canonical.to_string_lossy();
        if let Some(path) = text.strip_prefix(r"\\?\UNC\") {
            return Ok(PathBuf::from(format!(r"\\{path}")));
        }
        if let Some(path) = text.strip_prefix(r"\\?\") {
            return Ok(PathBuf::from(path));
        }
    }
    Ok(canonical)
}

fn install_git_askpass(cache_dir: &Path) -> Result<PathBuf, StorageError> {
    #[cfg(windows)]
    let (name, payload): (&str, &[u8]) = (
        ".git-askpass.cmd",
        b"@echo off\r\necho %CHATGPT2API_GIT_TOKEN%\r\n",
    );
    #[cfg(not(windows))]
    let (name, payload): (&str, &[u8]) = (
        ".git-askpass.sh",
        b"#!/bin/sh\ncase \"$1\" in *sername*) printf '%s\\n' \"${GIT_USERNAME}\" ;; *) printf '%s\\n' \"${CHATGPT2API_GIT_TOKEN}\" ;; esac\n",
    );
    let path = cache_dir.join(name);
    super::atomic_replace_checked_with_limit(&path, payload, 4096, false)
        .map_err(|_| StorageError::Unavailable)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .map_err(|_| StorageError::Unavailable)?;
    }
    Ok(path)
}

fn prepare_git_snapshot_parent(repo: &Path, relative: &str) -> Result<PathBuf, StorageError> {
    let relative = relative_path_buf(relative)?;
    let target = repo.join(&relative);
    let parent = target.parent().ok_or(StorageError::Invalid)?;
    let mut current = repo.to_owned();
    if let Some(relative_parent) = relative.parent() {
        for component in relative_parent.components() {
            let Component::Normal(name) = component else {
                return Err(StorageError::Invalid);
            };
            current.push(name);
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                    return Err(StorageError::Invalid);
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    fs::create_dir(&current).map_err(|_| StorageError::Unavailable)?;
                }
                Err(_) => return Err(StorageError::Unavailable),
            }
        }
    }
    if current != parent {
        return Err(StorageError::Invalid);
    }
    Ok(target)
}

fn remove_regular_file(path: &Path) -> Result<(), StorageError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| StorageError::Unavailable)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(StorageError::Invalid);
    }
    fs::remove_file(path).map_err(|_| StorageError::Unavailable)
}

fn remove_git_clone_temp(cache_dir: &Path, clone_path: &Path) -> Result<(), StorageError> {
    if clone_path.parent() != Some(cache_dir)
        || !clone_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(".repo.clone.") && name.ends_with(".tmp"))
    {
        return Err(StorageError::Invalid);
    }
    match fs::symlink_metadata(clone_path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(StorageError::Invalid)
        }
        Ok(_) => fs::remove_dir_all(clone_path).map_err(|_| StorageError::Unavailable),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(StorageError::Unavailable),
    }
}

fn postgres_single_index_column(definition: &str) -> Option<String> {
    let open = definition.rfind('(')?;
    let close = definition[open + 1..].find(')')? + open + 1;
    let column = definition[open + 1..close].trim();
    if column.is_empty() || column.contains(',') || column.contains(char::is_whitespace) {
        return None;
    }
    Some(column.trim_matches('"').to_owned())
}

fn quote_database_identifier(value: &str, kind: DatabaseKind) -> Result<String, StorageError> {
    if value.is_empty() || value.contains('\0') {
        return Err(StorageError::Invalid);
    }
    match kind {
        DatabaseKind::Postgres => Ok(format!("\"{}\"", value.replace('"', "\"\""))),
        DatabaseKind::MySql => Ok(format!("`{}`", value.replace('`', "``"))),
        DatabaseKind::Sqlite => Err(StorageError::Invalid),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DatabaseKind {
    Sqlite,
    Postgres,
    MySql,
}

impl DatabaseKind {
    fn from_url(database_url: &str) -> Result<Self, StorageError> {
        let scheme = database_url
            .split_once(':')
            .map(|(scheme, _)| scheme.to_ascii_lowercase())
            .ok_or(StorageError::Unsupported)?;
        match scheme.as_str() {
            "sqlite" => Ok(Self::Sqlite),
            "postgres" | "postgresql" => Ok(Self::Postgres),
            "mysql" | "mariadb" => Ok(Self::MySql),
            _ => Err(StorageError::Unsupported),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgresql",
            Self::MySql => "mysql",
        }
    }

    fn bind(self, index: usize) -> String {
        match self {
            Self::Postgres => format!("${index}"),
            Self::Sqlite | Self::MySql => "?".to_owned(),
        }
    }
}

pub(super) struct DatabaseStorage {
    pool: AnyPool,
    kind: DatabaseKind,
    redacted_url: String,
}

async fn quiescent_close_database_pool(pool: &AnyPool) {
    // PoolConnection::drop returns a connection on a spawned task. Each task can pass its closed
    // check before a drain and enqueue one late idle connection after that drain's queue pass.
    // The number of such tasks is bounded by pool capacity, so this is a fixed quiescence protocol,
    // not timeout/retry polling. For max_connections=1 it is the required two-pass drain.
    let drain_passes = pool.options().get_max_connections().saturating_add(1);
    for pass in 0..drain_passes {
        if pass != 0 {
            tokio::task::yield_now().await;
        }
        pool.close().await;
    }
}

impl DatabaseStorage {
    async fn connect(database_url: &str) -> Result<Self, StorageError> {
        let database_url = database_url.trim();
        if database_url.is_empty() {
            return Err(StorageError::Invalid);
        }
        let kind = DatabaseKind::from_url(database_url)?;
        install_default_drivers();
        let connect_url = database_connect_url(database_url, kind);
        let max_connections = if connect_url.contains(":memory:") {
            1
        } else {
            5
        };
        let pool = AnyPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&connect_url)
            .await
            .map_err(|_| StorageError::Unavailable)?;
        let backend = Self {
            pool,
            kind,
            redacted_url: redact_url(database_url),
        };
        if let Err(error) = backend.initialize_schema().await {
            quiescent_close_database_pool(&backend.pool).await;
            return Err(error);
        }
        Ok(backend)
    }

    async fn initialize_schema(&self) -> Result<(), StorageError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| StorageError::Unavailable)?;
        if self.kind == DatabaseKind::Sqlite {
            connection
                .execute("BEGIN IMMEDIATE")
                .await
                .map_err(|_| StorageError::Unavailable)?;
            let result = self.initialize_schema_on(&mut connection).await;
            let terminal = if result.is_ok() { "COMMIT" } else { "ROLLBACK" };
            if connection.execute(terminal).await.is_err() && result.is_ok() {
                return Err(StorageError::Unavailable);
            }
            return result;
        }
        connection.close_on_drop();
        match self.kind {
            DatabaseKind::Postgres => self.initialize_postgres_schema(&mut connection).await,
            DatabaseKind::MySql => self.initialize_mysql_schema(&mut connection).await,
            DatabaseKind::Sqlite => unreachable!(),
        }
    }

    async fn initialize_postgres_schema(
        &self,
        connection: &mut PoolConnection<Any>,
    ) -> Result<(), StorageError> {
        if connection.execute("BEGIN").await.is_err() {
            return Err(StorageError::Unavailable);
        }
        let result = async {
            let database_name = server_database_name(connection, DatabaseKind::Postgres).await?;
            let lock_key = postgres_schema_lock_key(&database_name);
            let deadline = tokio::time::Instant::now() + SERVER_SCHEMA_LOCK_WAIT;
            loop {
                let acquired = tokio::time::timeout_at(
                    deadline,
                    sqlx::query("SELECT pg_try_advisory_xact_lock($1) AS acquired")
                        .bind(lock_key)
                        .fetch_one(&mut **connection),
                )
                .await
                .map_err(|_| StorageError::Unavailable)?
                .map_err(|_| StorageError::Unavailable)?
                .try_get::<bool, _>("acquired")
                .map_err(|_| StorageError::Unavailable)?;
                if acquired {
                    break;
                }
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    return Err(StorageError::Unavailable);
                }
                tokio::time::sleep((deadline - now).min(SERVER_SCHEMA_LOCK_POLL)).await;
            }
            self.initialize_schema_on(connection).await
        }
        .await;
        let terminal = if result.is_ok() { "COMMIT" } else { "ROLLBACK" };
        if connection.execute(terminal).await.is_err() {
            return Err(StorageError::Unavailable);
        }
        result
    }

    async fn initialize_mysql_schema(
        &self,
        connection: &mut PoolConnection<Any>,
    ) -> Result<(), StorageError> {
        let database_name = server_database_name(connection, DatabaseKind::MySql).await?;
        let lock_name = mysql_schema_lock_name(&database_name);
        let acquired = tokio::time::timeout(
            SERVER_SCHEMA_LOCK_WAIT,
            sqlx::query("SELECT GET_LOCK(?, 10) AS acquired")
                .bind(&lock_name)
                .fetch_one(&mut **connection),
        )
        .await
        .map_err(|_| StorageError::Unavailable)?
        .map_err(|_| StorageError::Unavailable)?
        .try_get::<Option<i64>, _>("acquired")
        .map_err(|_| StorageError::Unavailable)?;
        if acquired != Some(1) {
            return Err(StorageError::Unavailable);
        }
        let result = self.initialize_schema_on(connection).await;
        let released = tokio::time::timeout(
            SERVER_SCHEMA_LOCK_WAIT,
            sqlx::query("SELECT RELEASE_LOCK(?) AS released")
                .bind(&lock_name)
                .fetch_one(&mut **connection),
        )
        .await
        .map_err(|_| StorageError::Unavailable)?
        .map_err(|_| StorageError::Unavailable)?
        .try_get::<Option<i64>, _>("released")
        .map_err(|_| StorageError::Unavailable)?;
        if released != Some(1) {
            return Err(StorageError::Unavailable);
        }
        result
    }

    async fn initialize_schema_on(
        &self,
        connection: &mut AnyConnection,
    ) -> Result<(), StorageError> {
        let statements: &[&str] = match self.kind {
            DatabaseKind::Sqlite => &[
                "CREATE TABLE IF NOT EXISTS accounts (id INTEGER PRIMARY KEY AUTOINCREMENT, access_token TEXT NOT NULL, access_token_hash CHAR(64) NOT NULL, data TEXT NOT NULL)",
            ],
            DatabaseKind::Postgres => &[
                "CREATE TABLE IF NOT EXISTS accounts (id BIGSERIAL PRIMARY KEY, access_token TEXT NOT NULL, access_token_hash VARCHAR(64) NOT NULL, data TEXT NOT NULL)",
            ],
            DatabaseKind::MySql => &[
                "CREATE TABLE IF NOT EXISTS accounts (id BIGINT PRIMARY KEY AUTO_INCREMENT, access_token LONGTEXT NOT NULL, access_token_hash CHAR(64) NOT NULL, data LONGTEXT NOT NULL)",
            ],
        };
        for statement in statements {
            connection
                .execute(*statement)
                .await
                .map_err(|_| StorageError::Unavailable)?;
        }
        if self.kind == DatabaseKind::Sqlite {
            self.migrate_sqlite_account_schema(connection).await?;
        } else {
            self.migrate_server_account_schema(connection).await?;
        }
        let common: &[&str] = match self.kind {
            DatabaseKind::Sqlite => &[
                "CREATE TABLE IF NOT EXISTS auth_keys (id INTEGER PRIMARY KEY AUTOINCREMENT, key_id VARCHAR(255) NOT NULL UNIQUE, data TEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS storage_mutation_locks (scope VARCHAR(64) PRIMARY KEY)",
                "CREATE TABLE IF NOT EXISTS storage_metadata (key VARCHAR(64) PRIMARY KEY, int_value BIGINT NOT NULL)",
            ],
            DatabaseKind::Postgres => &[
                "CREATE TABLE IF NOT EXISTS auth_keys (id BIGSERIAL PRIMARY KEY, key_id VARCHAR(255) NOT NULL UNIQUE, data TEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS storage_mutation_locks (scope VARCHAR(64) PRIMARY KEY)",
                "CREATE TABLE IF NOT EXISTS storage_metadata (key VARCHAR(64) PRIMARY KEY, int_value BIGINT NOT NULL)",
            ],
            DatabaseKind::MySql => &[
                "CREATE TABLE IF NOT EXISTS auth_keys (id BIGINT PRIMARY KEY AUTO_INCREMENT, key_id VARCHAR(255) NOT NULL UNIQUE, data LONGTEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS storage_mutation_locks (scope VARCHAR(64) PRIMARY KEY)",
                "CREATE TABLE IF NOT EXISTS storage_metadata (`key` VARCHAR(64) PRIMARY KEY, int_value BIGINT NOT NULL)",
            ],
        };
        for statement in common {
            connection
                .execute(*statement)
                .await
                .map_err(|_| StorageError::Unavailable)?;
        }
        for scope in ["accounts", "auth_keys"] {
            let statement = match self.kind {
                DatabaseKind::MySql => {
                    format!("INSERT IGNORE INTO storage_mutation_locks(scope) VALUES ('{scope}')")
                }
                DatabaseKind::Sqlite | DatabaseKind::Postgres => format!(
                    "INSERT INTO storage_mutation_locks(scope) VALUES ('{scope}') ON CONFLICT(scope) DO NOTHING"
                ),
            };
            connection
                .execute(statement.as_str())
                .await
                .map_err(|_| StorageError::Unavailable)?;
        }
        self.load_accounts_from(connection).await?;
        self.load_auth_keys_from(connection).await?;
        Ok(())
    }

    async fn migrate_sqlite_account_schema(
        &self,
        connection: &mut AnyConnection,
    ) -> Result<(), StorageError> {
        let columns = sqlx::query("PRAGMA table_info(accounts)")
            .fetch_all(&mut *connection)
            .await
            .map_err(|_| StorageError::Unavailable)?;
        let mut column_types = HashMap::with_capacity(columns.len());
        let mut column_not_null = HashMap::with_capacity(columns.len());
        for column in columns {
            let name = column
                .try_get::<String, _>("name")
                .map_err(|_| StorageError::Invalid)?;
            let kind = column
                .try_get::<String, _>("type")
                .map_err(|_| StorageError::Invalid)?;
            let not_null = column
                .try_get::<i64, _>("notnull")
                .map_err(|_| StorageError::Invalid)?;
            column_types.insert(name.clone(), kind);
            column_not_null.insert(name, not_null == 1);
        }
        if !column_types.contains_key("id")
            || !column_types.contains_key("access_token")
            || !column_types.contains_key("data")
        {
            return Err(StorageError::Invalid);
        }
        let has_hash = column_types.contains_key("access_token_hash");
        let indexes = sqlx::query("PRAGMA index_list(accounts)")
            .fetch_all(&mut *connection)
            .await
            .map_err(|_| StorageError::Unavailable)?;
        let mut has_unique_hash = false;
        let mut has_unique_raw_token = false;
        for index in indexes {
            if index
                .try_get::<i64, _>("unique")
                .map_err(|_| StorageError::Invalid)?
                != 1
            {
                continue;
            }
            let name = index
                .try_get::<String, _>("name")
                .map_err(|_| StorageError::Invalid)?;
            let quoted_name = name.replace('"', "\"\"");
            let statement = format!("PRAGMA index_info(\"{quoted_name}\")");
            let indexed_columns = sqlx::query(&statement)
                .fetch_all(&mut *connection)
                .await
                .map_err(|_| StorageError::Unavailable)?
                .into_iter()
                .map(|row| {
                    row.try_get::<String, _>("name")
                        .map_err(|_| StorageError::Invalid)
                })
                .collect::<Result<Vec<_>, _>>()?;
            has_unique_hash |= indexed_columns == ["access_token_hash"];
            has_unique_raw_token |= indexed_columns == ["access_token"];
        }

        let rows = if has_hash {
            sqlx::query(
                "SELECT id, access_token, access_token_hash, data FROM accounts ORDER BY id",
            )
            .fetch_all(&mut *connection)
            .await
            .map_err(|_| StorageError::Unavailable)?
        } else {
            sqlx::query("SELECT id, access_token, data FROM accounts ORDER BY id")
                .fetch_all(&mut *connection)
                .await
                .map_err(|_| StorageError::Unavailable)?
        };
        let mut migrated_rows = Vec::with_capacity(rows.len());
        let mut records = Vec::with_capacity(rows.len());
        let mut seen_tokens = HashSet::with_capacity(rows.len());
        let mut needs_token_normalization = false;
        for row in rows {
            let id = row
                .try_get::<i64, _>("id")
                .map_err(|_| StorageError::Invalid)?;
            let raw_token = row
                .try_get::<String, _>("access_token")
                .map_err(|_| StorageError::Invalid)?;
            let token = raw_token.trim().to_owned();
            if token.is_empty() || !seen_tokens.insert(token.clone()) {
                return Err(StorageError::Invalid);
            }
            let data = row
                .try_get::<String, _>("data")
                .map_err(|_| StorageError::Invalid)?;
            let value: Value = serde_json::from_str(&data).map_err(|_| StorageError::Invalid)?;
            if record_key(&value, "access_token")? != token {
                return Err(StorageError::Invalid);
            }
            let expected_hash = token_hash(&token);
            if has_hash {
                let stored_hash = row
                    .try_get::<Option<String>, _>("access_token_hash")
                    .map_err(|_| StorageError::Invalid)?;
                if stored_hash
                    .as_deref()
                    .is_some_and(|hash| hash != expected_hash)
                {
                    return Err(StorageError::Invalid);
                }
            }
            needs_token_normalization |= raw_token != token;
            records.push(value);
            migrated_rows.push((id, token, expected_hash, data));
        }
        make_snapshot(Collection::Accounts, records, None)?;

        let needs_rebuild = !has_hash
            || !column_not_null
                .get("access_token_hash")
                .copied()
                .unwrap_or(false)
            || column_types
                .get("access_token")
                .map(|kind| kind.trim().to_ascii_uppercase())
                .as_deref()
                != Some("TEXT")
            || has_unique_raw_token
            || needs_token_normalization;
        if needs_rebuild {
            connection
                .execute("DROP TABLE IF EXISTS accounts__chatgpt2api_token_migration")
                .await
                .map_err(|_| StorageError::Unavailable)?;
            connection
                .execute(
                    "CREATE TABLE accounts__chatgpt2api_token_migration (id INTEGER PRIMARY KEY, access_token TEXT NOT NULL, access_token_hash CHAR(64) NOT NULL, data TEXT NOT NULL)",
                )
                .await
                .map_err(|_| StorageError::Unavailable)?;
            for (id, token, hash, data) in migrated_rows {
                sqlx::query(
                    "INSERT INTO accounts__chatgpt2api_token_migration(id, access_token, access_token_hash, data) VALUES (?, ?, ?, ?)",
                )
                .bind(id)
                .bind(token)
                .bind(hash)
                .bind(data)
                .execute(&mut *connection)
                .await
                .map_err(|_| StorageError::Unavailable)?;
            }
            connection
                .execute("DROP TABLE accounts")
                .await
                .map_err(|_| StorageError::Unavailable)?;
            connection
                .execute("ALTER TABLE accounts__chatgpt2api_token_migration RENAME TO accounts")
                .await
                .map_err(|_| StorageError::Unavailable)?;
        }
        if needs_rebuild || !has_unique_hash {
            connection
                .execute(
                    "CREATE UNIQUE INDEX ux_accounts_access_token_hash ON accounts(access_token_hash)",
                )
                .await
                .map_err(|_| StorageError::Unavailable)?;
        }
        Ok(())
    }

    async fn migrate_server_account_schema(
        &self,
        connection: &mut AnyConnection,
    ) -> Result<(), StorageError> {
        let column_statement = match self.kind {
            DatabaseKind::Postgres => {
                "SELECT column_name::text AS column_name, data_type::text AS data_type, is_nullable::text AS is_nullable FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'accounts'"
            }
            DatabaseKind::MySql => {
                "SELECT CAST(column_name AS CHAR(64)) AS column_name, CAST(data_type AS CHAR(64)) AS data_type, CAST(is_nullable AS CHAR(3)) AS is_nullable FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'accounts'"
            }
            DatabaseKind::Sqlite => return Err(StorageError::Invalid),
        };
        let columns = sqlx::query(column_statement)
            .fetch_all(&mut *connection)
            .await
            .map_err(|_| StorageError::Unavailable)?;
        let mut column_types = HashMap::with_capacity(columns.len());
        let mut column_not_null = HashMap::with_capacity(columns.len());
        for column in columns {
            let name = column
                .try_get::<String, _>("column_name")
                .map_err(|_| invalid_database_stage("server.columns.column_name"))?;
            let kind = column
                .try_get::<String, _>("data_type")
                .map_err(|_| invalid_database_stage("server.columns.data_type"))?;
            let nullable = column
                .try_get::<String, _>("is_nullable")
                .map_err(|_| invalid_database_stage("server.columns.is_nullable"))?;
            column_types.insert(name.clone(), kind);
            column_not_null.insert(name, nullable.eq_ignore_ascii_case("NO"));
        }
        if !column_types.contains_key("id")
            || !column_types.contains_key("access_token")
            || !column_types.contains_key("data")
        {
            return Err(invalid_database_stage("server.columns.required"));
        }
        let has_hash = column_types.contains_key("access_token_hash");
        let mut unique_indexes = HashMap::<String, Vec<String>>::new();
        let mut raw_constraints = HashSet::<String>::new();
        match self.kind {
            DatabaseKind::Postgres => {
                let rows = sqlx::query(
                    "SELECT indexname::text AS indexname, indexdef::text AS indexdef FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'accounts'",
                )
                .fetch_all(&mut *connection)
                .await
                .map_err(|_| StorageError::Unavailable)?;
                for row in rows {
                    let name = row
                        .try_get::<String, _>("indexname")
                        .map_err(|_| StorageError::Invalid)?;
                    let definition = row
                        .try_get::<String, _>("indexdef")
                        .map_err(|_| StorageError::Invalid)?;
                    if !definition
                        .to_ascii_uppercase()
                        .contains("CREATE UNIQUE INDEX")
                    {
                        continue;
                    }
                    if let Some(column) = postgres_single_index_column(&definition) {
                        unique_indexes.insert(name, vec![column]);
                    }
                }
                let constraints = sqlx::query(
                    "SELECT tc.constraint_name::text AS constraint_name, kcu.column_name::text AS column_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_schema = kcu.constraint_schema AND tc.constraint_name = kcu.constraint_name AND tc.table_name = kcu.table_name WHERE tc.table_schema = current_schema() AND tc.table_name = 'accounts' AND tc.constraint_type = 'UNIQUE' ORDER BY tc.constraint_name, kcu.ordinal_position",
                )
                .fetch_all(&mut *connection)
                .await
                .map_err(|_| StorageError::Unavailable)?;
                let mut grouped = HashMap::<String, Vec<String>>::new();
                for row in constraints {
                    grouped
                        .entry(
                            row.try_get::<String, _>("constraint_name")
                                .map_err(|_| StorageError::Invalid)?,
                        )
                        .or_default()
                        .push(
                            row.try_get::<String, _>("column_name")
                                .map_err(|_| StorageError::Invalid)?,
                        );
                }
                for (name, columns) in grouped {
                    if columns == ["access_token"] {
                        raw_constraints.insert(name);
                    }
                }
            }
            DatabaseKind::MySql => {
                let rows = sqlx::query(
                    "SELECT CAST(index_name AS CHAR(64)) AS index_name, CAST(column_name AS CHAR(64)) AS column_name, CAST(non_unique AS SIGNED) AS non_unique, CAST(seq_in_index AS SIGNED) AS seq_in_index FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'accounts' ORDER BY index_name, seq_in_index",
                )
                .fetch_all(&mut *connection)
                .await
                .map_err(|_| StorageError::Unavailable)?;
                let mut grouped = HashMap::<String, Vec<(i64, String)>>::new();
                for row in rows {
                    if row
                        .try_get::<i64, _>("non_unique")
                        .map_err(|_| invalid_database_stage("mysql.indexes.non_unique"))?
                        != 0
                    {
                        continue;
                    }
                    let name = row
                        .try_get::<String, _>("index_name")
                        .map_err(|_| invalid_database_stage("mysql.indexes.index_name"))?;
                    if name.eq_ignore_ascii_case("PRIMARY") {
                        continue;
                    }
                    grouped.entry(name).or_default().push((
                        row.try_get::<i64, _>("seq_in_index")
                            .map_err(|_| invalid_database_stage("mysql.indexes.seq_in_index"))?,
                        row.try_get::<String, _>("column_name")
                            .map_err(|_| invalid_database_stage("mysql.indexes.column_name"))?,
                    ));
                }
                for (name, mut columns) in grouped {
                    columns.sort_by_key(|(sequence, _)| *sequence);
                    unique_indexes.insert(
                        name,
                        columns.into_iter().map(|(_, column)| column).collect(),
                    );
                }
            }
            DatabaseKind::Sqlite => unreachable!(),
        }
        let has_unique_hash = unique_indexes
            .values()
            .any(|columns| columns.len() == 1 && columns[0] == "access_token_hash");
        let raw_indexes = unique_indexes
            .iter()
            .filter(|(_, columns)| columns.len() == 1 && columns[0] == "access_token")
            .map(|(name, _)| name.clone())
            .filter(|name| !raw_constraints.contains(name))
            .collect::<Vec<_>>();

        let rows = if has_hash {
            let statement = match self.kind {
                DatabaseKind::Postgres => {
                    "SELECT id, access_token, access_token_hash::text AS access_token_hash, data FROM accounts ORDER BY id"
                }
                DatabaseKind::MySql => {
                    "SELECT id, access_token, access_token_hash, data FROM accounts ORDER BY id"
                }
                DatabaseKind::Sqlite => unreachable!(),
            };
            sqlx::query(statement)
                .fetch_all(&mut *connection)
                .await
                .map_err(|_| StorageError::Unavailable)?
        } else {
            sqlx::query("SELECT id, access_token, data FROM accounts ORDER BY id")
                .fetch_all(&mut *connection)
                .await
                .map_err(|_| StorageError::Unavailable)?
        };
        let mut migrated_rows = Vec::with_capacity(rows.len());
        let mut records = Vec::with_capacity(rows.len());
        let mut seen_tokens = HashSet::with_capacity(rows.len());
        let mut needs_token_normalization = false;
        for row in rows {
            let id = row
                .try_get::<i64, _>("id")
                .map_err(|_| invalid_database_stage("server.accounts.id"))?;
            let raw_token = database_row_text(
                &row,
                "access_token",
                self.kind,
                "server.accounts.access_token",
            )?;
            let token = raw_token.trim().to_owned();
            if token.is_empty() || !seen_tokens.insert(token.clone()) {
                return Err(StorageError::Invalid);
            }
            let data = database_row_text(&row, "data", self.kind, "server.accounts.data")?;
            let value: Value = serde_json::from_str(&data)
                .map_err(|_| invalid_database_stage("server.accounts.data_json"))?;
            if record_key(&value, "access_token")? != token {
                return Err(StorageError::Invalid);
            }
            let expected_hash = token_hash(&token);
            if has_hash {
                let stored_hash = row
                    .try_get::<Option<String>, _>("access_token_hash")
                    .map_err(|_| invalid_database_stage("server.accounts.access_token_hash"))?;
                if stored_hash
                    .as_deref()
                    .is_some_and(|hash| hash != expected_hash)
                {
                    return Err(StorageError::Invalid);
                }
            }
            needs_token_normalization |= raw_token != token;
            records.push(value);
            migrated_rows.push((id, token, expected_hash, data));
        }
        make_snapshot(Collection::Accounts, records, None)
            .map_err(|_| invalid_database_stage("server.accounts.snapshot"))?;
        let token_type_is_unbounded =
            column_types
                .get("access_token")
                .is_some_and(|kind| match self.kind {
                    DatabaseKind::Postgres => kind.eq_ignore_ascii_case("text"),
                    DatabaseKind::MySql => {
                        matches!(
                            kind.to_ascii_lowercase().as_str(),
                            "text" | "mediumtext" | "longtext"
                        )
                    }
                    DatabaseKind::Sqlite => false,
                });
        let needs_migration = !has_hash
            || !column_not_null
                .get("access_token_hash")
                .copied()
                .unwrap_or(false)
            || !token_type_is_unbounded
            || !has_unique_hash
            || !raw_indexes.is_empty()
            || !raw_constraints.is_empty()
            || needs_token_normalization;
        if !needs_migration {
            if self.kind == DatabaseKind::MySql {
                connection
                    .execute("DROP TABLE IF EXISTS accounts__chatgpt2api_token_backup")
                    .await
                    .map_err(|_| StorageError::Unavailable)?;
                connection
                    .execute("DROP TABLE IF EXISTS accounts__chatgpt2api_token_migration")
                    .await
                    .map_err(|_| StorageError::Unavailable)?;
            }
            return Ok(());
        }
        match self.kind {
            DatabaseKind::Postgres => {
                if !has_hash {
                    connection
                        .execute("ALTER TABLE accounts ADD COLUMN access_token_hash VARCHAR(64)")
                        .await
                        .map_err(|_| StorageError::Unavailable)?;
                }
                for (id, token, hash, _) in migrated_rows {
                    sqlx::query(
                        "UPDATE accounts SET access_token = $1, access_token_hash = $2 WHERE id = $3",
                    )
                    .bind(token)
                    .bind(hash)
                    .bind(id)
                    .execute(&mut *connection)
                    .await
                    .map_err(|_| StorageError::Unavailable)?;
                }
                if !has_unique_hash {
                    connection
                        .execute("CREATE UNIQUE INDEX ux_accounts_access_token_hash ON accounts(access_token_hash)")
                        .await
                        .map_err(|_| StorageError::Unavailable)?;
                }
                if !column_not_null
                    .get("access_token_hash")
                    .copied()
                    .unwrap_or(false)
                {
                    connection
                        .execute("ALTER TABLE accounts ALTER COLUMN access_token_hash SET NOT NULL")
                        .await
                        .map_err(|_| StorageError::Unavailable)?;
                }
                if !token_type_is_unbounded {
                    connection
                        .execute("ALTER TABLE accounts ALTER COLUMN access_token TYPE TEXT")
                        .await
                        .map_err(|_| StorageError::Unavailable)?;
                }
                for constraint in raw_constraints {
                    let statement = format!(
                        "ALTER TABLE accounts DROP CONSTRAINT {}",
                        quote_database_identifier(&constraint, self.kind)?
                    );
                    connection
                        .execute(statement.as_str())
                        .await
                        .map_err(|_| StorageError::Unavailable)?;
                }
                for index in raw_indexes {
                    let statement = format!(
                        "DROP INDEX {}",
                        quote_database_identifier(&index, self.kind)?
                    );
                    connection
                        .execute(statement.as_str())
                        .await
                        .map_err(|_| StorageError::Unavailable)?;
                }
            }
            DatabaseKind::MySql => {
                connection
                    .execute("DROP TABLE IF EXISTS accounts__chatgpt2api_token_migration")
                    .await
                    .map_err(|_| StorageError::Unavailable)?;
                connection
                    .execute("DROP TABLE IF EXISTS accounts__chatgpt2api_token_backup")
                    .await
                    .map_err(|_| StorageError::Unavailable)?;
                connection
                    .execute(
                        "CREATE TABLE accounts__chatgpt2api_token_migration (id BIGINT PRIMARY KEY AUTO_INCREMENT, access_token LONGTEXT NOT NULL, access_token_hash CHAR(64) NOT NULL, data LONGTEXT NOT NULL, UNIQUE KEY ux_accounts_access_token_hash(access_token_hash))",
                    )
                    .await
                    .map_err(|_| StorageError::Unavailable)?;
                for (id, token, hash, data) in migrated_rows {
                    sqlx::query(
                        "INSERT INTO accounts__chatgpt2api_token_migration(id, access_token, access_token_hash, data) VALUES (?, ?, ?, ?)",
                    )
                    .bind(id)
                    .bind(token)
                    .bind(hash)
                    .bind(data)
                    .execute(&mut *connection)
                    .await
                    .map_err(|_| StorageError::Unavailable)?;
                }
                connection
                    .execute(
                        "RENAME TABLE accounts TO accounts__chatgpt2api_token_backup, accounts__chatgpt2api_token_migration TO accounts",
                    )
                    .await
                    .map_err(|_| StorageError::Unavailable)?;
                connection
                    .execute("DROP TABLE accounts__chatgpt2api_token_backup")
                    .await
                    .map_err(|_| StorageError::Unavailable)?;
            }
            DatabaseKind::Sqlite => unreachable!(),
        }
        Ok(())
    }

    async fn load_accounts(&self) -> Result<StorageSnapshot, StorageError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| StorageError::Unavailable)?;
        self.load_accounts_from(&mut connection).await
    }

    async fn load_accounts_from(
        &self,
        connection: &mut AnyConnection,
    ) -> Result<StorageSnapshot, StorageError> {
        let statement = match self.kind {
            DatabaseKind::Postgres => {
                "SELECT access_token, access_token_hash::text AS access_token_hash, data FROM accounts ORDER BY id"
            }
            DatabaseKind::Sqlite | DatabaseKind::MySql => {
                "SELECT access_token, access_token_hash, data FROM accounts ORDER BY id"
            }
        };
        let rows = sqlx::query(statement)
            .fetch_all(&mut *connection)
            .await
            .map_err(|_| StorageError::Unavailable)?;
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let row_token =
                database_row_text(&row, "access_token", self.kind, "accounts.access_token")?;
            let row_hash = row
                .try_get::<String, _>("access_token_hash")
                .map_err(|_| StorageError::Invalid)?;
            let data = database_row_text(&row, "data", self.kind, "accounts.data")?;
            let value: Value = serde_json::from_str(&data).map_err(|_| StorageError::Invalid)?;
            let token = record_key(&value, "access_token")?;
            if token != row_token || token_hash(&token) != row_hash {
                return Err(StorageError::Invalid);
            }
            records.push(value);
        }
        let cumulative_total = self.load_cumulative_from(connection).await?;
        make_snapshot(Collection::Accounts, records, cumulative_total)
    }

    async fn load_auth_keys(&self) -> Result<StorageSnapshot, StorageError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| StorageError::Unavailable)?;
        self.load_auth_keys_from(&mut connection).await
    }

    async fn load_auth_keys_from(
        &self,
        connection: &mut AnyConnection,
    ) -> Result<StorageSnapshot, StorageError> {
        let rows = sqlx::query("SELECT key_id, data FROM auth_keys ORDER BY id")
            .fetch_all(&mut *connection)
            .await
            .map_err(|_| StorageError::Unavailable)?;
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let row_key = row
                .try_get::<String, _>("key_id")
                .map_err(|_| StorageError::Invalid)?;
            let data = database_row_text(&row, "data", self.kind, "auth_keys.data")?;
            let value: Value = serde_json::from_str(&data).map_err(|_| StorageError::Invalid)?;
            if record_key(&value, "id")? != row_key {
                return Err(StorageError::Invalid);
            }
            records.push(value);
        }
        make_snapshot(Collection::AuthKeys, records, None)
    }

    async fn load_cumulative_from(
        &self,
        connection: &mut AnyConnection,
    ) -> Result<Option<u64>, StorageError> {
        let key = if self.kind == DatabaseKind::MySql {
            "`key`"
        } else {
            "key"
        };
        let statement =
            format!("SELECT int_value FROM storage_metadata WHERE {key} = 'cumulative_total'");
        let row = sqlx::query(&statement)
            .fetch_optional(&mut *connection)
            .await
            .map_err(|_| StorageError::Unavailable)?;
        row.map(|row| {
            row.try_get::<i64, _>("int_value")
                .map_err(|_| StorageError::Invalid)
                .and_then(|value| u64::try_from(value).map_err(|_| StorageError::Invalid))
        })
        .transpose()
    }

    async fn save_accounts(
        &self,
        expected_revision: [u8; 32],
        records: Vec<Value>,
        cumulative_total: u64,
    ) -> Result<StorageSnapshot, StorageError> {
        let next = make_snapshot(Collection::Accounts, records, Some(cumulative_total))?;
        let mut transaction = self.begin_mutation("accounts").await?;
        let current = self.load_accounts_from(&mut transaction).await?;
        if current.revision != expected_revision
            || current
                .cumulative_total
                .is_some_and(|current| current > cumulative_total)
        {
            transaction.rollback().await.ok();
            return Err(StorageError::Conflict);
        }
        self.replace_rows(&mut transaction, Collection::Accounts, &next.records)
            .await?;
        self.store_cumulative(&mut transaction, cumulative_total)
            .await?;
        transaction
            .commit()
            .await
            .map_err(|_| StorageError::Unavailable)?;
        Ok(next)
    }

    async fn save_auth_keys(
        &self,
        expected_revision: [u8; 32],
        records: Vec<Value>,
    ) -> Result<StorageSnapshot, StorageError> {
        let next = make_snapshot(Collection::AuthKeys, records, None)?;
        let mut transaction = self.begin_mutation("auth_keys").await?;
        let current = self.load_auth_keys_from(&mut transaction).await?;
        if current.revision != expected_revision {
            transaction.rollback().await.ok();
            return Err(StorageError::Conflict);
        }
        self.replace_rows(&mut transaction, Collection::AuthKeys, &next.records)
            .await?;
        transaction
            .commit()
            .await
            .map_err(|_| StorageError::Unavailable)?;
        Ok(next)
    }

    async fn begin_mutation(&self, scope: &str) -> Result<Transaction<'_, Any>, StorageError> {
        if !matches!(scope, "accounts" | "auth_keys") {
            return Err(StorageError::Invalid);
        }
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| StorageError::Unavailable)?;
        let statement =
            format!("UPDATE storage_mutation_locks SET scope = scope WHERE scope = '{scope}'");
        let result = transaction
            .execute(statement.as_str())
            .await
            .map_err(|_| StorageError::Unavailable)?;
        if result.rows_affected() != 1 {
            transaction.rollback().await.ok();
            return Err(StorageError::Unavailable);
        }
        Ok(transaction)
    }

    async fn replace_rows(
        &self,
        transaction: &mut Transaction<'_, Any>,
        collection: Collection,
        records: &[Value],
    ) -> Result<(), StorageError> {
        let (table, key_column, record_key_name) = match collection {
            Collection::Accounts => ("accounts", "access_token", "access_token"),
            Collection::AuthKeys => ("auth_keys", "key_id", "id"),
        };
        let statement = format!("SELECT id, {key_column}, data FROM {table} ORDER BY id");
        let rows = sqlx::query(&statement)
            .fetch_all(&mut **transaction)
            .await
            .map_err(|_| StorageError::Unavailable)?;
        let mut existing = HashMap::with_capacity(rows.len());
        for row in rows {
            let id = row
                .try_get::<i64, _>("id")
                .map_err(|_| StorageError::Invalid)?;
            let key = database_row_text(&row, key_column, self.kind, "replace_rows.key")?;
            let data = database_row_text(&row, "data", self.kind, "replace_rows.data")?;
            if existing.insert(key, (id, data)).is_some() {
                return Err(StorageError::Invalid);
            }
        }
        let mut incoming = HashSet::with_capacity(records.len());
        for record in records {
            let key = record_key(record, record_key_name)?;
            if !incoming.insert(key.clone()) {
                return Err(StorageError::Invalid);
            }
            let data = serde_json::to_string(record).map_err(|_| StorageError::Invalid)?;
            if let Some((id, current_data)) = existing.get(&key) {
                if current_data != &data {
                    let statement = format!(
                        "UPDATE {table} SET data = {} WHERE id = {}",
                        self.kind.bind(1),
                        self.kind.bind(2)
                    );
                    sqlx::query(&statement)
                        .bind(data)
                        .bind(*id)
                        .execute(&mut **transaction)
                        .await
                        .map_err(|_| StorageError::Unavailable)?;
                }
                continue;
            }
            match collection {
                Collection::Accounts => {
                    let statement = format!(
                        "INSERT INTO accounts(access_token, access_token_hash, data) VALUES ({}, {}, {})",
                        self.kind.bind(1),
                        self.kind.bind(2),
                        self.kind.bind(3)
                    );
                    sqlx::query(&statement)
                        .bind(&key)
                        .bind(token_hash(&key))
                        .bind(data)
                        .execute(&mut **transaction)
                        .await
                        .map_err(|_| StorageError::Unavailable)?;
                }
                Collection::AuthKeys => {
                    let statement = format!(
                        "INSERT INTO auth_keys(key_id, data) VALUES ({}, {})",
                        self.kind.bind(1),
                        self.kind.bind(2)
                    );
                    sqlx::query(&statement)
                        .bind(&key)
                        .bind(data)
                        .execute(&mut **transaction)
                        .await
                        .map_err(|_| StorageError::Unavailable)?;
                }
            }
        }
        for (key, (id, _)) in existing {
            if incoming.contains(&key) {
                continue;
            }
            let statement = format!("DELETE FROM {table} WHERE id = {}", self.kind.bind(1));
            sqlx::query(&statement)
                .bind(id)
                .execute(&mut **transaction)
                .await
                .map_err(|_| StorageError::Unavailable)?;
        }
        Ok(())
    }

    async fn store_cumulative(
        &self,
        transaction: &mut Transaction<'_, Any>,
        cumulative_total: u64,
    ) -> Result<(), StorageError> {
        let cumulative_total =
            i64::try_from(cumulative_total).map_err(|_| StorageError::Invalid)?;
        let key = if self.kind == DatabaseKind::MySql {
            "`key`"
        } else {
            "key"
        };
        let update = format!(
            "UPDATE storage_metadata SET int_value = {} WHERE {key} = 'cumulative_total'",
            self.kind.bind(1)
        );
        let result = sqlx::query(&update)
            .bind(cumulative_total)
            .execute(&mut **transaction)
            .await
            .map_err(|_| StorageError::Unavailable)?;
        if result.rows_affected() == 0 {
            let insert = format!(
                "INSERT INTO storage_metadata({key}, int_value) VALUES ('cumulative_total', {})",
                self.kind.bind(1)
            );
            sqlx::query(&insert)
                .bind(cumulative_total)
                .execute(&mut **transaction)
                .await
                .map_err(|_| StorageError::Unavailable)?;
        }
        Ok(())
    }

    async fn load_health_snapshots(
        &self,
    ) -> Result<(StorageSnapshot, StorageSnapshot, Value), StorageError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| StorageError::Unavailable)?;
        connection
            .execute("SELECT 1")
            .await
            .map_err(|_| StorageError::Unavailable)?;
        let accounts = self.load_accounts_from(&mut connection).await?;
        let auth_keys = self.load_auth_keys_from(&mut connection).await?;
        let health = serde_json::json!({
            "status": "healthy",
            "backend": "database",
            "database_url": self.redacted_url,
            "account_count": accounts.records.len(),
            "auth_key_count": auth_keys.records.len(),
        });
        Ok((accounts, auth_keys, health))
    }

    fn info(&self) -> Value {
        serde_json::json!({
            "type": "database",
            "db_type": self.kind.label(),
            "description": format!("数据库存储 ({})", self.kind.label()),
            "database_url": self.redacted_url,
        })
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Collection {
    Accounts,
    AuthKeys,
}

fn make_snapshot(
    collection: Collection,
    records: Vec<Value>,
    cumulative_total: Option<u64>,
) -> Result<StorageSnapshot, StorageError> {
    let limit = match collection {
        Collection::Accounts => super::MAX_ACCOUNTS,
        Collection::AuthKeys => super::MAX_AUTH_KEYS,
    };
    if records.len() > limit || records.iter().any(|record| !record.is_object()) {
        return Err(StorageError::Invalid);
    }
    let key_name = match collection {
        Collection::Accounts => "access_token",
        Collection::AuthKeys => "id",
    };
    let mut identities = HashSet::with_capacity(records.len());
    for record in &records {
        if !identities.insert(record_key(record, key_name)?) {
            return Err(StorageError::Invalid);
        }
    }
    let bytes = match collection {
        Collection::Accounts => {
            let value = if let Some(cumulative_total) = cumulative_total {
                serde_json::json!({"items": records, "cumulative_total": cumulative_total})
            } else {
                Value::Array(records.clone())
            };
            let bytes = serde_json::to_vec(&value).map_err(|_| StorageError::Invalid)?;
            super::account_snapshot::validate_bytes(&bytes).map_err(|_| StorageError::Invalid)?;
            super::parse_account_document_bytes(&bytes).map_err(|_| StorageError::Invalid)?;
            bytes
        }
        Collection::AuthKeys => {
            let bytes = serde_json::to_vec(&serde_json::json!({"items": records}))
                .map_err(|_| StorageError::Invalid)?;
            if bytes.len() as u64 > super::MAX_AUTH_KEYS_BYTES {
                return Err(StorageError::Invalid);
            }
            super::parse_auth_document_bytes(&bytes).map_err(|_| StorageError::Invalid)?;
            bytes
        }
    };
    let revision = canonical_revision(&records)?;
    if bytes.is_empty() {
        return Err(StorageError::Invalid);
    }
    Ok(StorageSnapshot {
        records,
        revision,
        cumulative_total,
    })
}

fn canonical_revision(records: &[Value]) -> Result<[u8; 32], StorageError> {
    let mut bytes = Vec::new();
    write_canonical(&Value::Array(records.to_vec()), &mut bytes)?;
    Ok(Sha256::digest(bytes).into())
}

fn write_canonical(value: &Value, output: &mut Vec<u8>) -> Result<(), StorageError> {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_writer(&mut *output, value).map_err(|_| StorageError::Invalid)?;
        }
        Value::Array(items) => {
            output.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical(item, output)?;
            }
            output.push(b']');
        }
        Value::Object(object) => {
            output.push(b'{');
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key).map_err(|_| StorageError::Invalid)?;
                output.push(b':');
                write_canonical(&object[key], output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn record_key(record: &Value, name: &str) -> Result<String, StorageError> {
    record
        .as_object()
        .and_then(|object| object.get(name))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or(StorageError::Invalid)
}

fn token_hash(token: &str) -> String {
    Sha256::digest(token.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn schema_lock_digest(database_name: &str) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(SCHEMA_LOCK_NAMESPACE);
    digest.update([0]);
    digest.update(database_name.as_bytes());
    digest.finalize().into()
}

fn postgres_schema_lock_key(database_name: &str) -> i64 {
    let digest = schema_lock_digest(database_name);
    let mut key = [0_u8; 8];
    key.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(key)
}

fn mysql_schema_lock_name(database_name: &str) -> String {
    let digest = schema_lock_digest(database_name)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let digest_length = 64 - MYSQL_SCHEMA_LOCK_PREFIX.len();
    format!("{MYSQL_SCHEMA_LOCK_PREFIX}{}", &digest[..digest_length])
}

async fn server_database_name(
    connection: &mut AnyConnection,
    kind: DatabaseKind,
) -> Result<String, StorageError> {
    let statement = match kind {
        DatabaseKind::Postgres => "SELECT current_database()::text AS database_name",
        DatabaseKind::MySql => "SELECT DATABASE() AS database_name",
        DatabaseKind::Sqlite => return Err(StorageError::Invalid),
    };
    let row = sqlx::query(statement)
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| StorageError::Unavailable)?;
    row.try_get::<Option<String>, _>("database_name")
        .map_err(|_| StorageError::Unavailable)?
        .filter(|name| !name.is_empty() && name.len() <= 255 && !name.contains('\0'))
        .ok_or_else(|| invalid_database_stage("server.database_name"))
}

fn database_connect_url(database_url: &str, kind: DatabaseKind) -> String {
    if kind != DatabaseKind::Sqlite {
        return database_url.to_owned();
    }
    if database_url == "sqlite:///:memory:" {
        return "sqlite::memory:".to_owned();
    }
    if database_url.contains('?') {
        format!("{database_url}&mode=rwc")
    } else {
        format!("{database_url}?mode=rwc")
    }
}

fn redact_url(value: &str) -> String {
    let without_fragment = value.split('#').next().unwrap_or_default();
    let without_query = without_fragment.split('?').next().unwrap_or_default();
    let Some((scheme, remainder)) = without_query.split_once("://") else {
        return "[REDACTED_URL]".to_owned();
    };
    let authority_end = remainder.find('/').unwrap_or(remainder.len());
    let (authority, path) = remainder.split_at(authority_end);
    let authority = authority
        .rsplit_once('@')
        .map(|(_, host)| format!("[REDACTED]@{host}"))
        .unwrap_or_else(|| authority.to_owned());
    format!("{scheme}://{authority}{path}")
}

fn sqlite_url(path: &Path) -> String {
    format!("sqlite:///{}", path.to_string_lossy().replace('\\', "/"))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        process::Command,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode, header},
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn database_path(label: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = crate::project_local_test_tmp_dir().join(format!(
            "chatgpt2api-storage-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("storage test root");
        root.join("accounts.db")
    }

    async fn health_json_within(state: &crate::AppState, budget: Duration) -> Value {
        let response = tokio::time::timeout(
            budget,
            state.router().oneshot(
                Request::builder()
                    .uri("/health?format=json")
                    .body(Body::empty())
                    .expect("health request"),
            ),
        )
        .await
        .expect("health request exceeded public budget")
        .expect("health response");
        assert_eq!(response.status(), StatusCode::OK);
        serde_json::from_slice(
            &response
                .into_body()
                .collect()
                .await
                .expect("health body")
                .to_bytes(),
        )
        .expect("health JSON")
    }

    fn run_git(directory: &Path, arguments: &[&str]) {
        let output = Command::new("git")
            .args(arguments)
            .current_dir(directory)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .expect("run git test command");
        assert!(
            output.status.success(),
            "git test command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn create_bare_git_storage_remote(label: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let anchor = database_path(label);
        let root = anchor.parent().expect("Git test root").to_owned();
        let remote = root.join("remote.git");
        let seed = root.join("seed");
        fs::create_dir_all(&seed).expect("Git seed directory");
        run_git(
            &root,
            &["init", "--bare", remote.to_str().expect("remote path")],
        );
        run_git(&seed, &["init"]);
        fs::write(seed.join("accounts.json"), b"[]\n").expect("seed accounts");
        fs::write(seed.join("auth_keys.json"), b"{\"items\":[]}\n").expect("seed auth keys");
        run_git(&seed, &["add", "--", "accounts.json", "auth_keys.json"]);
        run_git(
            &seed,
            &[
                "-c",
                "user.name=chatgpt2api-test",
                "-c",
                "user.email=chatgpt2api-test@example.invalid",
                "commit",
                "-m",
                "seed",
            ],
        );
        run_git(&seed, &["branch", "-M", "main"]);
        run_git(
            &seed,
            &[
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        run_git(&seed, &["push", "-u", "origin", "main"]);
        (root, remote)
    }

    #[tokio::test]
    async fn git_backend_owns_accounts_auth_cumulative_cas_and_redacted_info() {
        let (root, remote) = create_bare_git_storage_remote("git-owner-contract");
        let first = StorageBackend::connect_git(
            remote.to_str().expect("remote URL"),
            "opaque-test-token",
            "main",
            "snapshots/accounts.json",
            "snapshots/auth_keys.json",
            &root.join("cache-first"),
        )
        .await
        .expect("first Git backend");
        let second = StorageBackend::connect_git(
            remote.to_str().expect("remote URL"),
            "opaque-test-token",
            "main",
            "snapshots/accounts.json",
            "snapshots/auth_keys.json",
            &root.join("cache-second"),
        )
        .await
        .expect("second Git backend");

        let initial = first.load_accounts().await.expect("initial Git accounts");
        assert!(initial.records.is_empty());
        let stale = second.load_accounts().await.expect("stale Git accounts");
        let saved = first
            .save_accounts(
                initial.revision,
                vec![serde_json::json!({
                    "access_token":"git-token-a",
                    "status":"正常",
                    "type":"pro",
                    "source_type":"web"
                })],
                9,
            )
            .await
            .expect("save Git accounts");
        assert_eq!(saved.cumulative_total, Some(9));
        assert_eq!(
            second.load_accounts().await.expect("pulled Git accounts"),
            saved
        );
        assert_eq!(
            second.save_accounts(stale.revision, Vec::new(), 0).await,
            Err(StorageError::Conflict)
        );

        let auth = first.load_auth_keys().await.expect("initial Git auth");
        let auth = first
            .save_auth_keys(
                auth.revision,
                vec![serde_json::json!({
                    "id":"git-key",
                    "role":"admin",
                    "key_hash":"c".repeat(64),
                    "enabled":true
                })],
            )
            .await
            .expect("save Git auth");
        assert_eq!(
            second.load_auth_keys().await.expect("pulled Git auth"),
            auth
        );
        let (_, _, health) = first
            .load_health_snapshots()
            .await
            .expect("Git health snapshots");
        assert_eq!(health["status"], "healthy");
        assert_eq!(health["backend"], "git");
        assert!(
            health["last_commit"]
                .as_str()
                .is_some_and(|value| value.len() == 8)
        );
        let info = first.info();
        assert_eq!(info["type"], "git");
        let serialized = serde_json::to_string(&info).expect("Git info JSON");
        assert!(!serialized.contains("opaque-test-token"));

        first.close().await;
        second.close().await;
        fs::remove_dir_all(root).expect("Git storage cleanup");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn git_health_reclaims_orphan_candidate_refs_across_instances_and_retries_cleanup() {
        let (root, remote) = create_bare_git_storage_remote("git-health-orphan-ref");
        let cache = root.join("cache");
        let first = Arc::new(
            StorageBackend::connect_git(
                remote.to_str().expect("remote URL"),
                "",
                "main",
                "accounts.json",
                "auth_keys.json",
                &cache,
            )
            .await
            .expect("first Git backend"),
        );
        let first_git = match first.as_ref() {
            StorageBackend::Git(git) => git.clone(),
            StorageBackend::Json(_) => panic!("expected Git backend"),
            StorageBackend::Database(_) => panic!("expected Git backend"),
        };
        let repo = first_git.validate_repo_path().expect("first Git repo");
        let head = first_git.current_head(&repo).await.expect("first Git head");
        first.close().await;
        run_git(
            &repo,
            &[
                "update-ref",
                "refs/chatgpt2api/health-refresh/999999-1",
                &head,
            ],
        );
        drop(first);

        let second = Arc::new(
            StorageBackend::connect_git(
                remote.to_str().expect("remote URL"),
                "",
                "main",
                "accounts.json",
                "auth_keys.json",
                &cache,
            )
            .await
            .expect("second Git backend reclaims orphan"),
        );
        let second_git = match second.as_ref() {
            StorageBackend::Git(git) => git.clone(),
            StorageBackend::Json(_) => panic!("expected Git backend"),
            StorageBackend::Database(_) => panic!("expected Git backend"),
        };
        assert_eq!(second_git.health_candidate_ref_count_for_test().await, 0);

        let state = crate::AppState::new_with_storage_backend(
            crate::AppConfig {
                version: "git-health-orphan-ref-test".to_owned(),
                auth_key: None,
                models: vec!["auto".to_owned()],
                upstream_base_url: None,
                upstream_auth: None,
                auth_keys_path: None,
                models_path: None,
                accounts_path: None,
                upstream_protocol: crate::UpstreamProtocol::ChatGpt,
            },
            second.clone(),
            root.clone(),
        )
        .await
        .expect("Git state after orphan recovery");
        second_git.force_health_refresh_due_for_test(false);
        let _ = health_json_within(&state, Duration::from_millis(250)).await;
        tokio::time::timeout(Duration::from_secs(10), async {
            while second_git.health_refresh_running_for_test() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("legitimate refresh after orphan recovery did not terminate");
        let healthy = health_json_within(&state, Duration::from_millis(250)).await;
        assert_eq!(healthy["storage"]["health"]["status"], "healthy");
        assert_eq!(second_git.health_candidate_ref_count_for_test().await, 0);

        second_git.fail_health_candidate_cleanup_for_test(true);
        let cleanup_ref = format!(
            "{}{}-{}",
            GIT_HEALTH_CANDIDATE_NAMESPACE,
            std::process::id(),
            777_u64
        );
        run_git(&repo, &["update-ref", &cleanup_ref, &head]);
        second_git.cleanup_health_refresh_candidate().await;
        assert!(second_git.health_candidate_cleanup_pending_for_test());
        assert_eq!(second_git.health_candidate_ref_count_for_test().await, 1);
        second_git.fail_health_candidate_cleanup_for_test(false);
        second_git.cleanup_health_refresh_candidate().await;
        assert!(!second_git.health_candidate_cleanup_pending_for_test());
        assert_eq!(second_git.health_candidate_ref_count_for_test().await, 0);

        drop(state);
        second.close().await;
        assert_eq!(second_git.health_candidate_ref_count_for_test().await, 0);
        drop(second);
        fs::remove_dir_all(root).expect("Git orphan-ref cleanup");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn git_health_cache_tracks_successful_account_and_auth_mutations() {
        let (root, remote) = create_bare_git_storage_remote("git-health-cache-mutations");
        let backend = Arc::new(
            StorageBackend::connect_git(
                remote.to_str().expect("remote URL"),
                "",
                "main",
                "accounts.json",
                "auth_keys.json",
                &root.join("cache"),
            )
            .await
            .expect("Git backend"),
        );
        let accounts = backend.load_accounts().await.expect("empty accounts");
        backend
            .save_accounts(
                accounts.revision,
                vec![serde_json::json!({
                    "access_token":"git-cache-account",
                    "type":"pro",
                    "source_type":"web"
                })],
                1,
            )
            .await
            .expect("initial account");
        let auth = backend.load_auth_keys().await.expect("empty auth");
        backend
            .save_auth_keys(
                auth.revision,
                vec![serde_json::json!({
                    "id":"git-cache-admin",
                    "role":"admin",
                    "key_hash":token_hash("git-cache-admin-secret"),
                    "enabled":true
                })],
            )
            .await
            .expect("initial auth");
        let state = crate::AppState::new_with_storage_backend(
            crate::AppConfig {
                version: "git-health-cache-mutations-test".to_owned(),
                auth_key: None,
                models: vec!["auto".to_owned()],
                upstream_base_url: None,
                upstream_auth: None,
                auth_keys_path: None,
                models_path: None,
                accounts_path: None,
                upstream_protocol: crate::UpstreamProtocol::ChatGpt,
            },
            backend.clone(),
            root.clone(),
        )
        .await
        .expect("Git app state");
        let initial = health_json_within(&state, Duration::from_millis(250)).await;
        assert_eq!(
            initial["storage"],
            json!({"backend": "git", "health": {"status": "healthy"}})
        );
        let initial_cache = state
            .health_snapshot_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(initial_cache.account_count, 1);
        assert_eq!(initial_cache.auth_key_count, 1);

        let account_response = state
            .router()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/accounts")
                    .header(header::AUTHORIZATION, "Bearer git-cache-admin-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"tokens":["git-cache-account"]}"#))
                    .expect("account mutation request"),
            )
            .await
            .expect("account mutation response");
        assert_eq!(account_response.status(), StatusCode::OK);
        assert!(state.account_store.records().is_empty());
        let after_account = health_json_within(&state, Duration::from_millis(250)).await;
        assert_eq!(
            after_account["storage"],
            json!({"backend": "git", "health": {"status": "healthy"}})
        );
        let after_account_cache = state
            .health_snapshot_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(after_account_cache.account_count, 0);
        assert_eq!(after_account_cache.auth_key_count, 1);
        assert_eq!(after_account["healthy"], false);

        let auth_response = state
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/users")
                    .header(header::AUTHORIZATION, "Bearer git-cache-admin-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"name":"cache-second-user"}"#))
                    .expect("auth mutation request"),
            )
            .await
            .expect("auth mutation response");
        assert_eq!(auth_response.status(), StatusCode::OK);
        assert_eq!(state.auth_store.public_records().len(), 2);
        let after_auth = health_json_within(&state, Duration::from_millis(250)).await;
        assert_eq!(
            after_auth["storage"],
            json!({"backend": "git", "health": {"status": "healthy"}})
        );
        let after_auth_cache = state
            .health_snapshot_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(after_auth_cache.account_count, 0);
        assert_eq!(after_auth_cache.auth_key_count, 2);
        assert_eq!(after_auth["healthy"], false);

        let concurrent_add = state
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/accounts")
                    .header(header::AUTHORIZATION, "Bearer git-cache-admin-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"tokens":["git-cache-concurrent-account"]}"#))
                    .expect("concurrent account seed request"),
            )
            .await
            .expect("concurrent account seed response");
        assert_eq!(concurrent_add.status(), StatusCode::OK);
        assert_eq!(state.account_store.records().len(), 1);

        let (concurrent_account, concurrent_auth) = tokio::join!(
            state.router().oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/accounts")
                    .header(header::AUTHORIZATION, "Bearer git-cache-admin-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"tokens":["git-cache-concurrent-account"]}"#,))
                    .expect("concurrent account mutation request"),
            ),
            state.router().oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/users")
                    .header(header::AUTHORIZATION, "Bearer git-cache-admin-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"name":"cache-third-user"}"#))
                    .expect("concurrent auth mutation request"),
            )
        );
        assert_eq!(
            concurrent_account
                .expect("concurrent account mutation response")
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            concurrent_auth
                .expect("concurrent auth mutation response")
                .status(),
            StatusCode::OK
        );
        assert!(state.account_store.records().is_empty());
        assert_eq!(state.auth_store.public_records().len(), 3);
        let after_concurrent = health_json_within(&state, Duration::from_millis(250)).await;
        assert_eq!(
            after_concurrent["storage"],
            json!({"backend": "git", "health": {"status": "healthy"}})
        );
        let after_concurrent_cache = state
            .health_snapshot_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(after_concurrent_cache.account_count, 0);
        assert_eq!(after_concurrent_cache.auth_key_count, 3);
        assert_eq!(after_concurrent["healthy"], false);

        drop(state);
        backend.close().await;
        fs::remove_dir_all(root).expect("Git cache mutation cleanup");
    }

    #[tokio::test]
    async fn git_backend_pending_push_and_rejected_push_recover_remote_state() {
        let (root, remote) = create_bare_git_storage_remote("git-recovery-contract");
        let cache = root.join("cache");
        let backend = StorageBackend::connect_git(
            remote.to_str().expect("remote URL"),
            "",
            "main",
            "accounts.json",
            "auth_keys.json",
            &cache,
        )
        .await
        .expect("Git backend");
        let initial = backend.load_accounts().await.expect("initial accounts");
        let saved = backend
            .save_accounts(
                initial.revision,
                vec![serde_json::json!({
                    "access_token":"remote-token",
                    "status":"正常",
                    "type":"pro",
                    "source_type":"web"
                })],
                3,
            )
            .await
            .expect("seed remote snapshot");
        backend.close().await;

        let repo = cache.join("repo");
        fs::write(
            repo.join("accounts.json"),
            b"[{\"access_token\":\"local-only-secret\"}]\n",
        )
        .expect("local-only snapshot");
        run_git(&repo, &["add", "--", "accounts.json"]);
        run_git(
            &repo,
            &[
                "-c",
                "user.name=chatgpt2api-test",
                "-c",
                "user.email=chatgpt2api-test@example.invalid",
                "commit",
                "-m",
                "local-only",
            ],
        );
        fs::write(cache.join(GIT_PENDING_PUSH), b"pending\n").expect("pending marker");
        let recovered = StorageBackend::connect_git(
            remote.to_str().expect("remote URL"),
            "",
            "main",
            "accounts.json",
            "auth_keys.json",
            &cache,
        )
        .await
        .expect("recovered Git backend");
        assert_eq!(
            recovered.load_accounts().await.expect("recovered accounts"),
            saved
        );
        assert!(!cache.join(GIT_PENDING_PUSH).exists());

        let hook = remote.join("hooks").join("pre-receive");
        fs::write(&hook, b"#!/bin/sh\nexit 1\n").expect("rejecting hook");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&hook, fs::Permissions::from_mode(0o700))
                .expect("hook permissions");
        }
        assert_eq!(
            recovered
                .save_accounts(
                    saved.revision,
                    vec![serde_json::json!({
                        "access_token":"must-not-persist",
                        "status":"正常",
                        "type":"team",
                        "source_type":"web"
                    })],
                    4,
                )
                .await,
            Err(StorageError::Unavailable)
        );
        assert!(!cache.join(GIT_PENDING_PUSH).exists());
        assert_eq!(
            recovered
                .load_accounts()
                .await
                .expect("remote after rejection"),
            saved
        );
        fs::remove_file(&hook).expect("remove rejecting hook");
        let retried = recovered
            .save_accounts(
                saved.revision,
                vec![serde_json::json!({
                    "access_token":"retry-token",
                    "status":"正常",
                    "type":"team",
                    "source_type":"web"
                })],
                4,
            )
            .await
            .expect("retry after recovery");
        assert_eq!(retried.cumulative_total, Some(4));
        recovered.close().await;
        fs::remove_dir_all(root).expect("Git recovery cleanup");
    }

    #[tokio::test]
    async fn git_backend_rejects_unsafe_paths_cache_types_and_orphan_pending_marker() {
        let (root, remote) = create_bare_git_storage_remote("git-boundary-contract");
        for path in ["", ".", "../accounts.json", "/accounts.json", "a//b.json"] {
            assert!(matches!(
                StorageBackend::connect_git(
                    remote.to_str().expect("remote URL"),
                    "",
                    "main",
                    path,
                    "auth_keys.json",
                    &root.join(format!("invalid-{}", token_hash(path))),
                )
                .await,
                Err(StorageError::Invalid)
            ));
        }
        let regular_cache = root.join("regular-cache");
        fs::write(&regular_cache, b"not a directory").expect("regular cache file");
        assert!(matches!(
            StorageBackend::connect_git(
                remote.to_str().expect("remote URL"),
                "",
                "main",
                "accounts.json",
                "auth_keys.json",
                &regular_cache,
            )
            .await,
            Err(StorageError::Invalid)
        ));
        let orphan_cache = root.join("orphan-cache");
        fs::create_dir(&orphan_cache).expect("orphan cache");
        fs::write(orphan_cache.join(GIT_PENDING_PUSH), b"pending\n").expect("orphan marker");
        assert!(matches!(
            StorageBackend::connect_git(
                remote.to_str().expect("remote URL"),
                "",
                "main",
                "accounts.json",
                "auth_keys.json",
                &orphan_cache,
            )
            .await,
            Err(StorageError::Invalid)
        ));
        assert!(!orphan_cache.join("repo").exists());

        let (execution_url, embedded) = git_execution_url(
            "https://user:opaque-secret@example.test/private.git?query-secret#fragment-secret",
        )
        .expect("sanitized Git URL");
        assert_eq!(embedded.as_deref(), Some("opaque-secret"));
        for secret in ["user", "opaque-secret", "query-secret", "fragment-secret"] {
            assert!(!execution_url.contains(secret));
        }
        assert_eq!(execution_url, "https://example.test/private.git");
        fs::remove_dir_all(root).expect("Git boundary cleanup");
    }

    #[tokio::test]
    async fn concurrent_git_account_import_retries_remote_cas_without_lost_records() {
        let (root, remote) = create_bare_git_storage_remote("git-concurrent-import");
        let first_backend = Arc::new(
            StorageBackend::connect_git(
                remote.to_str().expect("remote URL"),
                "",
                "main",
                "accounts.json",
                "auth_keys.json",
                &root.join("cache-first"),
            )
            .await
            .expect("first Git backend"),
        );
        let initial = first_backend
            .load_accounts()
            .await
            .expect("initial Git accounts");
        first_backend
            .save_accounts(
                initial.revision,
                vec![serde_json::json!({
                    "access_token":"token-a",
                    "status":"正常",
                    "type":"pro",
                    "source_type":"web"
                })],
                30,
            )
            .await
            .expect("seed Git accounts");
        let second_backend = Arc::new(
            StorageBackend::connect_git(
                remote.to_str().expect("remote URL"),
                "",
                "main",
                "accounts.json",
                "auth_keys.json",
                &root.join("cache-second"),
            )
            .await
            .expect("second Git backend"),
        );
        let first_store = crate::account_pool::AccountStore::load_backend(first_backend.clone())
            .await
            .expect("first Git store");
        let second_store = crate::account_pool::AccountStore::load_backend(second_backend.clone())
            .await
            .expect("second Git store");
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let first_barrier = barrier.clone();
        let first = tokio::spawn(async move {
            first_barrier.wait().await;
            first_store
                .merge_import_records(vec![serde_json::json!({
                    "access_token":"token-b",
                    "status":"正常",
                    "type":"plus",
                    "source_type":"web"
                })])
                .await
        });
        let second_barrier = barrier.clone();
        let second = tokio::spawn(async move {
            second_barrier.wait().await;
            second_store
                .merge_import_records(vec![serde_json::json!({
                    "access_token":"token-c",
                    "status":"正常",
                    "type":"team",
                    "source_type":"web"
                })])
                .await
        });
        barrier.wait().await;
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.expect("first task").expect("first import"), (1, 0));
        assert_eq!(second.expect("second task").expect("second import"), (1, 0));
        let persisted = first_backend
            .load_accounts()
            .await
            .expect("persisted Git accounts");
        assert_eq!(persisted.cumulative_total, Some(32));
        let tokens = persisted
            .records
            .iter()
            .map(|record| record["access_token"].as_str().expect("token"))
            .collect::<HashSet<_>>();
        assert_eq!(tokens, HashSet::from(["token-a", "token-b", "token-c"]));
        first_backend.close().await;
        second_backend.close().await;
        fs::remove_dir_all(root).expect("Git concurrent cleanup");
    }

    #[tokio::test]
    async fn app_state_uses_git_backend_for_auth_mutation_health_and_info() {
        let (root, remote) = create_bare_git_storage_remote("git-app-state");
        let backend = Arc::new(
            StorageBackend::connect_git(
                remote.to_str().expect("remote URL"),
                "",
                "main",
                "accounts.json",
                "auth_keys.json",
                &root.join("cache"),
            )
            .await
            .expect("Git backend"),
        );
        let accounts = backend.load_accounts().await.expect("empty accounts");
        backend
            .save_accounts(
                accounts.revision,
                vec![serde_json::json!({
                    "access_token":"git-app-token",
                    "status":"正常",
                    "type":"pro",
                    "source_type":"web"
                })],
                5,
            )
            .await
            .expect("seed Git accounts");
        let raw_key = "git-app-admin-secret";
        let auth = backend.load_auth_keys().await.expect("empty auth");
        backend
            .save_auth_keys(
                auth.revision,
                vec![serde_json::json!({
                    "id":"git-app-admin",
                    "role":"admin",
                    "key_hash":token_hash(raw_key),
                    "enabled":true
                })],
            )
            .await
            .expect("seed Git auth");
        let state = crate::AppState::new_with_storage_backend(
            crate::AppConfig {
                version: "git-storage-app-test".to_owned(),
                auth_key: None,
                models: vec!["auto".to_owned()],
                upstream_base_url: None,
                upstream_auth: None,
                auth_keys_path: None,
                models_path: None,
                accounts_path: None,
                upstream_protocol: crate::UpstreamProtocol::ChatGpt,
            },
            backend.clone(),
            root.clone(),
        )
        .await
        .expect("Git app state");
        let request = Request::builder()
            .method("POST")
            .uri("/api/accounts/update")
            .header(header::AUTHORIZATION, format!("Bearer {raw_key}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"access_token":"git-app-token","status":"禁用"}"#,
            ))
            .expect("account update request");
        let response = state
            .router()
            .oneshot(request)
            .await
            .expect("account update");
        assert_eq!(response.status(), StatusCode::OK);
        let persisted = backend
            .load_accounts()
            .await
            .expect("persisted Git accounts");
        assert_eq!(persisted.records[0]["status"], "禁用");
        assert_eq!(persisted.cumulative_total, Some(5));

        let response = state
            .router()
            .oneshot(
                Request::builder()
                    .uri("/health?format=json")
                    .body(Body::empty())
                    .expect("health request"),
            )
            .await
            .expect("health response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload: Value = serde_json::from_slice(
            &response
                .into_body()
                .collect()
                .await
                .expect("health body")
                .to_bytes(),
        )
        .expect("health JSON");
        assert_eq!(
            payload["storage"],
            json!({"backend": "git", "health": {"status": "healthy"}})
        );
        assert_eq!(
            payload["healthy"], false,
            "disabled-only account degrades overall health"
        );

        let response = state
            .router()
            .oneshot(
                Request::builder()
                    .uri("/api/storage/info")
                    .header(header::AUTHORIZATION, format!("Bearer {raw_key}"))
                    .body(Body::empty())
                    .expect("storage info request"),
            )
            .await
            .expect("storage info response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload: Value = serde_json::from_slice(
            &response
                .into_body()
                .collect()
                .await
                .expect("storage info body")
                .to_bytes(),
        )
        .expect("storage info JSON");
        assert_eq!(
            payload,
            json!({
                "backend": {"type": "git"},
                "health": {"status": "healthy"},
            })
        );
        drop(state);
        backend.close().await;
        fs::remove_dir_all(root).expect("Git app cleanup");
    }

    #[tokio::test]
    async fn git_health_stays_bounded_during_slow_refresh_and_exposes_failure_staleness() {
        let (root, remote) = create_bare_git_storage_remote("git-health-refresh");
        let backend = Arc::new(
            StorageBackend::connect_git(
                remote.to_str().expect("remote URL"),
                "",
                "main",
                "accounts.json",
                "auth_keys.json",
                &root.join("cache"),
            )
            .await
            .expect("Git backend"),
        );
        let accounts = backend.load_accounts().await.expect("empty accounts");
        backend
            .save_accounts(
                accounts.revision,
                vec![serde_json::json!({
                    "access_token":"git-health-token",
                    "status":"正常",
                    "type":"pro",
                    "source_type":"web"
                })],
                1,
            )
            .await
            .expect("seed Git accounts");
        let auth = backend.load_auth_keys().await.expect("empty auth");
        backend
            .save_auth_keys(
                auth.revision,
                vec![serde_json::json!({
                    "id":"git-health-admin",
                    "role":"admin",
                    "key_hash":token_hash("git-health-admin-secret"),
                    "enabled":true
                })],
            )
            .await
            .expect("seed Git auth");
        let state = crate::AppState::new_with_storage_backend(
            crate::AppConfig {
                version: "git-health-refresh-test".to_owned(),
                auth_key: None,
                models: vec!["auto".to_owned()],
                upstream_base_url: None,
                upstream_auth: None,
                auth_keys_path: None,
                models_path: None,
                accounts_path: None,
                upstream_protocol: crate::UpstreamProtocol::ChatGpt,
            },
            backend.clone(),
            root.clone(),
        )
        .await
        .expect("Git app state");
        let git = match backend.as_ref() {
            StorageBackend::Git(git) => git.clone(),
            StorageBackend::Json(_) => panic!("expected Git backend"),
            StorageBackend::Database(_) => panic!("expected Git backend"),
        };
        let hook = GitHealthRefreshTestHook {
            started: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            starts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            fail: Arc::new(AtomicBool::new(false)),
            pause_before_prepare: Arc::new(AtomicBool::new(true)),
            after_fetch: Arc::new(tokio::sync::Notify::new()),
            release_after_fetch: Arc::new(tokio::sync::Notify::new()),
            pause_after_fetch: Arc::new(AtomicBool::new(true)),
            after_prepare: Arc::new(tokio::sync::Notify::new()),
            release_before_commit: Arc::new(tokio::sync::Notify::new()),
            pause_after_prepare: Arc::new(AtomicBool::new(false)),
        };
        git.set_health_refresh_test_hook(Some(hook.clone()));
        git.force_health_refresh_due_for_test(false);

        let refreshing = health_json_within(&state, Duration::from_millis(250)).await;
        assert_eq!(
            refreshing["storage"],
            json!({"backend": "git", "health": {"status": "healthy"}})
        );
        tokio::time::timeout(Duration::from_secs(2), hook.started.notified())
            .await
            .expect("slow Git verification did not start");
        assert_eq!(
            git.health_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .status,
            GitSyncStatus::Refreshing
        );
        hook.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), hook.after_fetch.notified())
            .await
            .expect("slow Git Phase A did not reach post-fetch barrier");

        let accounts_response = tokio::time::timeout(
            Duration::from_millis(250),
            state.router().oneshot(
                Request::builder()
                    .uri("/api/accounts")
                    .header(header::AUTHORIZATION, "Bearer git-health-admin-secret")
                    .body(Body::empty())
                    .expect("accounts request during Phase A"),
            ),
        )
        .await
        .expect("accounts request blocked by Phase A fetch")
        .expect("accounts response during Phase A");
        assert_eq!(accounts_response.status(), StatusCode::OK);
        let accounts_payload: Value = serde_json::from_slice(
            &accounts_response
                .into_body()
                .collect()
                .await
                .expect("accounts body during Phase A")
                .to_bytes(),
        )
        .expect("accounts JSON during Phase A");
        assert_eq!(accounts_payload["items"].as_array().map(Vec::len), Some(1));

        let still_refreshing = health_json_within(&state, Duration::from_millis(250)).await;
        assert_eq!(
            still_refreshing["storage"],
            json!({"backend": "git", "health": {"status": "healthy"}})
        );
        assert_eq!(hook.starts.load(Ordering::SeqCst), 1);
        hook.release_after_fetch.notify_one();
        tokio::time::timeout(Duration::from_secs(10), async {
            while git.health_refresh_running_for_test() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("slow Git Phase A did not finish");

        git.force_health_stale_for_test();
        let stale = health_json_within(&state, Duration::from_millis(250)).await;
        assert_eq!(
            stale["storage"],
            json!({
                "backend": "git",
                "health": {"status": "unhealthy", "error": "存储后端健康检查失败"},
            })
        );
        assert!(matches!(
            git.health_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .status,
            GitSyncStatus::Refreshing | GitSyncStatus::Stale
        ));
        assert_eq!(stale["healthy"], false);

        hook.fail.store(true, Ordering::SeqCst);
        hook.release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while git.health_refresh_running_for_test() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed Git verification did not terminate");
        let failed = health_json_within(&state, Duration::from_millis(250)).await;
        assert_eq!(
            failed["storage"],
            json!({
                "backend": "git",
                "health": {"status": "unhealthy", "error": "存储后端健康检查失败"},
            })
        );
        assert_eq!(
            git.health_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .status,
            GitSyncStatus::Error
        );

        git.set_health_refresh_test_hook(None);
        git.force_health_refresh_due_for_test(false);
        let retrying = health_json_within(&state, Duration::from_millis(250)).await;
        assert_eq!(
            retrying["storage"],
            json!({
                "backend": "git",
                "health": {"status": "unhealthy", "error": "存储后端健康检查失败"},
            })
        );
        tokio::time::timeout(Duration::from_secs(10), async {
            while git.health_refresh_running_for_test() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Git verification recovery did not terminate");
        let recovered = health_json_within(&state, Duration::from_millis(250)).await;
        assert_eq!(
            recovered["storage"],
            json!({"backend": "git", "health": {"status": "healthy"}})
        );
        assert_eq!(recovered["healthy"], true);

        drop(state);
        backend.close().await;
        fs::remove_dir_all(root).expect("Git health refresh cleanup");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn git_health_refresh_keeps_validated_head_on_invalid_remote_and_publishes_pair_after_recovery()
     {
        let (root, remote) = create_bare_git_storage_remote("git-health-pair-transaction");
        let backend = Arc::new(
            StorageBackend::connect_git(
                remote.to_str().expect("remote URL"),
                "",
                "main",
                "accounts.json",
                "auth_keys.json",
                &root.join("cache"),
            )
            .await
            .expect("Git backend"),
        );
        let accounts = backend.load_accounts().await.expect("empty accounts");
        backend
            .save_accounts(
                accounts.revision,
                vec![serde_json::json!({
                    "access_token":"git-pair-old-account",
                    "status":"正常",
                    "type":"pro",
                    "source_type":"web"
                })],
                1,
            )
            .await
            .expect("seed Git account");
        let auth = backend.load_auth_keys().await.expect("empty auth");
        backend
            .save_auth_keys(
                auth.revision,
                vec![serde_json::json!({
                    "id":"git-pair-old-auth",
                    "role":"admin",
                    "key_hash":token_hash("git-pair-old-secret"),
                    "enabled":true
                })],
            )
            .await
            .expect("seed Git auth");
        let state = crate::AppState::new_with_storage_backend(
            crate::AppConfig {
                version: "git-health-pair-transaction-test".to_owned(),
                auth_key: None,
                models: vec!["auto".to_owned()],
                upstream_base_url: None,
                upstream_auth: None,
                auth_keys_path: None,
                models_path: None,
                accounts_path: None,
                upstream_protocol: crate::UpstreamProtocol::ChatGpt,
            },
            backend.clone(),
            root.clone(),
        )
        .await
        .expect("Git app state");
        let git = match backend.as_ref() {
            StorageBackend::Git(git) => git.clone(),
            StorageBackend::Json(_) => panic!("expected Git backend"),
            StorageBackend::Database(_) => panic!("expected Git backend"),
        };
        let repo = git.validate_repo_path().expect("validated Git repo");
        let old_commit = git.current_head(&repo).await.expect("old Git head");
        let seed = root.join("seed");

        run_git(&seed, &["fetch", "origin", "main"]);
        run_git(&seed, &["reset", "--hard", "FETCH_HEAD"]);
        fs::write(
            seed.join("accounts.json"),
            br#"{"items":[{"access_token":123,"type":"pro","source_type":"web"}],"cumulative_total":1}
"#,
        )
        .expect("invalid remote accounts");
        run_git(&seed, &["add", "--", "accounts.json"]);
        run_git(
            &seed,
            &[
                "-c",
                "user.name=chatgpt2api-test",
                "-c",
                "user.email=chatgpt2api-test@example.invalid",
                "commit",
                "-m",
                "invalid accounts snapshot",
            ],
        );
        run_git(&seed, &["push", "origin", "main"]);

        git.force_health_refresh_due_for_test(false);
        let _ = health_json_within(&state, Duration::from_millis(250)).await;
        tokio::time::timeout(Duration::from_secs(10), async {
            while !git.health_refresh_running_for_test() {
                tokio::task::yield_now().await;
            }
            while git.health_refresh_running_for_test() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("invalid Git refresh did not terminate");
        let head_after_invalid = git
            .current_head(&repo)
            .await
            .expect("Git head after invalid refresh");
        assert_eq!(
            head_after_invalid, old_commit,
            "invalid remote must not move the validated canonical HEAD"
        );
        let invalid = health_json_within(&state, Duration::from_millis(250)).await;
        assert_eq!(
            invalid["storage"],
            json!({
                "backend": "git",
                "health": {"status": "unhealthy", "error": "存储后端健康检查失败"},
            })
        );
        {
            let invalid_state = git
                .health_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert_eq!(invalid_state.status, GitSyncStatus::Error);
            assert_eq!(
                invalid_state.last_commit.as_deref(),
                Some(old_commit.as_str())
            );
        }

        fs::write(
            seed.join("accounts.json"),
            br#"{"items":[{"access_token":"git-pair-new-account-a","type":"pro","source_type":"web"},{"access_token":"git-pair-new-account-b","type":"pro","source_type":"web"}],"cumulative_total":2}
"#,
        )
        .expect("recovered remote accounts");
        fs::write(
            seed.join("auth_keys.json"),
            format!(
                "{{\"items\":[{{\"id\":\"git-pair-new-auth-a\",\"role\":\"admin\",\"key_hash\":\"{}\",\"enabled\":true}},{{\"id\":\"git-pair-new-auth-b\",\"role\":\"user\",\"key_hash\":\"{}\",\"enabled\":true}}]}}\n",
                token_hash("git-pair-new-secret-a"),
                token_hash("git-pair-new-secret-b")
            )
            .as_bytes(),
        )
        .expect("recovered remote auth");
        run_git(&seed, &["add", "--", "accounts.json", "auth_keys.json"]);
        run_git(
            &seed,
            &[
                "-c",
                "user.name=chatgpt2api-test",
                "-c",
                "user.email=chatgpt2api-test@example.invalid",
                "commit",
                "-m",
                "recovered account and auth snapshots",
            ],
        );
        run_git(&seed, &["push", "origin", "main"]);

        git.force_health_refresh_due_for_test(false);
        let _ = health_json_within(&state, Duration::from_millis(250)).await;
        tokio::time::timeout(Duration::from_secs(10), async {
            while git.health_refresh_running_for_test() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recovery Git refresh did not terminate");
        let recovered = health_json_within(&state, Duration::from_millis(250)).await;
        assert_eq!(
            recovered["storage"],
            json!({"backend": "git", "health": {"status": "healthy"}})
        );
        let recovered_cache = state
            .health_snapshot_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(recovered_cache.account_count, 2);
        assert_eq!(recovered_cache.auth_key_count, 2);
        assert_eq!(recovered["healthy"], true);

        drop(state);
        backend.close().await;
        fs::remove_dir_all(root).expect("Git pair transaction cleanup");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn git_health_refresh_rejects_candidate_when_local_mutation_wins_between_phases() {
        let (root, remote) = create_bare_git_storage_remote("git-health-phase-conflict");
        let backend = Arc::new(
            StorageBackend::connect_git(
                remote.to_str().expect("remote URL"),
                "",
                "main",
                "accounts.json",
                "auth_keys.json",
                &root.join("cache"),
            )
            .await
            .expect("Git backend"),
        );
        let accounts = backend.load_accounts().await.expect("empty accounts");
        backend
            .save_accounts(
                accounts.revision,
                vec![serde_json::json!({
                    "access_token":"git-phase-old-account",
                    "type":"pro",
                    "source_type":"web"
                })],
                1,
            )
            .await
            .expect("old Git account");
        let auth = backend.load_auth_keys().await.expect("empty auth");
        backend
            .save_auth_keys(
                auth.revision,
                vec![serde_json::json!({
                    "id":"git-phase-old-auth",
                    "role":"admin",
                    "key_hash":token_hash("git-phase-old-secret"),
                    "enabled":true
                })],
            )
            .await
            .expect("old Git auth");
        let state = crate::AppState::new_with_storage_backend(
            crate::AppConfig {
                version: "git-health-phase-conflict-test".to_owned(),
                auth_key: None,
                models: vec!["auto".to_owned()],
                upstream_base_url: None,
                upstream_auth: None,
                auth_keys_path: None,
                models_path: None,
                accounts_path: None,
                upstream_protocol: crate::UpstreamProtocol::ChatGpt,
            },
            backend.clone(),
            root.clone(),
        )
        .await
        .expect("Git app state");
        let git = match backend.as_ref() {
            StorageBackend::Git(git) => git.clone(),
            StorageBackend::Json(_) => panic!("expected Git backend"),
            StorageBackend::Database(_) => panic!("expected Git backend"),
        };

        let seed = root.join("seed");
        run_git(&seed, &["fetch", "origin", "main"]);
        run_git(&seed, &["reset", "--hard", "FETCH_HEAD"]);
        fs::write(
            seed.join("accounts.json"),
            br#"{"items":[{"access_token":"git-phase-remote-account","type":"pro","source_type":"web"}],"cumulative_total":1}
"#,
        )
        .expect("remote candidate accounts");
        fs::write(
            seed.join("auth_keys.json"),
            format!(
                "{{\"items\":[{{\"id\":\"git-phase-remote-auth\",\"role\":\"admin\",\"key_hash\":\"{}\",\"enabled\":true}}]}}\n",
                token_hash("git-phase-remote-secret")
            )
            .as_bytes(),
        )
        .expect("remote candidate auth");
        run_git(&seed, &["add", "--", "accounts.json", "auth_keys.json"]);
        run_git(
            &seed,
            &[
                "-c",
                "user.name=chatgpt2api-test",
                "-c",
                "user.email=chatgpt2api-test@example.invalid",
                "commit",
                "-m",
                "phase A candidate",
            ],
        );
        run_git(&seed, &["push", "origin", "main"]);

        let hook = GitHealthRefreshTestHook {
            started: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            starts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            fail: Arc::new(AtomicBool::new(false)),
            pause_before_prepare: Arc::new(AtomicBool::new(false)),
            after_fetch: Arc::new(tokio::sync::Notify::new()),
            release_after_fetch: Arc::new(tokio::sync::Notify::new()),
            pause_after_fetch: Arc::new(AtomicBool::new(false)),
            after_prepare: Arc::new(tokio::sync::Notify::new()),
            release_before_commit: Arc::new(tokio::sync::Notify::new()),
            pause_after_prepare: Arc::new(AtomicBool::new(true)),
        };
        git.set_health_refresh_test_hook(Some(hook.clone()));
        git.force_health_refresh_due_for_test(false);
        let _ = health_json_within(&state, Duration::from_millis(250)).await;
        tokio::time::timeout(Duration::from_secs(5), hook.after_prepare.notified())
            .await
            .expect("refresh did not finish Phase A");

        let current = backend
            .load_accounts()
            .await
            .expect("current account snapshot");
        let mutation = tokio::time::timeout(
            Duration::from_secs(5),
            backend.save_accounts(
                current.revision,
                vec![serde_json::json!({
                    "access_token":"git-phase-local-mutation",
                    "type":"pro",
                    "source_type":"web"
                })],
                2,
            ),
        )
        .await
        .expect("local mutation deadlocked with Phase A")
        .expect("local mutation failed");
        assert_eq!(
            mutation.records[0]["access_token"],
            "git-phase-local-mutation"
        );

        hook.release_before_commit.notify_one();
        tokio::time::timeout(Duration::from_secs(10), async {
            while git.health_refresh_running_for_test() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Phase B conflict did not terminate");
        let conflicted = health_json_within(&state, Duration::from_millis(250)).await;
        assert_eq!(
            conflicted["storage"],
            json!({
                "backend": "git",
                "health": {"status": "unhealthy", "error": "存储后端健康检查失败"},
            })
        );
        assert_eq!(
            git.health_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .status,
            GitSyncStatus::Error
        );
        assert_eq!(
            state.account_store.records()[0].token,
            "git-phase-old-account"
        );

        git.set_health_refresh_test_hook(None);
        git.force_health_refresh_due_for_test(false);
        let _ = health_json_within(&state, Duration::from_millis(250)).await;
        tokio::time::timeout(Duration::from_secs(10), async {
            while git.health_refresh_running_for_test() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reprepare did not terminate");
        let recovered = health_json_within(&state, Duration::from_millis(250)).await;
        assert_eq!(recovered["storage"]["health"]["status"], "healthy");
        assert_eq!(
            state.account_store.records()[0].token,
            "git-phase-local-mutation"
        );

        let shutdown_hook = GitHealthRefreshTestHook {
            started: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            starts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            fail: Arc::new(AtomicBool::new(false)),
            pause_before_prepare: Arc::new(AtomicBool::new(false)),
            after_fetch: Arc::new(tokio::sync::Notify::new()),
            release_after_fetch: Arc::new(tokio::sync::Notify::new()),
            pause_after_fetch: Arc::new(AtomicBool::new(false)),
            after_prepare: Arc::new(tokio::sync::Notify::new()),
            release_before_commit: Arc::new(tokio::sync::Notify::new()),
            pause_after_prepare: Arc::new(AtomicBool::new(true)),
        };
        git.set_health_refresh_test_hook(Some(shutdown_hook.clone()));
        git.force_health_refresh_due_for_test(false);
        let _ = health_json_within(&state, Duration::from_millis(250)).await;
        tokio::time::timeout(
            Duration::from_secs(5),
            shutdown_hook.after_prepare.notified(),
        )
        .await
        .expect("shutdown canary did not reach Phase B");
        drop(state);
        backend.close().await;
        assert!(
            git.health_candidate_ref
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_none()
        );
        fs::remove_dir_all(root).expect("Git Phase A/B conflict cleanup");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn health_snapshot_publish_gate_blocks_auth_account_and_reverse_observers_until_pair_is_visible()
     {
        let (root, remote) = create_bare_git_storage_remote("git-health-publish-gate");
        let backend = Arc::new(
            StorageBackend::connect_git(
                remote.to_str().expect("remote URL"),
                "",
                "main",
                "accounts.json",
                "auth_keys.json",
                &root.join("cache"),
            )
            .await
            .expect("Git backend"),
        );
        let accounts = backend.load_accounts().await.expect("empty accounts");
        backend
            .save_accounts(
                accounts.revision,
                vec![serde_json::json!({
                    "access_token":"git-publish-old-account",
                    "type":"pro",
                    "source_type":"web"
                })],
                1,
            )
            .await
            .expect("old Git account");
        let auth = backend.load_auth_keys().await.expect("empty auth");
        backend
            .save_auth_keys(
                auth.revision,
                vec![serde_json::json!({
                    "id":"git-publish-old-auth",
                    "role":"admin",
                    "key_hash":token_hash("git-publish-old-secret"),
                    "enabled":true
                })],
            )
            .await
            .expect("old Git auth");
        let state = crate::AppState::new_with_storage_backend(
            crate::AppConfig {
                version: "git-health-publish-gate-test".to_owned(),
                auth_key: None,
                models: vec!["auto".to_owned()],
                upstream_base_url: None,
                upstream_auth: None,
                auth_keys_path: None,
                models_path: None,
                accounts_path: None,
                upstream_protocol: crate::UpstreamProtocol::ChatGpt,
            },
            backend.clone(),
            root.clone(),
        )
        .await
        .expect("Git app state");
        let git = match backend.as_ref() {
            StorageBackend::Git(git) => git.clone(),
            StorageBackend::Json(_) => panic!("expected Git backend"),
            StorageBackend::Database(_) => panic!("expected Git backend"),
        };
        let hook = crate::HealthSnapshotPublishTestHook {
            after_accounts: Arc::new(tokio::sync::Notify::new()),
            release_before_auth: Arc::new(tokio::sync::Notify::new()),
            pause_between: Arc::new(AtomicBool::new(true)),
        };
        *state
            .health_snapshot_publish_test_hook
            .write()
            .expect("publish hook lock") = Some(hook.clone());

        let seed = root.join("seed");
        run_git(&seed, &["fetch", "origin", "main"]);
        run_git(&seed, &["reset", "--hard", "FETCH_HEAD"]);
        fs::write(
            seed.join("accounts.json"),
            br#"{"items":[{"access_token":"git-publish-new-account-a","type":"pro","source_type":"web"},{"access_token":"git-publish-new-account-b","type":"pro","source_type":"web"}],"cumulative_total":2}
"#,
        )
        .expect("new remote accounts");
        fs::write(
            seed.join("auth_keys.json"),
            format!(
                "{{\"items\":[{{\"id\":\"git-publish-new-auth-a\",\"role\":\"admin\",\"key_hash\":\"{}\",\"enabled\":true}},{{\"id\":\"git-publish-new-auth-b\",\"role\":\"user\",\"key_hash\":\"{}\",\"enabled\":true}}]}}\n",
                token_hash("git-publish-new-secret-a"),
                token_hash("git-publish-new-secret-b")
            )
            .as_bytes(),
        )
        .expect("new remote auth");
        run_git(&seed, &["add", "--", "accounts.json", "auth_keys.json"]);
        run_git(
            &seed,
            &[
                "-c",
                "user.name=chatgpt2api-test",
                "-c",
                "user.email=chatgpt2api-test@example.invalid",
                "commit",
                "-m",
                "publish pair gate",
            ],
        );
        run_git(&seed, &["push", "origin", "main"]);

        git.force_health_refresh_due_for_test(false);
        let started = hook.after_accounts.notified();
        let _ = health_json_within(&state, Duration::from_millis(250)).await;
        tokio::time::timeout(Duration::from_secs(10), started)
            .await
            .expect("publisher did not reach the between-snapshot barrier");

        let mut background_refresh = tokio::spawn({
            let state = state.clone();
            async move {
                crate::refresh_accounts_now(&state, &["git-publish-new-account-a".to_owned()]).await
            }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut background_refresh)
                .await
                .is_err(),
            "background account refresh crossed the Phase B account gate"
        );

        let mut admin_request = tokio::spawn({
            let state = state.clone();
            async move {
                state
                    .router()
                    .oneshot(
                        Request::builder()
                            .uri("/api/accounts")
                            .header(header::AUTHORIZATION, "Bearer git-publish-old-secret")
                            .body(Body::empty())
                            .expect("accounts observer request"),
                    )
                    .await
                    .expect("accounts observer response")
            }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut admin_request)
                .await
                .is_err(),
            "auth-then-account request crossed the publish barrier"
        );
        let reverse_response = tokio::time::timeout(
            Duration::from_millis(100),
            state.router().oneshot(
                Request::builder()
                    .uri("/health?format=json")
                    .body(Body::empty())
                    .expect("reverse observer request"),
            ),
        )
        .await
        .expect("health cache blocked by pair publish barrier")
        .expect("reverse observer response");
        assert_eq!(reverse_response.status(), StatusCode::OK);
        let reverse_payload: Value = serde_json::from_slice(
            &reverse_response
                .into_body()
                .collect()
                .await
                .expect("reverse observer body")
                .to_bytes(),
        )
        .expect("reverse observer JSON");
        assert_eq!(
            reverse_payload["storage"],
            json!({"backend": "git", "health": {"status": "healthy"}})
        );
        assert_eq!(reverse_payload["healthy"], true);

        hook.release_before_auth.notify_one();
        let admin_response = tokio::time::timeout(Duration::from_secs(2), admin_request)
            .await
            .expect("accounts observer remained blocked after publish")
            .expect("accounts observer task");
        assert_eq!(admin_response.status(), StatusCode::UNAUTHORIZED);

        tokio::time::timeout(Duration::from_secs(10), async {
            while git.health_refresh_running_for_test() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("publish refresh did not terminate");
        let background_result = tokio::time::timeout(Duration::from_secs(2), background_refresh)
            .await
            .expect("background account refresh remained blocked after Phase B")
            .expect("background account refresh task");
        assert!(background_result.is_ok());
        assert_eq!(state.account_store.records()[0].status, "异常");
        let published = health_json_within(&state, Duration::from_millis(250)).await;
        assert_eq!(
            published["storage"],
            json!({"backend": "git", "health": {"status": "healthy"}})
        );
        let published_cache = state
            .health_snapshot_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(published_cache.account_count, 2);
        assert_eq!(published_cache.auth_key_count, 2);
        assert_eq!(published["accounts"]["abnormal"], 1);
        assert_eq!(published["healthy"], true);
        *state
            .health_snapshot_publish_test_hook
            .write()
            .expect("publish hook cleanup") = None;
        drop(state);
        backend.close().await;
        fs::remove_dir_all(root).expect("Git publish gate cleanup");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn git_health_refresh_generation_first_lock_order_cannot_deadlock_with_request_reader() {
        let (root, remote) = create_bare_git_storage_remote("git-health-lock-order");
        let backend = Arc::new(
            StorageBackend::connect_git(
                remote.to_str().expect("remote URL"),
                "",
                "main",
                "accounts.json",
                "auth_keys.json",
                &root.join("cache"),
            )
            .await
            .expect("Git backend"),
        );
        let state = crate::AppState::new_with_storage_backend(
            crate::AppConfig {
                version: "git-health-lock-order-test".to_owned(),
                auth_key: None,
                models: vec!["auto".to_owned()],
                upstream_base_url: None,
                upstream_auth: None,
                auth_keys_path: None,
                models_path: None,
                accounts_path: None,
                upstream_protocol: crate::UpstreamProtocol::ChatGpt,
            },
            backend.clone(),
            root.clone(),
        )
        .await
        .expect("Git app state");
        let git = match backend.as_ref() {
            StorageBackend::Git(git) => git.clone(),
            StorageBackend::Json(_) => panic!("expected Git backend"),
            StorageBackend::Database(_) => panic!("expected Git backend"),
        };
        let hook = crate::HealthRefreshCoordinatorTestHook {
            before_generation: Arc::new(tokio::sync::Notify::new()),
        };
        *state
            .health_refresh_coordinator_test_hook
            .write()
            .expect("coordinator hook lock") = Some(hook.clone());

        let generation_read = state.health_snapshot_gate.clone().read_owned().await;
        git.force_health_refresh_due_for_test(false);
        git.start_health_refresh_if_due();
        tokio::time::timeout(Duration::from_secs(2), hook.before_generation.notified())
            .await
            .expect("refresh worker did not reach generation gate");

        let account_guard = tokio::time::timeout(
            Duration::from_secs(1),
            state.account_store.lock_reload_gate(),
        )
        .await
        .expect(
            "refresh acquired account reload gate before generation write; lock order regressed",
        );
        drop(account_guard);
        drop(generation_read);

        tokio::time::timeout(Duration::from_secs(10), async {
            while git.health_refresh_running_for_test() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("generation-first refresh deadlocked or leaked");
        *state
            .health_refresh_coordinator_test_hook
            .write()
            .expect("coordinator hook cleanup") = None;
        drop(state);
        backend.close().await;
        fs::remove_dir_all(root).expect("Git lock-order cleanup");
    }

    async fn open_sqlite_test_pool(path: &Path) -> AnyPool {
        install_default_drivers();
        AnyPoolOptions::new()
            .max_connections(1)
            .connect(&database_connect_url(
                &sqlite_url(path),
                DatabaseKind::Sqlite,
            ))
            .await
            .expect("SQLite test pool")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quiescent_close_drains_connection_returned_after_first_close_pass() {
        let path = database_path("quiescent-close-raced-return");
        install_default_drivers();
        let release_started = Arc::new(tokio::sync::Notify::new());
        let allow_release = Arc::new(tokio::sync::Notify::new());
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .after_release({
                let release_started = release_started.clone();
                let allow_release = allow_release.clone();
                move |_, _| {
                    let release_started = release_started.clone();
                    let allow_release = allow_release.clone();
                    Box::pin(async move {
                        release_started.notify_one();
                        allow_release.notified().await;
                        Ok(true)
                    })
                }
            })
            .connect(&database_connect_url(
                &sqlite_url(&path),
                DatabaseKind::Sqlite,
            ))
            .await
            .expect("single-connection SQLite pool");
        let mut connection = pool.acquire().await.expect("checked-out connection");
        let return_future = connection.return_to_pool();
        drop(connection);
        let return_task = tokio::spawn(return_future);
        tokio::time::timeout(Duration::from_secs(2), release_started.notified())
            .await
            .expect("connection return did not reach controlled release");

        let first_close_pool = pool.clone();
        let first_close = tokio::spawn(async move {
            first_close_pool.close().await;
        });
        tokio::time::timeout(Duration::from_secs(2), pool.close_event())
            .await
            .expect("first close did not mark the pool closed");
        allow_release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), return_task)
            .await
            .expect("controlled connection return timed out")
            .expect("controlled connection return task");
        tokio::time::timeout(Duration::from_secs(2), first_close)
            .await
            .expect("first close pass timed out")
            .expect("first close task");
        assert_eq!(pool.size(), 1, "first close must expose the SQLx race");
        assert_eq!(pool.num_idle(), 1);

        tokio::time::timeout(Duration::from_secs(2), quiescent_close_database_pool(&pool))
            .await
            .expect("quiescent close timed out");
        assert_eq!(pool.size(), 0);
        fs::remove_file(&path).expect("quiescent close retained SQLite handle");
        drop(pool);
        fs::remove_dir(path.parent().expect("database parent"))
            .expect("quiescent close test root cleanup");
    }

    #[test]
    fn server_schema_lock_keys_are_database_scoped_and_bounded() {
        let postgres_key = postgres_schema_lock_key("shared_database");
        assert_eq!(postgres_key, postgres_schema_lock_key("shared_database"));
        assert_ne!(postgres_key, postgres_schema_lock_key("other_database"));

        let mysql_name = mysql_schema_lock_name("shared_database");
        assert_eq!(mysql_name, mysql_schema_lock_name("shared_database"));
        assert_ne!(mysql_name, mysql_schema_lock_name("other_database"));
        assert!(mysql_name.is_ascii());
        assert!(mysql_name.len() <= 64);
    }

    #[tokio::test]
    async fn database_text_decoder_preserves_large_utf8_and_rejects_invalid_blob() {
        let path = database_path("text-decoder");
        let pool = open_sqlite_test_pool(&path).await;
        let large = format!("prefix-{}-suffix", "数据库文本边界".repeat(12_000));
        assert!(large.len() > 64 * 1024);

        let text_row = sqlx::query("SELECT ? AS value")
            .bind(&large)
            .fetch_one(&pool)
            .await
            .expect("text row");
        assert_eq!(
            database_row_text(&text_row, "value", DatabaseKind::Postgres, "test.text")
                .expect("PostgreSQL text decoding"),
            large
        );

        let blob_row = sqlx::query("SELECT ? AS value")
            .bind(large.as_bytes())
            .fetch_one(&pool)
            .await
            .expect("blob row");
        assert_eq!(
            database_row_text(&blob_row, "value", DatabaseKind::MySql, "test.blob")
                .expect("MySQL LONGTEXT blob decoding"),
            large
        );
        assert_eq!(
            database_row_text(
                &blob_row,
                "value",
                DatabaseKind::Postgres,
                "test.postgres_blob"
            ),
            Err(StorageError::Invalid),
            "only the MySQL/Any LONGTEXT compatibility boundary accepts blobs"
        );

        let invalid_row = sqlx::query("SELECT ? AS value")
            .bind(vec![0xff_u8, 0xfe, 0x80])
            .fetch_one(&pool)
            .await
            .expect("invalid UTF-8 blob row");
        assert_eq!(
            database_row_text(
                &invalid_row,
                "value",
                DatabaseKind::MySql,
                "test.invalid_utf8"
            ),
            Err(StorageError::Invalid)
        );

        quiescent_close_database_pool(&pool).await;
        fs::remove_file(path).expect("text decoder database cleanup");
    }

    async fn create_legacy_accounts_table(path: &Path, records: &[Value]) {
        let pool = open_sqlite_test_pool(path).await;
        pool.execute(
            "CREATE TABLE accounts (id INTEGER PRIMARY KEY, access_token VARCHAR(2048) NOT NULL, data TEXT NOT NULL)",
        )
        .await
        .expect("legacy accounts table");
        pool.execute("CREATE UNIQUE INDEX ix_accounts_access_token ON accounts(access_token)")
            .await
            .expect("legacy raw-token index");
        for record in records {
            let token = record
                .get("access_token")
                .and_then(Value::as_str)
                .expect("legacy token");
            sqlx::query("INSERT INTO accounts(access_token, data) VALUES (?, ?)")
                .bind(token)
                .bind(serde_json::to_string(record).expect("legacy JSON"))
                .execute(&pool)
                .await
                .expect("legacy account row");
        }
        quiescent_close_database_pool(&pool).await;
    }

    #[tokio::test]
    async fn schema_initialization_failure_releases_sqlite_file_immediately() {
        let path = database_path("schema-failure-quiescent-close");
        create_legacy_accounts_table(
            &path,
            &[
                serde_json::json!({"access_token":" duplicated ","name":"spaced"}),
                serde_json::json!({"access_token":"duplicated","name":"plain"}),
            ],
        )
        .await;

        assert!(matches!(
            StorageBackend::connect_database(&sqlite_url(&path)).await,
            Err(StorageError::Invalid)
        ));
        fs::remove_file(&path).expect("failed schema initialization retained SQLite handle");
        fs::remove_dir(path.parent().expect("database parent"))
            .expect("schema failure test root cleanup");
    }

    #[tokio::test]
    async fn database_migrates_legacy_token_schema_atomically_and_idempotently() {
        let path = database_path("legacy-migration");
        let token = format!("legacy-token-{}", "x".repeat(2050));
        let record = serde_json::json!({"access_token":token,"name":"legacy"});
        create_legacy_accounts_table(&path, std::slice::from_ref(&record)).await;

        let first = StorageBackend::connect_database(&sqlite_url(&path))
            .await
            .expect("legacy migration");
        assert_eq!(
            first
                .load_accounts()
                .await
                .expect("migrated accounts")
                .records,
            vec![record.clone()]
        );
        let StorageBackend::Database(first_database) = &first else {
            panic!("expected database backend");
        };
        let row = sqlx::query(
            "SELECT id, access_token, access_token_hash, data FROM accounts ORDER BY id",
        )
        .fetch_one(&first_database.pool)
        .await
        .expect("migrated row");
        assert_eq!(row.try_get::<i64, _>("id").expect("row id"), 1);
        assert_eq!(
            row.try_get::<String, _>("access_token")
                .expect("stored token"),
            token
        );
        assert_eq!(
            row.try_get::<String, _>("access_token_hash")
                .expect("stored token hash"),
            token_hash(&token)
        );
        let columns = sqlx::query("PRAGMA table_info(accounts)")
            .fetch_all(&first_database.pool)
            .await
            .expect("accounts columns");
        let token_column = columns
            .iter()
            .find(|column| {
                column.try_get::<String, _>("name").ok().as_deref() == Some("access_token")
            })
            .expect("access_token column");
        assert_eq!(
            token_column
                .try_get::<String, _>("type")
                .expect("token column type")
                .to_ascii_uppercase(),
            "TEXT"
        );
        let hash_column = columns
            .iter()
            .find(|column| {
                column.try_get::<String, _>("name").ok().as_deref() == Some("access_token_hash")
            })
            .expect("access_token_hash column");
        assert_eq!(
            hash_column.try_get::<i64, _>("notnull").expect("not-null"),
            1
        );
        let indexes = sqlx::query(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'accounts' ORDER BY name",
        )
        .fetch_all(&first_database.pool)
        .await
        .expect("accounts indexes")
        .into_iter()
        .map(|row| row.try_get::<String, _>("name").expect("index name"))
        .collect::<Vec<_>>();
        assert!(
            indexes
                .iter()
                .any(|name| name == "ux_accounts_access_token_hash")
        );
        assert!(
            !indexes
                .iter()
                .any(|name| name == "ix_accounts_access_token")
        );
        tokio::time::timeout(Duration::from_secs(2), first.close())
            .await
            .expect("first migrated database pool did not quiesce");
        assert!(first_database.pool.is_closed());
        drop(indexes);
        drop(columns);
        drop(row);
        drop(first);

        let second = StorageBackend::connect_database(&sqlite_url(&path))
            .await
            .expect("idempotent migration");
        let StorageBackend::Database(second_database) = &second else {
            panic!("expected database backend");
        };
        let second_row = sqlx::query(
            "SELECT id, access_token, access_token_hash, data FROM accounts ORDER BY id",
        )
        .fetch_one(&second_database.pool)
        .await
        .expect("idempotent row");
        assert_eq!(second_row.try_get::<i64, _>("id").expect("row id"), 1);
        assert_eq!(
            second_row
                .try_get::<String, _>("access_token_hash")
                .expect("token hash"),
            token_hash(&token)
        );
        tokio::time::timeout(Duration::from_secs(2), second.close())
            .await
            .expect("second migrated database pool did not quiesce");
        assert!(second_database.pool.is_closed());
        drop(second_row);
        drop(second);
        fs::remove_dir_all(path.parent().expect("database parent")).expect("storage cleanup");
    }

    #[tokio::test]
    async fn database_rejects_corrupt_hash_without_rewriting_row() {
        let path = database_path("corrupt-hash");
        let backend = StorageBackend::connect_database(&sqlite_url(&path))
            .await
            .expect("database backend");
        let initial = backend.load_accounts().await.expect("initial accounts");
        backend
            .save_accounts(
                initial.revision,
                vec![serde_json::json!({"access_token":"token-a","name":"A"})],
                1,
            )
            .await
            .expect("seed account");
        backend.close().await;
        let pool = open_sqlite_test_pool(&path).await;
        sqlx::query("UPDATE accounts SET access_token_hash = ? WHERE access_token = ?")
            .bind("0".repeat(64))
            .bind("token-a")
            .execute(&pool)
            .await
            .expect("corrupt stored hash");
        quiescent_close_database_pool(&pool).await;

        assert!(matches!(
            StorageBackend::connect_database(&sqlite_url(&path)).await,
            Err(StorageError::Invalid)
        ));
        let pool = open_sqlite_test_pool(&path).await;
        let row = sqlx::query("SELECT access_token, access_token_hash, data FROM accounts")
            .fetch_one(&pool)
            .await
            .expect("corrupt row remains");
        assert_eq!(
            row.try_get::<String, _>("access_token_hash")
                .expect("corrupt hash"),
            "0".repeat(64)
        );
        assert_eq!(
            row.try_get::<String, _>("access_token").expect("token"),
            "token-a"
        );
        quiescent_close_database_pool(&pool).await;
        fs::remove_dir_all(path.parent().expect("database parent")).expect("storage cleanup");
    }

    #[tokio::test]
    async fn database_rejects_normalized_legacy_duplicates_without_rewriting_schema_or_rows() {
        let path = database_path("legacy-duplicates");
        create_legacy_accounts_table(
            &path,
            &[
                serde_json::json!({"access_token":" token ","name":"spaced"}),
                serde_json::json!({"access_token":"token","name":"plain"}),
            ],
        )
        .await;

        assert!(matches!(
            StorageBackend::connect_database(&sqlite_url(&path)).await,
            Err(StorageError::Invalid)
        ));
        let pool = open_sqlite_test_pool(&path).await;
        let rows = sqlx::query("SELECT access_token FROM accounts ORDER BY id")
            .fetch_all(&pool)
            .await
            .expect("legacy rows remain")
            .into_iter()
            .map(|row| {
                row.try_get::<String, _>("access_token")
                    .expect("legacy token")
            })
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![" token ", "token"]);
        let columns = sqlx::query("PRAGMA table_info(accounts)")
            .fetch_all(&pool)
            .await
            .expect("legacy columns");
        assert!(!columns.iter().any(|column| {
            column.try_get::<String, _>("name").ok().as_deref() == Some("access_token_hash")
        }));
        quiescent_close_database_pool(&pool).await;
        fs::remove_dir_all(path.parent().expect("database parent")).expect("storage cleanup");
    }

    #[tokio::test]
    async fn concurrent_legacy_database_startup_is_serialized() {
        let path = database_path("concurrent-legacy-startup");
        let record = serde_json::json!({"access_token":"legacy-token","name":"legacy"});
        create_legacy_accounts_table(&path, std::slice::from_ref(&record)).await;
        let url = sqlite_url(&path);
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let first_barrier = barrier.clone();
        let first_url = url.clone();
        let first = tokio::spawn(async move {
            first_barrier.wait().await;
            StorageBackend::connect_database(&first_url).await
        });
        let second_barrier = barrier.clone();
        let second_url = url.clone();
        let second = tokio::spawn(async move {
            second_barrier.wait().await;
            StorageBackend::connect_database(&second_url).await
        });
        barrier.wait().await;
        let (first, second) = tokio::join!(first, second);
        let first = first.expect("first startup task").expect("first startup");
        let second = second
            .expect("second startup task")
            .expect("second startup");
        assert_eq!(
            first.load_accounts().await.expect("first accounts").records,
            vec![record.clone()]
        );
        assert_eq!(
            second
                .load_accounts()
                .await
                .expect("second accounts")
                .records,
            vec![record]
        );
        first.close().await;
        second.close().await;
        fs::remove_dir_all(path.parent().expect("database parent")).expect("storage cleanup");
    }

    #[tokio::test]
    async fn concurrent_database_cas_has_exactly_one_winner_for_each_collection() {
        let path = database_path("concurrent-cas");
        let url = sqlite_url(&path);
        let first = Arc::new(
            StorageBackend::connect_database(&url)
                .await
                .expect("first database backend"),
        );
        let second = Arc::new(
            StorageBackend::connect_database(&url)
                .await
                .expect("second database backend"),
        );

        for round in 0..16_u64 {
            let expected = first.load_accounts().await.expect("account revision");
            let barrier = Arc::new(tokio::sync::Barrier::new(3));
            let first_writer = first.clone();
            let first_barrier = barrier.clone();
            let first_revision = expected.revision;
            let left = tokio::spawn(async move {
                first_barrier.wait().await;
                first_writer
                    .save_accounts(
                        first_revision,
                        vec![serde_json::json!({
                            "access_token":"shared-token",
                            "writer":"left",
                            "round":round
                        })],
                        round + 1,
                    )
                    .await
            });
            let second_writer = second.clone();
            let second_barrier = barrier.clone();
            let second_revision = expected.revision;
            let right = tokio::spawn(async move {
                second_barrier.wait().await;
                second_writer
                    .save_accounts(
                        second_revision,
                        vec![serde_json::json!({
                            "access_token":"shared-token",
                            "writer":"right",
                            "round":round
                        })],
                        round + 1,
                    )
                    .await
            });
            barrier.wait().await;
            let (left, right) = tokio::join!(left, right);
            let results = [left.expect("left writer"), right.expect("right writer")];
            assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
            assert_eq!(
                results
                    .iter()
                    .filter(|result| matches!(result, Err(StorageError::Conflict)))
                    .count(),
                1
            );
            let persisted = first.load_accounts().await.expect("persisted accounts");
            assert_eq!(persisted.records.len(), 1);
            assert_eq!(persisted.records[0]["round"], round);
            assert!(matches!(
                persisted.records[0]["writer"].as_str(),
                Some("left" | "right")
            ));
            assert_eq!(persisted.cumulative_total, Some(round + 1));
        }

        let expected = first.load_auth_keys().await.expect("auth revision");
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let first_writer = first.clone();
        let first_barrier = barrier.clone();
        let first_revision = expected.revision;
        let left = tokio::spawn(async move {
            first_barrier.wait().await;
            first_writer
                .save_auth_keys(
                    first_revision,
                    vec![serde_json::json!({
                        "id":"shared-key",
                        "role":"user",
                        "key_hash":"a".repeat(64),
                        "enabled":true,
                        "writer":"left"
                    })],
                )
                .await
        });
        let second_writer = second.clone();
        let second_barrier = barrier.clone();
        let second_revision = expected.revision;
        let right = tokio::spawn(async move {
            second_barrier.wait().await;
            second_writer
                .save_auth_keys(
                    second_revision,
                    vec![serde_json::json!({
                        "id":"shared-key",
                        "role":"user",
                        "key_hash":"b".repeat(64),
                        "enabled":true,
                        "writer":"right"
                    })],
                )
                .await
        });
        barrier.wait().await;
        let (left, right) = tokio::join!(left, right);
        let results = [
            left.expect("left auth writer"),
            right.expect("right auth writer"),
        ];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(StorageError::Conflict)))
                .count(),
            1
        );
        let persisted = first.load_auth_keys().await.expect("persisted auth keys");
        assert_eq!(persisted.records.len(), 1);
        assert!(matches!(
            persisted.records[0]["writer"].as_str(),
            Some("left" | "right")
        ));

        first.close().await;
        second.close().await;
        fs::remove_dir_all(path.parent().expect("database parent")).expect("storage cleanup");
    }

    #[tokio::test]
    async fn account_import_updates_cumulative_total_only_for_new_identities() {
        let db_path = database_path("import-cumulative-database");
        let backend = Arc::new(
            StorageBackend::connect_database(&sqlite_url(&db_path))
                .await
                .expect("database backend"),
        );
        let initial = backend.load_accounts().await.expect("initial accounts");
        backend
            .save_accounts(
                initial.revision,
                vec![serde_json::json!({
                    "access_token":"token-a",
                    "status":"禁用",
                    "type":"pro",
                    "source_type":"web"
                })],
                7,
            )
            .await
            .expect("seed database accounts");
        let store = crate::account_pool::AccountStore::load_backend(backend.clone())
            .await
            .expect("database account store");
        assert_eq!(
            store
                .merge_import_records(vec![
                    serde_json::json!({
                        "access_token":"token-a",
                        "status":"正常",
                        "type":"pro",
                        "source_type":"web"
                    }),
                    serde_json::json!({
                        "access_token":"token-b",
                        "status":"正常",
                        "type":"plus",
                        "source_type":"web"
                    }),
                ])
                .await
                .expect("database import"),
            (1, 1)
        );
        let persisted = backend.load_accounts().await.expect("database persisted");
        assert_eq!(persisted.cumulative_total, Some(8));
        assert_eq!(persisted.records.len(), 2);
        assert_eq!(persisted.records[0]["status"], "禁用");
        store
            .update_refreshed_account(
                "token-b",
                serde_json::json!({"access_token":"token-b","status":"异常"}),
            )
            .await
            .expect("status update");
        assert_eq!(
            backend
                .load_accounts()
                .await
                .expect("database after update")
                .cumulative_total,
            Some(8)
        );
        assert_eq!(
            store
                .merge_import_records(vec![serde_json::json!({
                    "access_token":"token-b",
                    "status":"正常",
                    "type":"plus",
                    "source_type":"web"
                })])
                .await
                .expect("duplicate import"),
            (0, 1)
        );
        assert_eq!(
            backend
                .load_accounts()
                .await
                .expect("database after duplicate")
                .cumulative_total,
            Some(8)
        );
        drop(store);
        backend.close().await;
        fs::remove_dir_all(db_path.parent().expect("database parent")).expect("database cleanup");

        let json_root = database_path("import-cumulative-json");
        let json_root = json_root.parent().expect("JSON parent").to_owned();
        let accounts_path = json_root.join("accounts.json");
        fs::write(
            &accounts_path,
            serde_json::to_vec(&serde_json::json!({
                "items":[{
                    "access_token":"json-token-a",
                    "status":"禁用",
                    "type":"pro",
                    "source_type":"web"
                }],
                "cumulative_total":11
            }))
            .expect("JSON snapshot"),
        )
        .expect("write JSON snapshot");
        let store = crate::account_pool::AccountStore::load(Some(&accounts_path))
            .expect("JSON account store");
        assert_eq!(
            store
                .merge_import_records(vec![serde_json::json!({
                    "access_token":"json-token-b",
                    "status":"正常",
                    "type":"team",
                    "source_type":"web"
                })])
                .await
                .expect("JSON import"),
            (1, 0)
        );
        let document: Value =
            serde_json::from_slice(&fs::read(&accounts_path).expect("read JSON snapshot"))
                .expect("parse JSON snapshot");
        assert_eq!(document["cumulative_total"], 12);
        assert_eq!(document["items"].as_array().expect("JSON items").len(), 2);
        drop(store);
        fs::remove_dir_all(json_root).expect("JSON cleanup");
    }

    #[tokio::test]
    async fn concurrent_account_import_retries_cas_without_losing_records_or_cumulative_total() {
        let path = database_path("concurrent-import-cumulative");
        let url = sqlite_url(&path);
        let first_backend = Arc::new(
            StorageBackend::connect_database(&url)
                .await
                .expect("first backend"),
        );
        let initial = first_backend
            .load_accounts()
            .await
            .expect("initial accounts");
        first_backend
            .save_accounts(
                initial.revision,
                vec![serde_json::json!({
                    "access_token":"token-a",
                    "status":"正常",
                    "type":"pro",
                    "source_type":"web"
                })],
                20,
            )
            .await
            .expect("seed accounts");
        let second_backend = Arc::new(
            StorageBackend::connect_database(&url)
                .await
                .expect("second backend"),
        );
        let first_store = crate::account_pool::AccountStore::load_backend(first_backend.clone())
            .await
            .expect("first store");
        let second_store = crate::account_pool::AccountStore::load_backend(second_backend.clone())
            .await
            .expect("second store");
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let first_barrier = barrier.clone();
        let first = tokio::spawn(async move {
            first_barrier.wait().await;
            first_store
                .merge_import_records(vec![serde_json::json!({
                    "access_token":"token-b",
                    "status":"正常",
                    "type":"plus",
                    "source_type":"web"
                })])
                .await
        });
        let second_barrier = barrier.clone();
        let second = tokio::spawn(async move {
            second_barrier.wait().await;
            second_store
                .merge_import_records(vec![serde_json::json!({
                    "access_token":"token-c",
                    "status":"正常",
                    "type":"team",
                    "source_type":"web"
                })])
                .await
        });
        barrier.wait().await;
        let (first, second) = tokio::join!(first, second);
        assert_eq!(
            first.expect("first import task").expect("first import"),
            (1, 0)
        );
        assert_eq!(
            second.expect("second import task").expect("second import"),
            (1, 0)
        );
        let persisted = first_backend
            .load_accounts()
            .await
            .expect("persisted accounts");
        assert_eq!(persisted.cumulative_total, Some(22));
        let tokens = persisted
            .records
            .iter()
            .map(|record| record["access_token"].as_str().expect("token"))
            .collect::<HashSet<_>>();
        assert_eq!(tokens, HashSet::from(["token-a", "token-b", "token-c"]));
        first_backend.close().await;
        second_backend.close().await;
        fs::remove_dir_all(path.parent().expect("database parent")).expect("storage cleanup");
    }

    #[tokio::test]
    async fn database_backend_owns_accounts_auth_cumulative_cas_and_health() {
        let path = database_path("sqlite-contract");
        let url = sqlite_url(&path);
        let first = StorageBackend::connect_database(&url)
            .await
            .expect("first database backend");
        let second = StorageBackend::connect_database(&url)
            .await
            .expect("second database backend");

        let initial = first.load_accounts().await.expect("initial accounts");
        assert!(initial.records.is_empty());
        assert_eq!(initial.cumulative_total, None);
        let saved = first
            .save_accounts(
                initial.revision,
                vec![serde_json::json!({"access_token":"token-a","status":"正常"})],
                7,
            )
            .await
            .expect("save accounts");
        assert_eq!(saved.cumulative_total, Some(7));
        assert_eq!(
            second.load_accounts().await.expect("reload accounts"),
            saved
        );

        let stale = second.load_accounts().await.expect("stale base");
        first
            .save_accounts(
                saved.revision,
                vec![serde_json::json!({"access_token":"token-a","status":"禁用"})],
                7,
            )
            .await
            .expect("newer writer");
        assert_eq!(
            second.save_accounts(stale.revision, Vec::new(), 7).await,
            Err(StorageError::Conflict)
        );

        let auth = first.load_auth_keys().await.expect("initial auth");
        let auth = first
            .save_auth_keys(
                auth.revision,
                vec![serde_json::json!({
                    "id":"key-a",
                    "role":"admin",
                    "key_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "enabled":true
                })],
            )
            .await
            .expect("save auth");
        assert_eq!(second.load_auth_keys().await.expect("reload auth"), auth);

        let (_, _, health) = first
            .load_health_snapshots()
            .await
            .expect("database health snapshots");
        assert_eq!(health["status"], "healthy");
        assert_eq!(health["backend"], "database");
        assert_eq!(health["account_count"], 1);
        assert_eq!(health["auth_key_count"], 1);
        let info = first.info();
        assert_eq!(info["type"], "database");
        assert_eq!(info["db_type"], "sqlite");

        first.close().await;
        second.close().await;
        fs::remove_dir_all(path.parent().expect("database parent")).expect("storage cleanup");
    }

    #[tokio::test]
    async fn app_state_uses_database_backend_for_real_auth_accounts_mutation_and_health() {
        let path = database_path("app-state");
        let backend = Arc::new(
            StorageBackend::connect_database(&sqlite_url(&path))
                .await
                .expect("database backend"),
        );
        let accounts = backend.load_accounts().await.expect("empty accounts");
        backend
            .save_accounts(
                accounts.revision,
                vec![serde_json::json!({
                    "access_token":"database-token",
                    "status":"正常",
                    "type":"pro",
                    "source_type":"web"
                })],
                4,
            )
            .await
            .expect("seed accounts");
        let raw_key = "database-admin-secret";
        let auth = backend.load_auth_keys().await.expect("empty auth");
        backend
            .save_auth_keys(
                auth.revision,
                vec![serde_json::json!({
                    "id":"database-admin",
                    "role":"admin",
                    "key_hash":token_hash(raw_key),
                    "enabled":true
                })],
            )
            .await
            .expect("seed auth");

        let state = crate::AppState::new_with_storage_backend(
            crate::AppConfig {
                version: "database-storage-test".to_owned(),
                auth_key: None,
                models: vec!["auto".to_owned()],
                upstream_base_url: None,
                upstream_auth: None,
                auth_keys_path: None,
                models_path: None,
                accounts_path: None,
                upstream_protocol: crate::UpstreamProtocol::ChatGpt,
            },
            backend.clone(),
            path.parent().expect("database parent").to_owned(),
        )
        .await
        .expect("database app state");
        let request = Request::builder()
            .method("POST")
            .uri("/api/accounts/update")
            .header(header::AUTHORIZATION, format!("Bearer {raw_key}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"access_token":"database-token","status":"禁用"}"#,
            ))
            .expect("account update request");
        let response = state
            .router()
            .oneshot(request)
            .await
            .expect("account update");
        assert_eq!(response.status(), StatusCode::OK);
        let persisted = backend.load_accounts().await.expect("persisted accounts");
        assert_eq!(persisted.records[0]["status"], "禁用");
        assert_eq!(persisted.cumulative_total, Some(4));

        let request = Request::builder()
            .uri("/health?format=json")
            .body(Body::empty())
            .expect("health request");
        let response = state
            .router()
            .oneshot(request)
            .await
            .expect("health response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload: Value = serde_json::from_slice(
            &response
                .into_body()
                .collect()
                .await
                .expect("health body")
                .to_bytes(),
        )
        .expect("health JSON");
        assert_eq!(
            payload["storage"],
            json!({"backend": "database", "health": {"status": "healthy"}})
        );

        let request = Request::builder()
            .uri("/api/storage/info")
            .header(header::AUTHORIZATION, format!("Bearer {raw_key}"))
            .body(Body::empty())
            .expect("storage info request");
        let response = state
            .router()
            .oneshot(request)
            .await
            .expect("storage info response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload: Value = serde_json::from_slice(
            &response
                .into_body()
                .collect()
                .await
                .expect("storage info body")
                .to_bytes(),
        )
        .expect("storage info JSON");
        assert_eq!(
            payload,
            json!({
                "backend": {"type": "database", "db_type": "sqlite"},
                "health": {"status": "healthy"},
            })
        );

        drop(state);
        backend.close().await;
        fs::remove_dir_all(path.parent().expect("database parent")).expect("storage cleanup");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn database_storage_info_probes_database_independently_of_model_health() {
        let path = database_path("storage-info-health-contract");
        let models_path = path.parent().expect("database parent").join("models.json");
        fs::write(&models_path, br#"{"data":[{"id":"database-info-model"}]}"#)
            .expect("models snapshot");
        let backend = Arc::new(
            StorageBackend::connect_database(&sqlite_url(&path))
                .await
                .expect("database backend"),
        );
        let accounts = backend.load_accounts().await.expect("empty accounts");
        backend
            .save_accounts(
                accounts.revision,
                vec![serde_json::json!({
                    "access_token": "database-info-account",
                    "status": "正常",
                    "type": "pro",
                    "source_type": "web"
                })],
                1,
            )
            .await
            .expect("database account");
        let state = crate::AppState::new_with_storage_backend(
            crate::AppConfig {
                version: "database-storage-info-health-contract".to_owned(),
                auth_key: Some("database-info-admin".to_owned()),
                models: vec![],
                upstream_base_url: None,
                upstream_auth: None,
                auth_keys_path: None,
                models_path: Some(models_path.clone()),
                accounts_path: None,
                upstream_protocol: crate::UpstreamProtocol::ChatGpt,
            },
            backend.clone(),
            path.parent().expect("database parent").to_owned(),
        )
        .await
        .expect("database app state");

        async fn storage_info(state: &crate::AppState) -> Value {
            let response = state
                .router()
                .oneshot(
                    Request::builder()
                        .uri("/api/storage/info")
                        .header(header::AUTHORIZATION, "Bearer database-info-admin")
                        .body(Body::empty())
                        .expect("storage info request"),
                )
                .await
                .expect("storage info response");
            assert_eq!(response.status(), StatusCode::OK);
            serde_json::from_slice(
                &response
                    .into_body()
                    .collect()
                    .await
                    .expect("storage info body")
                    .to_bytes(),
            )
            .expect("storage info JSON")
        }

        async fn health_info(state: &crate::AppState) -> Value {
            let response = state
                .router()
                .oneshot(
                    Request::builder()
                        .uri("/health?format=json")
                        .body(Body::empty())
                        .expect("health request"),
                )
                .await
                .expect("health response");
            assert_eq!(response.status(), StatusCode::OK);
            serde_json::from_slice(
                &response
                    .into_body()
                    .collect()
                    .await
                    .expect("health body")
                    .to_bytes(),
            )
            .expect("health JSON")
        }

        assert_eq!(
            storage_info(&state).await,
            json!({
                "backend": {"type": "database", "db_type": "sqlite"},
                "health": {"status": "healthy"},
            })
        );

        let occupied = state
            .health_storage_semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("storage info admission permit");
        let started = Instant::now();
        assert_eq!(
            storage_info(&state).await,
            json!({
                "backend": {"type": "database", "db_type": "sqlite"},
                "health": {"status": "unhealthy", "error": "存储后端健康检查失败"},
            })
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "storage info admission timeout exceeded bound"
        );
        drop(occupied);
        let _ = tokio::time::timeout(
            Duration::from_secs(1),
            state.health_storage_semaphore.clone().acquire_owned(),
        )
        .await
        .expect("storage info permit remained held after bounded timeout")
        .expect("storage info semaphore closed");

        fs::write(&models_path, b"not-json").expect("invalid models snapshot");
        assert!(!state.models.reload().await);
        assert_eq!(
            storage_info(&state).await,
            json!({
                "backend": {"type": "database", "db_type": "sqlite"},
                "health": {"status": "healthy"},
            }),
            "model invalidity must not make a healthy database unhealthy"
        );
        let health = health_info(&state).await;
        assert_eq!(
            health["storage"],
            json!({
                "backend": "database",
                "health": {"status": "healthy"},
            })
        );
        assert_eq!(
            health["healthy"], true,
            "model invalidity must not make a healthy database unhealthy"
        );

        backend.close().await;
        assert_eq!(
            storage_info(&state).await,
            json!({
                "backend": {"type": "database", "db_type": "sqlite"},
                "health": {"status": "unhealthy", "error": "存储后端健康检查失败"},
            }),
            "closed database must not be reported healthy from a stale cache"
        );

        drop(state);
        let _ = fs::remove_file(models_path);
        fs::remove_dir_all(path.parent().expect("database parent")).expect("storage cleanup");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn json_health_storage_ignores_model_health() {
        let anchor = database_path("json-health-model-contract");
        let root = anchor.parent().expect("JSON health parent").to_owned();
        let accounts_path = root.join("accounts.json");
        let auth_keys_path = root.join("auth_keys.json");
        let models_path = root.join("models.json");
        fs::write(
            &accounts_path,
            serde_json::to_vec(&json!({
                "items": [{
                    "access_token": "json-health-account",
                    "status": "正常",
                    "type": "pro"
                }]
            }))
            .expect("JSON health accounts JSON"),
        )
        .expect("JSON health accounts");
        fs::write(&auth_keys_path, br#"{"items":[]}"#).expect("JSON health auth keys");
        fs::write(&models_path, br#"{"data":[{"id":"json-health-model"}]}"#)
            .expect("JSON health models");
        let backend = Arc::new(
            StorageBackend::connect_json(&accounts_path, &auth_keys_path)
                .expect("JSON health backend"),
        );
        let state = crate::AppState::new_with_storage_backend(
            crate::AppConfig {
                version: "json-health-model-contract".to_owned(),
                auth_key: None,
                models: Vec::new(),
                upstream_base_url: None,
                upstream_auth: None,
                auth_keys_path: Some(auth_keys_path.clone()),
                models_path: Some(models_path.clone()),
                accounts_path: Some(accounts_path.clone()),
                upstream_protocol: crate::UpstreamProtocol::ChatGpt,
            },
            backend.clone(),
            root.clone(),
        )
        .await
        .expect("JSON health app state");

        async fn health_json(state: &crate::AppState) -> Value {
            let response = state
                .router()
                .oneshot(
                    Request::builder()
                        .uri("/health?format=json")
                        .body(Body::empty())
                        .expect("JSON health request"),
                )
                .await
                .expect("JSON health response");
            assert_eq!(response.status(), StatusCode::OK);
            serde_json::from_slice(
                &response
                    .into_body()
                    .collect()
                    .await
                    .expect("JSON health body")
                    .to_bytes(),
            )
            .expect("JSON health JSON")
        }

        let healthy = health_json(&state).await;
        assert_eq!(healthy["storage"]["health"]["status"], "healthy");
        assert_eq!(healthy["healthy"], true);

        fs::write(&models_path, b"not-json").expect("invalid JSON health models");
        assert!(!state.models.reload().await);
        let model_invalid = health_json(&state).await;
        assert_eq!(model_invalid["storage"]["health"]["status"], "healthy");
        assert_eq!(
            model_invalid["healthy"], true,
            "model invalidity must not make local JSON storage unhealthy"
        );

        drop(state);
        backend.close().await;
        fs::remove_dir_all(root).expect("JSON health cleanup");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn database_health_generation_write_blocks_observers_until_pair_publish() {
        let path = database_path("health-generation-barrier");
        let backend = Arc::new(
            StorageBackend::connect_database(&sqlite_url(&path))
                .await
                .expect("database backend"),
        );
        let old_account = serde_json::json!({
            "access_token": "database-health-old-account",
            "status": "正常",
            "type": "pro",
            "source_type": "web"
        });
        let accounts = backend.load_accounts().await.expect("empty accounts");
        backend
            .save_accounts(accounts.revision, vec![old_account.clone()], 1)
            .await
            .expect("old account");
        let old_key = "database-health-old-secret";
        let old_auth = serde_json::json!({
            "id": "database-health-old-auth",
            "role": "admin",
            "key_hash": token_hash(old_key),
            "enabled": true
        });
        let auth = backend.load_auth_keys().await.expect("empty auth");
        backend
            .save_auth_keys(auth.revision, vec![old_auth.clone()])
            .await
            .expect("old auth");
        let state = crate::AppState::new_with_storage_backend(
            crate::AppConfig {
                version: "database-health-generation-barrier".to_owned(),
                auth_key: None,
                models: vec!["auto".to_owned()],
                upstream_base_url: None,
                upstream_auth: None,
                auth_keys_path: None,
                models_path: None,
                accounts_path: None,
                upstream_protocol: crate::UpstreamProtocol::ChatGpt,
            },
            backend.clone(),
            path.parent().expect("database parent").to_owned(),
        )
        .await
        .expect("database app state");
        let accounts = backend.load_accounts().await.expect("current accounts");
        backend
            .save_accounts(
                accounts.revision,
                vec![
                    old_account,
                    serde_json::json!({
                        "access_token": "database-health-new-account",
                        "status": "正常",
                        "type": "team",
                        "source_type": "web"
                    }),
                ],
                2,
            )
            .await
            .expect("new account");
        let auth = backend.load_auth_keys().await.expect("current auth");
        backend
            .save_auth_keys(
                auth.revision,
                vec![
                    old_auth,
                    serde_json::json!({
                        "id": "database-health-new-auth",
                        "role": "user",
                        "key_hash": token_hash("database-health-new-secret"),
                        "enabled": true
                    }),
                ],
            )
            .await
            .expect("new auth");

        let hook = crate::HealthSnapshotPublishTestHook {
            after_accounts: Arc::new(tokio::sync::Notify::new()),
            release_before_auth: Arc::new(tokio::sync::Notify::new()),
            pause_between: Arc::new(AtomicBool::new(true)),
        };
        *state
            .health_snapshot_publish_test_hook
            .write()
            .expect("database health publish hook") = Some(hook.clone());
        let health_request = tokio::spawn({
            let app = state.router();
            async move {
                app.oneshot(
                    Request::builder()
                        .uri("/health?format=json")
                        .body(Body::empty())
                        .expect("database health request"),
                )
                .await
                .expect("database health response")
            }
        });
        tokio::time::timeout(Duration::from_secs(2), hook.after_accounts.notified())
            .await
            .expect("database health did not reach pair barrier");
        let during = state
            .health_snapshot_cache
            .lock()
            .expect("database health cache")
            .clone();
        assert_eq!(during.account_count, 1);
        assert_eq!(during.auth_key_count, 1);
        assert!(during.local_snapshot_available);

        let mut observer = tokio::spawn({
            let app = state.router();
            let old_key = old_key.to_owned();
            async move {
                app.oneshot(
                    Request::builder()
                        .uri("/api/accounts")
                        .header(header::AUTHORIZATION, format!("Bearer {old_key}"))
                        .body(Body::empty())
                        .expect("database account observer"),
                )
                .await
                .expect("database account observer response")
            }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut observer)
                .await
                .is_err(),
            "database observer crossed generation publish barrier"
        );

        hook.release_before_auth.notify_one();
        let health_response = tokio::time::timeout(Duration::from_secs(2), health_request)
            .await
            .expect("database health remained blocked after release")
            .expect("database health task");
        assert_eq!(health_response.status(), StatusCode::OK);
        let payload: Value = serde_json::from_slice(
            &health_response
                .into_body()
                .collect()
                .await
                .expect("database health body")
                .to_bytes(),
        )
        .expect("database health JSON");
        assert_eq!(
            payload["storage"],
            json!({"backend": "database", "health": {"status": "healthy"}})
        );
        let cache = state
            .health_snapshot_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(cache.account_count, 2);
        assert_eq!(cache.auth_key_count, 2);
        assert_eq!(payload["healthy"], true);
        let observer_response = tokio::time::timeout(Duration::from_secs(2), observer)
            .await
            .expect("database observer remained blocked after release")
            .expect("database observer task");
        assert_eq!(observer_response.status(), StatusCode::OK);
        *state
            .health_snapshot_publish_test_hook
            .write()
            .expect("database health publish hook cleanup") = None;
        drop(state);
        backend.close().await;
        fs::remove_dir_all(path.parent().expect("database parent")).expect("storage cleanup");
    }

    #[test]
    fn database_url_redaction_removes_full_userinfo_query_and_fragment() {
        let redacted = redact_url(
            "postgresql://user:opaque@tail@db.example.test/app?password=secret#fragment",
        );
        assert_eq!(redacted, "postgresql://[REDACTED]@db.example.test/app");
        for secret in ["user", "opaque", "tail", "password", "secret", "fragment"] {
            assert!(!redacted.contains(secret));
        }
    }

    #[tokio::test]
    async fn configured_database_defaults_to_data_dir_and_unknown_fails_closed() {
        let path = database_path("configured");
        let data_dir = path.parent().expect("database parent");
        assert!(
            StorageBackend::connect_configured("json", None, data_dir)
                .await
                .expect("json configuration")
                .is_some()
        );
        let backend = StorageBackend::connect_configured("sqlite", None, data_dir)
            .await
            .expect("sqlite configuration")
            .expect("database backend");
        assert!(data_dir.join("accounts.db").is_file());
        backend.close().await;
        assert!(matches!(
            StorageBackend::connect_configured("unknown", None, data_dir).await,
            Err(StorageError::Unsupported)
        ));
        fs::remove_dir_all(data_dir).expect("storage cleanup");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn json_owner_cross_instance_cas_allows_one_winner_and_preserves_file_integrity() {
        let root = database_path("json-owner-cross-instance-cas");
        let data_dir = root.parent().expect("JSON parent").to_owned();
        fs::create_dir_all(&data_dir).expect("JSON directory");
        let accounts_path = data_dir.join("accounts.json");
        fs::write(
            &accounts_path,
            r#"{"items":[{"access_token":"cas-base","status":"正常","type":"free"}],"cumulative_total":1}"#
                .as_bytes(),
        )
        .expect("JSON accounts");
        fs::write(data_dir.join("auth_keys.json"), br#"{"items":[]}"#).expect("JSON auth");

        let first = StorageBackend::connect_configured("json", None, &data_dir)
            .await
            .expect("first JSON backend")
            .expect("JSON backend owner");
        let second = StorageBackend::connect_configured("json", None, &data_dir)
            .await
            .expect("second JSON backend")
            .expect("JSON backend owner");
        for round in 0..16_u64 {
            let initial = first
                .load_accounts()
                .await
                .expect("initial account snapshot");
            assert_eq!(
                second
                    .load_accounts()
                    .await
                    .expect("second account snapshot"),
                initial
            );
            let barrier = Arc::new(tokio::sync::Barrier::new(3));
            let first_barrier = barrier.clone();
            let second_barrier = barrier.clone();
            let first_backend = first.clone();
            let second_backend = second.clone();
            let expected = initial.revision;
            let first_task = tokio::spawn(async move {
                first_barrier.wait().await;
                first_backend
                    .save_accounts(
                        expected,
                        vec![serde_json::json!({
                            "access_token": format!("cas-first-{round}"),
                            "status":"正常",
                            "type":"plus"
                        })],
                        10 + round * 2,
                    )
                    .await
            });
            let second_task = tokio::spawn(async move {
                second_barrier.wait().await;
                second_backend
                    .save_accounts(
                        expected,
                        vec![serde_json::json!({
                            "access_token": format!("cas-second-{round}"),
                            "status":"正常",
                            "type":"team"
                        })],
                        11 + round * 2,
                    )
                    .await
            });
            barrier.wait().await;
            let first_result = first_task.await.expect("first CAS task");
            let second_result = second_task.await.expect("second CAS task");
            let outcomes = vec![first_result, second_result];
            assert_eq!(
                outcomes.iter().filter(|outcome| outcome.is_ok()).count(),
                1,
                "CAS must have exactly one successful complete snapshot: {outcomes:?}"
            );
            assert_eq!(
                outcomes
                    .iter()
                    .filter(|outcome| matches!(outcome, Err(StorageError::Conflict)))
                    .count(),
                1,
                "CAS must have exactly one revision conflict: {outcomes:?}"
            );
            let winner = outcomes
                .into_iter()
                .find_map(Result::ok)
                .expect("CAS winner snapshot");
            let final_snapshot = first.load_accounts().await.expect("final account snapshot");
            assert_eq!(
                final_snapshot, winner,
                "disk must equal the complete account winner"
            );
        }

        for round in 0..16_u64 {
            let initial = first.load_auth_keys().await.expect("initial auth snapshot");
            assert_eq!(
                second.load_auth_keys().await.expect("second auth snapshot"),
                initial
            );
            let barrier = Arc::new(tokio::sync::Barrier::new(3));
            let first_barrier = barrier.clone();
            let second_barrier = barrier.clone();
            let first_backend = first.clone();
            let second_backend = second.clone();
            let expected = initial.revision;
            let first_task = tokio::spawn(async move {
                first_barrier.wait().await;
                first_backend
                    .save_auth_keys(
                        expected,
                        vec![serde_json::json!({
                            "id": format!("cas-auth-first-{round}"),
                            "role":"admin",
                            "key_hash": token_hash(&format!("cas-auth-first-key-{round}")),
                            "enabled":true
                        })],
                    )
                    .await
            });
            let second_task = tokio::spawn(async move {
                second_barrier.wait().await;
                second_backend
                    .save_auth_keys(
                        expected,
                        vec![serde_json::json!({
                            "id": format!("cas-auth-second-{round}"),
                            "role":"admin",
                            "key_hash": token_hash(&format!("cas-auth-second-key-{round}")),
                            "enabled":true
                        })],
                    )
                    .await
            });
            barrier.wait().await;
            let first_result = first_task.await.expect("first auth CAS task");
            let second_result = second_task.await.expect("second auth CAS task");
            let outcomes = vec![first_result, second_result];
            assert_eq!(
                outcomes.iter().filter(|outcome| outcome.is_ok()).count(),
                1,
                "auth CAS must have exactly one successful complete snapshot: {outcomes:?}"
            );
            assert_eq!(
                outcomes
                    .iter()
                    .filter(|outcome| matches!(outcome, Err(StorageError::Conflict)))
                    .count(),
                1,
                "auth CAS must have exactly one revision conflict: {outcomes:?}"
            );
            let winner = outcomes
                .into_iter()
                .find_map(Result::ok)
                .expect("auth CAS winner snapshot");
            let final_snapshot = first.load_auth_keys().await.expect("final auth snapshot");
            assert_eq!(
                final_snapshot, winner,
                "disk must equal the complete auth winner"
            );
        }
        let names = fs::read_dir(&data_dir)
            .expect("JSON directory entries")
            .map(|entry| {
                entry
                    .expect("JSON entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<Vec<_>>();
        assert!(
            names
                .iter()
                .all(|name| !name.contains(".tmp") && !name.contains(".partial")),
            "atomic temporary files must not remain: {names:?}"
        );
        first.close().await;
        second.close().await;
        fs::remove_dir_all(data_dir).expect("JSON cleanup");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn json_owner_cas_reads_only_after_cross_process_lock_and_rejects_external_replace() {
        let root = database_path("json-owner-cas-lock-wait");
        let data_dir = root.parent().expect("JSON parent").to_owned();
        fs::create_dir_all(&data_dir).expect("JSON directory");
        let accounts_path = data_dir.join("accounts.json");
        fs::write(
            &accounts_path,
            r#"{"items":[{"access_token":"lock-base","status":"正常","type":"free"}],"cumulative_total":1}"#
                .as_bytes(),
        )
        .expect("JSON accounts");
        fs::write(data_dir.join("auth_keys.json"), br#"{"items":[]}"#).expect("JSON auth");
        let backend = StorageBackend::connect_configured("json", None, &data_dir)
            .await
            .expect("JSON backend")
            .expect("JSON owner");
        let json = match backend.as_ref() {
            StorageBackend::Json(json) => json.clone(),
            StorageBackend::Database(_) | StorageBackend::Git(_) => panic!("expected JSON owner"),
        };
        let initial = backend
            .load_accounts()
            .await
            .expect("initial JSON snapshot");
        let after_read = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        json.set_cas_read_hook(Some(JsonCasReadHook {
            after_read: after_read.clone(),
            release: release.clone(),
            pause: Arc::new(AtomicBool::new(true)),
        }));
        let held_lock = super::super::acquire_path_write_lock_sync(&accounts_path)
            .expect("external accounts lock");
        let task_backend = backend.clone();
        let task = tokio::spawn(async move {
            task_backend
                .save_accounts(
                    initial.revision,
                    vec![serde_json::json!({
                        "access_token":"should-not-overwrite",
                        "status":"正常",
                        "type":"pro"
                    })],
                    9,
                )
                .await
        });
        let read_before_lock_release =
            tokio::time::timeout(Duration::from_millis(250), after_read.notified())
                .await
                .is_ok();
        let replacement = r#"{"items":[{"access_token":"external-winner","status":"正常","type":"team"}],"cumulative_total":4}"#
            .as_bytes();
        fs::write(&accounts_path, replacement).expect("external replacement");
        release.notify_one();
        drop(held_lock);
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("CAS lock waiter completed")
            .expect("CAS task completed");
        assert!(
            !read_before_lock_release,
            "CAS read happened before the file lock"
        );
        assert!(matches!(result, Err(StorageError::Conflict)));
        let final_snapshot = backend
            .load_accounts()
            .await
            .expect("external snapshot remains");
        assert_eq!(final_snapshot.records[0]["access_token"], "external-winner");
        assert_eq!(final_snapshot.cumulative_total, Some(4));
        json.set_cas_read_hook(None);
        backend.close().await;
        fs::remove_dir_all(data_dir).expect("JSON cleanup");
    }

    #[cfg(feature = "live-database-tests")]
    mod live_database {
        use std::sync::atomic::{AtomicBool, Ordering};

        use super::*;

        fn required_url(name: &str, kind: DatabaseKind) -> String {
            let value = std::env::var(name)
                .unwrap_or_else(|_| panic!("{name} must be set for live database tests"));
            assert_eq!(
                DatabaseKind::from_url(&value).expect("live database URL scheme"),
                kind,
                "live database URL has the wrong dialect"
            );
            value
        }

        async fn live_pool(url: &str, max_connections: u32) -> AnyPool {
            install_default_drivers();
            AnyPoolOptions::new()
                .max_connections(max_connections)
                .acquire_timeout(Duration::from_secs(5))
                .connect(url)
                .await
                .expect("live database pool")
        }

        async fn reset_live_schema(pool: &AnyPool) {
            for table in [
                "accounts__chatgpt2api_token_migration",
                "accounts__chatgpt2api_token_backup",
                "auth_keys",
                "storage_metadata",
                "storage_mutation_locks",
                "accounts",
            ] {
                pool.execute(format!("DROP TABLE IF EXISTS {table}").as_str())
                    .await
                    .expect("reset live database table");
            }
        }

        async fn scalar_i64(connection: &mut AnyConnection, statement: &str) -> i64 {
            sqlx::query(statement)
                .fetch_one(&mut *connection)
                .await
                .expect("live scalar query")
                .try_get::<i64, _>(0)
                .expect("live scalar value")
        }

        #[derive(Debug, Eq, PartialEq)]
        struct MySqlInvalidFixtureState {
            column_type: String,
            row_count: i64,
            access_token: String,
            access_token_hash: String,
            data_len: usize,
            data_sha256: [u8; 32],
            residue_count: i64,
        }

        async fn mysql_invalid_fixture_state(pool: &AnyPool) -> MySqlInvalidFixtureState {
            let column_type = sqlx::query(
                "SELECT CAST(data_type AS CHAR(64)) AS data_type FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'accounts' AND column_name = 'data'",
            )
            .fetch_one(pool)
            .await
            .expect("invalid fixture column metadata")
            .try_get::<String, _>("data_type")
            .expect("invalid fixture column type");
            let row_count = sqlx::query("SELECT COUNT(*) AS count FROM accounts")
                .fetch_one(pool)
                .await
                .expect("invalid fixture row count query")
                .try_get::<i64, _>("count")
                .expect("invalid fixture row count");
            let row = sqlx::query(
                "SELECT access_token, access_token_hash, data FROM accounts ORDER BY id",
            )
            .fetch_one(pool)
            .await
            .expect("invalid fixture row");
            let access_token = database_row_text(
                &row,
                "access_token",
                DatabaseKind::MySql,
                "test.invalid_fixture_token",
            )
            .expect("invalid fixture token");
            let access_token_hash = row
                .try_get::<String, _>("access_token_hash")
                .expect("invalid fixture hash");
            let data = row
                .try_get::<Vec<u8>, _>("data")
                .expect("invalid fixture bytes");
            let residue_count = sqlx::query(
                "SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name IN ('accounts__chatgpt2api_token_migration', 'accounts__chatgpt2api_token_backup')",
            )
            .fetch_one(pool)
            .await
            .expect("invalid fixture residue query")
            .try_get::<i64, _>("count")
            .expect("invalid fixture residue count");
            MySqlInvalidFixtureState {
                column_type,
                row_count,
                access_token,
                access_token_hash,
                data_len: data.len(),
                data_sha256: Sha256::digest(&data).into(),
                residue_count,
            }
        }

        #[tokio::test]
        async fn postgres_equivalent_urls_share_bounded_transaction_lock_and_rollback() {
            let first_url = required_url("CHATGPT2API_TEST_POSTGRES_URL_A", DatabaseKind::Postgres);
            let second_url =
                required_url("CHATGPT2API_TEST_POSTGRES_URL_B", DatabaseKind::Postgres);
            assert_ne!(
                first_url, second_url,
                "live URLs must exercise distinct credentials"
            );

            let setup = live_pool(&first_url, 4).await;
            reset_live_schema(&setup).await;
            setup.close().await;

            let first = DatabaseStorage::connect(&first_url)
                .await
                .expect("initial PostgreSQL schema");
            let postgres_blob = sqlx::query("SELECT decode('ff', 'hex') AS value")
                .fetch_one(&first.pool)
                .await
                .expect("PostgreSQL bytea row");
            assert_eq!(
                database_row_text(
                    &postgres_blob,
                    "value",
                    DatabaseKind::Postgres,
                    "test.postgres_blob"
                ),
                Err(StorageError::Invalid),
                "PostgreSQL must not inherit the MySQL LONGTEXT blob compatibility path"
            );
            let mut owner = first.pool.acquire().await.expect("PostgreSQL lock owner");
            owner.execute("BEGIN").await.expect("owner transaction");
            let database_name = server_database_name(&mut owner, DatabaseKind::Postgres)
                .await
                .expect("PostgreSQL database identity");
            let acquired = sqlx::query("SELECT pg_try_advisory_xact_lock($1) AS acquired")
                .bind(postgres_schema_lock_key(&database_name))
                .fetch_one(&mut *owner)
                .await
                .expect("hold PostgreSQL schema lock")
                .try_get::<bool, _>("acquired")
                .expect("PostgreSQL lock result");
            assert!(acquired);

            let started = tokio::time::Instant::now();
            let blocked = tokio::time::timeout(
                Duration::from_secs(15),
                DatabaseStorage::connect(&second_url),
            )
            .await
            .expect("PostgreSQL lock wait is externally bounded");
            let elapsed = started.elapsed();
            assert!(matches!(blocked, Err(StorageError::Unavailable)));
            assert!(
                (Duration::from_secs(9)..=Duration::from_secs(13)).contains(&elapsed),
                "PostgreSQL lock wait did not honor its ten-second budget: {elapsed:?}"
            );
            owner
                .execute("ROLLBACK")
                .await
                .expect("release PostgreSQL lock");
            drop(owner);

            let second = tokio::time::timeout(
                Duration::from_secs(5),
                DatabaseStorage::connect(&second_url),
            )
            .await
            .expect("PostgreSQL retry after lock release")
            .expect("second PostgreSQL schema startup");
            quiescent_close_database_pool(&second.pool).await;
            quiescent_close_database_pool(&first.pool).await;

            let setup = live_pool(&first_url, 4).await;
            reset_live_schema(&setup).await;
            setup
                .execute(
                    "CREATE TABLE accounts (id BIGSERIAL PRIMARY KEY, access_token VARCHAR(2048) NOT NULL, access_token_hash CHAR(64), data TEXT NOT NULL)",
                )
                .await
                .expect("PostgreSQL malformed legacy table");
            let token = "postgres-restart-token";
            sqlx::query(
                "INSERT INTO accounts(access_token, access_token_hash, data) VALUES ($1, $2, $3)",
            )
            .bind(token)
            .bind("0".repeat(64))
            .bind(serde_json::json!({"access_token":token}).to_string())
            .execute(&setup)
            .await
            .expect("PostgreSQL malformed row");
            setup.close().await;

            assert!(matches!(
                DatabaseStorage::connect(&first_url).await,
                Err(StorageError::Invalid)
            ));
            let repair = live_pool(&second_url, 2).await;
            sqlx::query("UPDATE accounts SET access_token_hash = $1 WHERE access_token = $2")
                .bind(token_hash(token))
                .bind(token)
                .execute(&repair)
                .await
                .expect("repair PostgreSQL row");
            repair.close().await;
            let recovered = tokio::time::timeout(
                Duration::from_secs(5),
                DatabaseStorage::connect(&second_url),
            )
            .await
            .expect("PostgreSQL failed migration released its lock")
            .expect("PostgreSQL startup after repair");
            assert_eq!(
                recovered
                    .load_accounts()
                    .await
                    .expect("recovered PostgreSQL accounts")
                    .records
                    .len(),
                1
            );
            reset_live_schema(&recovered.pool).await;
            quiescent_close_database_pool(&recovered.pool).await;
        }

        #[tokio::test]
        async fn mysql_equivalent_urls_share_lock_close_session_and_recover_shadow_residue() {
            let first_url = required_url("CHATGPT2API_TEST_MYSQL_URL_A", DatabaseKind::MySql);
            let second_url = required_url("CHATGPT2API_TEST_MYSQL_URL_B", DatabaseKind::MySql);
            assert_ne!(
                first_url, second_url,
                "live URLs must exercise distinct credentials"
            );

            let setup = live_pool(&first_url, 4).await;
            reset_live_schema(&setup).await;
            setup.close().await;
            let first = DatabaseStorage::connect(&first_url)
                .await
                .expect("initial MySQL schema");
            let mut owner = first.pool.acquire().await.expect("MySQL lock owner");
            let database_name = server_database_name(&mut owner, DatabaseKind::MySql)
                .await
                .expect("MySQL database identity");
            let lock_name = mysql_schema_lock_name(&database_name);
            let acquired = sqlx::query("SELECT GET_LOCK(?, 0) AS acquired")
                .bind(&lock_name)
                .fetch_one(&mut *owner)
                .await
                .expect("hold MySQL schema lock")
                .try_get::<Option<i64>, _>("acquired")
                .expect("MySQL lock result");
            assert_eq!(acquired, Some(1));

            let started = tokio::time::Instant::now();
            let blocked = tokio::time::timeout(
                Duration::from_secs(15),
                DatabaseStorage::connect(&second_url),
            )
            .await
            .expect("MySQL lock wait is externally bounded");
            let elapsed = started.elapsed();
            assert!(matches!(blocked, Err(StorageError::Unavailable)));
            assert!(
                (Duration::from_secs(9)..=Duration::from_secs(13)).contains(&elapsed),
                "MySQL lock wait did not honor its ten-second budget: {elapsed:?}"
            );
            let released = sqlx::query("SELECT RELEASE_LOCK(?) AS released")
                .bind(&lock_name)
                .fetch_one(&mut *owner)
                .await
                .expect("release MySQL schema lock")
                .try_get::<Option<i64>, _>("released")
                .expect("MySQL release result");
            assert_eq!(released, Some(1));
            drop(owner);
            let second = tokio::time::timeout(
                Duration::from_secs(5),
                DatabaseStorage::connect(&second_url),
            )
            .await
            .expect("MySQL retry after lock release")
            .expect("second MySQL schema startup");
            quiescent_close_database_pool(&second.pool).await;
            quiescent_close_database_pool(&first.pool).await;

            let disposal_pool = live_pool(&first_url, 1).await;
            let mut disposable = disposal_pool
                .acquire()
                .await
                .expect("disposable MySQL session");
            let first_connection_id = scalar_i64(&mut disposable, "SELECT CONNECTION_ID()").await;
            let database_name = server_database_name(&mut disposable, DatabaseKind::MySql)
                .await
                .expect("disposable MySQL database identity");
            let disposal_lock_name = mysql_schema_lock_name(&database_name);
            let acquired = sqlx::query("SELECT GET_LOCK(?, 0) AS acquired")
                .bind(&disposal_lock_name)
                .fetch_one(&mut *disposable)
                .await
                .expect("disposable MySQL lock")
                .try_get::<Option<i64>, _>("acquired")
                .expect("disposable lock result");
            assert_eq!(acquired, Some(1));
            disposable.close_on_drop();
            drop(disposable);
            let mut replacement =
                tokio::time::timeout(Duration::from_secs(5), disposal_pool.acquire())
                    .await
                    .expect("discarded MySQL session closes promptly")
                    .expect("replacement MySQL session");
            let replacement_id = scalar_i64(&mut replacement, "SELECT CONNECTION_ID()").await;
            assert_ne!(first_connection_id, replacement_id);
            let reacquired = sqlx::query("SELECT GET_LOCK(?, 0) AS acquired")
                .bind(&disposal_lock_name)
                .fetch_one(&mut *replacement)
                .await
                .expect("reacquire lock after physical session close")
                .try_get::<Option<i64>, _>("acquired")
                .expect("reacquired lock result");
            assert_eq!(reacquired, Some(1));
            sqlx::query("SELECT RELEASE_LOCK(?)")
                .bind(&disposal_lock_name)
                .execute(&mut *replacement)
                .await
                .expect("release replacement lock");
            drop(replacement);
            quiescent_close_database_pool(&disposal_pool).await;

            let setup = live_pool(&first_url, 6).await;
            reset_live_schema(&setup).await;
            setup
                .execute(
                    "CREATE TABLE accounts (id BIGINT PRIMARY KEY AUTO_INCREMENT, access_token VARCHAR(255) NOT NULL, data LONGTEXT NOT NULL, UNIQUE KEY ix_accounts_access_token(access_token))",
                )
                .await
                .expect("MySQL legacy table");
            let large_segments = (0..16)
                .map(|index| format!("{index}:{}", "数据库🙂".repeat(500)))
                .collect::<Vec<_>>();
            let large_account = serde_json::json!({
                "access_token":"mysql-legacy-token-0",
                "index":0,
                "segments":large_segments,
            });
            assert!(large_account.to_string().len() > 64 * 1024);
            for index in 0..128_i64 {
                let token = format!("mysql-legacy-token-{index}");
                let record = if index == 0 {
                    large_account.clone()
                } else {
                    serde_json::json!({"access_token":token,"index":index})
                };
                sqlx::query("INSERT INTO accounts(id, access_token, data) VALUES (?, ?, ?)")
                    .bind(index + 1)
                    .bind(&token)
                    .bind(record.to_string())
                    .execute(&setup)
                    .await
                    .expect("MySQL legacy row");
            }
            setup
                .execute("CREATE TABLE accounts__chatgpt2api_token_migration LIKE accounts")
                .await
                .expect("pre-rename migration residue");
            setup
                .execute("CREATE TABLE accounts__chatgpt2api_token_backup LIKE accounts")
                .await
                .expect("pre-rename backup residue");

            let stop = Arc::new(AtomicBool::new(false));
            let reader_stop = stop.clone();
            let reader_pool = setup.clone();
            let (reader_started_tx, reader_started_rx) = tokio::sync::oneshot::channel();
            let reader = tokio::spawn(async move {
                let mut iterations = 0_u64;
                let mut reader_started_tx = Some(reader_started_tx);
                while !reader_stop.load(Ordering::Acquire) {
                    let row = sqlx::query("SELECT COUNT(*) AS count FROM accounts")
                        .fetch_one(&reader_pool)
                        .await
                        .map_err(|_| "reader query failed")?;
                    let count = row
                        .try_get::<i64, _>("count")
                        .map_err(|_| "reader count decode failed")?;
                    if count != 128 {
                        return Err("reader observed a partial table");
                    }
                    iterations += 1;
                    if let Some(started) = reader_started_tx.take() {
                        let _ = started.send(());
                    }
                    tokio::task::yield_now().await;
                }
                Ok::<u64, &'static str>(iterations)
            });
            reader_started_rx.await.expect("MySQL reader started");
            let migrated = DatabaseStorage::connect(&second_url)
                .await
                .expect("MySQL shadow migration");
            stop.store(true, Ordering::Release);
            let iterations = reader
                .await
                .expect("MySQL reader task")
                .expect("atomic MySQL reader contract");
            assert!(iterations > 0);
            let migrated_accounts = migrated
                .load_accounts()
                .await
                .expect("migrated MySQL accounts");
            assert_eq!(migrated_accounts.records.len(), 128);
            assert_eq!(
                migrated_accounts.records[0]["segments"], large_account["segments"],
                "MySQL LONGTEXT migration must preserve large multibyte JSON"
            );
            let saved_accounts = migrated
                .save_accounts(
                    migrated_accounts.revision,
                    migrated_accounts.records.clone(),
                    17,
                )
                .await
                .expect("MySQL accounts RMW over LONGTEXT rows");
            assert_eq!(saved_accounts.cumulative_total, Some(17));
            assert_eq!(
                migrated
                    .load_accounts()
                    .await
                    .expect("MySQL accounts after RMW")
                    .records[0]["segments"],
                large_account["segments"]
            );

            let empty_auth = migrated
                .load_auth_keys()
                .await
                .expect("empty MySQL auth keys");
            let large_auth = serde_json::json!({
                "id":"mysql-auth-large",
                "key_hash":token_hash("mysql-auth-large-secret"),
                "enabled":true,
                "segments":large_account["segments"].clone(),
            });
            assert!(large_auth.to_string().len() > 64 * 1024);
            let saved_auth = migrated
                .save_auth_keys(empty_auth.revision, vec![large_auth.clone()])
                .await
                .expect("save large MySQL auth key");
            let mut updated_auth = large_auth.clone();
            updated_auth["enabled"] = Value::Bool(false);
            migrated
                .save_auth_keys(saved_auth.revision, vec![updated_auth.clone()])
                .await
                .expect("MySQL auth-key RMW over LONGTEXT row");
            assert_eq!(
                migrated
                    .load_auth_keys()
                    .await
                    .expect("load large MySQL auth key")
                    .records,
                vec![updated_auth]
            );
            let residue_count = sqlx::query(
                "SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name IN ('accounts__chatgpt2api_token_migration', 'accounts__chatgpt2api_token_backup')",
            )
            .fetch_one(&migrated.pool)
            .await
            .expect("MySQL residue query")
            .try_get::<i64, _>("count")
            .expect("MySQL residue count");
            assert_eq!(residue_count, 0);

            migrated
                .pool
                .execute("CREATE TABLE accounts__chatgpt2api_token_backup LIKE accounts")
                .await
                .expect("post-rename backup residue");
            migrated
                .pool
                .execute("CREATE TABLE accounts__chatgpt2api_token_migration LIKE accounts")
                .await
                .expect("post-rename migration residue");
            quiescent_close_database_pool(&migrated.pool).await;
            let recovered = DatabaseStorage::connect(&first_url)
                .await
                .expect("MySQL post-rename residue recovery");
            let residue_count = sqlx::query(
                "SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name IN ('accounts__chatgpt2api_token_migration', 'accounts__chatgpt2api_token_backup')",
            )
            .fetch_one(&recovered.pool)
            .await
            .expect("MySQL recovered residue query")
            .try_get::<i64, _>("count")
            .expect("MySQL recovered residue count");
            assert_eq!(residue_count, 0);

            sqlx::query("UPDATE accounts SET access_token_hash = ? WHERE access_token = ?")
                .bind("0".repeat(64))
                .bind("mysql-legacy-token-0")
                .execute(&recovered.pool)
                .await
                .expect("corrupt MySQL hash");
            quiescent_close_database_pool(&recovered.pool).await;
            assert!(matches!(
                DatabaseStorage::connect(&first_url).await,
                Err(StorageError::Invalid)
            ));
            let repair = live_pool(&second_url, 2).await;
            sqlx::query("UPDATE accounts SET access_token_hash = ? WHERE access_token = ?")
                .bind(token_hash("mysql-legacy-token-0"))
                .bind("mysql-legacy-token-0")
                .execute(&repair)
                .await
                .expect("repair MySQL hash");
            repair.close().await;
            let restarted = tokio::time::timeout(
                Duration::from_secs(5),
                DatabaseStorage::connect(&second_url),
            )
            .await
            .expect("failed MySQL migration released its session lock")
            .expect("MySQL startup after repair");
            reset_live_schema(&restarted.pool).await;
            quiescent_close_database_pool(&restarted.pool).await;
            setup.close().await;

            let invalid = DatabaseStorage::connect(&first_url)
                .await
                .expect("initialize invalid UTF-8 fixture schema");
            invalid
                .pool
                .execute("ALTER TABLE accounts MODIFY data LONGBLOB NOT NULL")
                .await
                .expect("use binary fixture column");
            let invalid_token = "mysql-invalid-utf8-token";
            let invalid_hash = token_hash(invalid_token);
            let invalid_bytes = vec![0xff_u8, 0xfe, 0x80, 0x00, 0x61];
            sqlx::query(
                "INSERT INTO accounts(access_token, access_token_hash, data) VALUES (?, ?, ?)",
            )
            .bind(invalid_token)
            .bind(&invalid_hash)
            .bind(&invalid_bytes)
            .execute(&invalid.pool)
            .await
            .expect("insert invalid UTF-8 fixture");
            invalid
                .pool
                .execute("CREATE TABLE accounts__chatgpt2api_token_backup LIKE accounts")
                .await
                .expect("invalid fixture backup residue");
            invalid
                .pool
                .execute("CREATE TABLE accounts__chatgpt2api_token_migration LIKE accounts")
                .await
                .expect("invalid fixture migration residue");
            let before = mysql_invalid_fixture_state(&invalid.pool).await;
            assert_eq!(before.column_type.to_ascii_lowercase(), "longblob");
            assert_eq!(before.row_count, 1);
            assert_eq!(before.access_token, invalid_token);
            assert_eq!(before.access_token_hash, invalid_hash);
            assert_eq!(before.data_len, invalid_bytes.len());
            let invalid_bytes_sha256: [u8; 32] = Sha256::digest(&invalid_bytes).into();
            assert_eq!(before.data_sha256, invalid_bytes_sha256);
            assert_eq!(before.residue_count, 2);
            quiescent_close_database_pool(&invalid.pool).await;

            assert!(matches!(
                DatabaseStorage::connect(&second_url).await,
                Err(StorageError::Invalid)
            ));
            let direct = DatabaseStorage {
                pool: live_pool(&first_url, 2).await,
                kind: DatabaseKind::MySql,
                redacted_url: redact_url(&first_url),
            };
            assert!(matches!(
                direct.load_accounts().await,
                Err(StorageError::Invalid)
            ));
            assert!(matches!(
                direct
                    .save_accounts(
                        [0_u8; 32],
                        vec![serde_json::json!({"access_token":"must-not-write"})],
                        0,
                    )
                    .await,
                Err(StorageError::Invalid)
            ));
            let after = mysql_invalid_fixture_state(&direct.pool).await;
            assert_eq!(after, before);
            reset_live_schema(&direct.pool).await;
            quiescent_close_database_pool(&direct.pool).await;
        }
    }
}
