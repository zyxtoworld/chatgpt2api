use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{self, File},
    future::Future,
    io::{Cursor, Read, Write},
    path::{Component, Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, LazyLock, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use std::sync::{Condvar, atomic::AtomicUsize};

use aes::Aes256;
use aes::cipher::{BlockDecrypt, BlockEncrypt, KeyInit, generic_array::GenericArray};
use axum::{
    Json,
    body::Body,
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use flate2::{
    Compression,
    read::GzDecoder,
    write::{DeflateEncoder, GzEncoder},
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use image::{ImageDecoder, ImageEncoder, ImageReader};
use reqwest::{Client, Method};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha1::Sha1;
use sha2::{Digest, Sha256};
use tar::{Archive, Builder, Header};
use tokio::sync::Semaphore;
use url::Url;

use super::model_pool::{
    ModelProvenance, is_web_image_model_id, model_provenance_label, model_provenance_rank,
    project_account_model_entries, project_imported_model_entries,
};
use super::proxy_service::{ProxyProfile, flaresolverr_payload, parse_flaresolverr_bundle};
use super::{
    ApiError, AppState, admin_authenticated, authenticated, config, data_file, image_content_type,
    image_root, read_image_tags, redact_config, safe_relative_path,
};
pub(super) type Sub2ApiLoginCache = HashMap<String, (String, [u8; 32], std::time::Instant)>;

const MAX_LOG_BYTES: u64 = 16 * 1024 * 1024;
const MAX_IMAGE_INDEX_BYTES: usize = 16 * 1024 * 1024;
const MAX_LOG_ITEMS: usize = 200;
const MAX_IMAGE_ARCHIVE_BYTES: usize = 512 * 1024 * 1024;
const MAX_BACKUP_BYTES: u64 = 512 * 1024 * 1024;
const MAX_BACKUP_MEMBER_BYTES: u64 = 64 * 1024 * 1024;
const MAX_BACKUP_DETAIL_MEMBERS: usize = 5_000;
const CCLOAD_CHANNEL_BROWSE_DEADLINE: Duration = Duration::from_secs(90);
const CCLOAD_IMPORT_DEADLINE: Duration = Duration::from_secs(30 * 60);
const CCLOAD_CHANNEL_MODEL_LOGIN_DEADLINE: Duration = Duration::from_secs(15);
const CCLOAD_CHANNEL_MODEL_CONCURRENCY: usize = 8;
const CCLOAD_MAX_CHANNELS: usize = 5_000;
const CCLOAD_MAX_CHANNEL_PAGES: usize = 25;
const MAX_R2_DOWNLOAD_BYTES: u64 = MAX_BACKUP_BYTES;
const MAX_R2_LIST_RESPONSE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_R2_LIST_OBJECTS: usize = 5_000;
const MAX_R2_LIST_PAGES: usize = 25;
const R2_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const BACKUP_CRYPT_MAX_CONCURRENCY: usize = 2;
const BACKUP_CRYPT_ADMISSION_TIMEOUT: Duration = Duration::from_secs(1);
const BACKUP_CRYPT_HEADER_BYTES: usize = 16;
const BACKUP_CRYPT_BLOCK_BYTES: usize = 16;

static BACKUP_CRYPT_SEMAPHORE: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(BACKUP_CRYPT_MAX_CONCURRENCY)));
static LOG_WRITE_GATE: LazyLock<StdMutex<()>> = LazyLock::new(|| StdMutex::new(()));
static IMAGE_INDEX_LOCK: LazyLock<StdMutex<()>> = LazyLock::new(|| StdMutex::new(()));

#[derive(Debug, Deserialize, Default)]
pub(super) struct LogQuery {
    #[serde(rename = "type")]
    pub r#type: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub(super) struct ImageListQuery {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub(super) struct ImageCleanupQuery {
    pub target_free_mb: Option<String>,
    pub dry_run: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub(super) struct BackupKeyQuery {
    pub key: Option<String>,
}

#[cfg(test)]
#[derive(Clone)]
pub(super) struct BackupTestHook {
    pub path: PathBuf,
    pub running_key: Arc<StdMutex<Option<String>>>,
    pub after_running: Arc<tokio::sync::Notify>,
    pub release_after_running: Arc<tokio::sync::Notify>,
    pub pause_after_running: Arc<AtomicBool>,
    pub after_archive_commit: Arc<tokio::sync::Notify>,
    pub release_after_archive_commit: Arc<tokio::sync::Notify>,
    pub pause_after_archive_commit: Arc<AtomicBool>,
    pub fail_next_state_publish: Arc<AtomicBool>,
}

#[cfg(test)]
static BACKUP_TEST_HOOK: LazyLock<StdMutex<Option<BackupTestHook>>> =
    LazyLock::new(|| StdMutex::new(None));

#[cfg(test)]
static BACKUP_R2_ENDPOINT: LazyLock<StdMutex<Option<String>>> =
    LazyLock::new(|| StdMutex::new(None));

#[cfg(test)]
#[derive(Clone)]
pub(super) struct BackupCryptTestHook {
    pub active: Arc<AtomicUsize>,
    pub max_active: Arc<AtomicUsize>,
    pub entered: Arc<tokio::sync::Notify>,
    pub release: Arc<(StdMutex<bool>, Condvar)>,
}

#[cfg(test)]
static BACKUP_CRYPT_TEST_HOOK: LazyLock<StdMutex<Option<BackupCryptTestHook>>> =
    LazyLock::new(|| StdMutex::new(None));

#[cfg(test)]
pub(super) struct BackupCryptTestHookGuard;

#[cfg(test)]
impl Drop for BackupCryptTestHookGuard {
    fn drop(&mut self) {
        *BACKUP_CRYPT_TEST_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

#[cfg(test)]
pub(super) fn install_backup_crypt_test_hook(
    hook: BackupCryptTestHook,
) -> BackupCryptTestHookGuard {
    *BACKUP_CRYPT_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
    BackupCryptTestHookGuard
}

#[cfg(test)]
pub(super) struct BackupR2EndpointGuard;

#[cfg(test)]
impl Drop for BackupR2EndpointGuard {
    fn drop(&mut self) {
        *BACKUP_R2_ENDPOINT
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

#[cfg(test)]
pub(super) fn install_backup_r2_endpoint(endpoint: String) -> BackupR2EndpointGuard {
    *BACKUP_R2_ENDPOINT
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(endpoint);
    BackupR2EndpointGuard
}

#[cfg(test)]
pub(super) struct BackupTestHookGuard;

#[cfg(test)]
impl Drop for BackupTestHookGuard {
    fn drop(&mut self) {
        *BACKUP_TEST_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

#[cfg(test)]
pub(super) fn install_backup_test_hook(hook: BackupTestHook) -> BackupTestHookGuard {
    *BACKUP_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
    BackupTestHookGuard
}

#[cfg(test)]
fn backup_test_hook_for(path: &Path) -> Option<BackupTestHook> {
    BACKUP_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
        .filter(|hook| hook.path == path)
}

#[cfg(test)]
async fn backup_test_after_running(path: &Path, key: &str) {
    let Some(hook) = backup_test_hook_for(path) else {
        return;
    };
    *hook
        .running_key
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(key.to_owned());
    if hook.pause_after_running.swap(false, Ordering::SeqCst) {
        hook.after_running.notify_one();
        hook.release_after_running.notified().await;
    }
}

#[cfg(test)]
async fn backup_test_after_archive_commit(path: &Path) {
    let Some(hook) = backup_test_hook_for(path) else {
        return;
    };
    if hook
        .pause_after_archive_commit
        .swap(false, Ordering::SeqCst)
    {
        hook.after_archive_commit.notify_one();
        hook.release_after_archive_commit.notified().await;
    }
}

#[cfg(test)]
fn backup_test_should_fail_state_publish(path: &Path) -> bool {
    backup_test_hook_for(path)
        .is_some_and(|hook| hook.fail_next_state_publish.swap(false, Ordering::SeqCst))
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos())
        .unwrap_or_default()
}
fn random_hex_id(bytes_len: usize) -> String {
    let mut bytes = vec![0_u8; bytes_len];
    if getrandom::getrandom(&mut bytes).is_err() {
        let fallback = now_nanos().to_be_bytes();
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = fallback[index % fallback.len()];
        }
    }
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn log_uuid_hex() -> String {
    let mut bytes = [0_u8; 16];
    if getrandom::getrandom(&mut bytes).is_err() {
        let fallback = now_nanos().to_be_bytes();
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = fallback[index % fallback.len()];
        }
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn backup_object_name(encrypted: bool) -> String {
    let timestamp = iso_timestamp(SystemTime::now()).replace(['-', ':'], "");
    let mut random = [0_u8; 2];
    let value = if getrandom::getrandom(&mut random).is_ok() {
        u16::from_be_bytes(random)
    } else {
        (now_nanos() & 0xffff) as u16
    };
    format!(
        "backup-{timestamp}-{value:04x}.{}",
        if encrypted { "tar.gz.enc" } else { "tar.gz" }
    )
}

fn unix_seconds(value: SystemTime) -> i64 {
    value
        .duration_since(UNIX_EPOCH)
        .map(|value| i64::try_from(value.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

fn date_from_unix(seconds: i64) -> String {
    // Howard Hinnant's civil_from_days, kept local so management routes do
    // not need a second date/time dependency merely for the UI grouping key.
    let days = seconds.div_euclid(86_400);
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted / 146_097
    } else {
        (shifted - 146_096) / 146_097
    };
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month + 2) / 5 + 1;
    let year = year + if month < 10 { 0 } else { 1 };
    let month = month + if month < 10 { 3 } else { -9 };
    format!("{year:04}-{month:02}-{day:02}")
}

fn image_local_timestamp() -> String {
    let now = time::OffsetDateTime::now_local().unwrap_or_else(|_| time::OffsetDateTime::now_utc());
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
pub(super) fn local_timestamp(value: SystemTime) -> String {
    let utc = time::OffsetDateTime::from_unix_timestamp(unix_seconds(value))
        .unwrap_or_else(|_| time::OffsetDateTime::now_utc());
    let local =
        utc.to_offset(time::UtcOffset::current_local_offset().unwrap_or(time::UtcOffset::UTC));
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        local.year(),
        local.month() as u8,
        local.day(),
        local.hour(),
        local.minute(),
        local.second(),
    )
}

pub(super) fn iso_timestamp(value: SystemTime) -> String {
    let seconds = unix_seconds(value);
    let day_seconds = seconds.rem_euclid(86_400);
    let hour = day_seconds / 3_600;
    let minute = (day_seconds % 3_600) / 60;
    let second = day_seconds % 60;
    format!(
        "{}T{hour:02}:{minute:02}:{second:02}Z",
        date_from_unix(seconds)
    )
}
pub(super) fn python_iso_timestamp(value: SystemTime) -> String {
    let seconds = unix_seconds(value);
    let micros = value
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.subsec_micros())
        .unwrap_or_default();
    let day_seconds = seconds.rem_euclid(86_400);
    let hour = day_seconds / 3_600;
    let minute = (day_seconds % 3_600) / 60;
    let second = day_seconds % 60;
    format!(
        "{}T{hour:02}:{minute:02}:{second:02}.{micros:06}+00:00",
        date_from_unix(seconds)
    )
}

fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>, ApiError> {
    let file = File::open(path).map_err(|_| ApiError::unavailable())?;
    let mut bytes = Vec::new();
    file.take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| ApiError::unavailable())?;
    if bytes.len() as u64 > limit {
        return Err(ApiError::unavailable());
    }
    Ok(bytes)
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), ApiError> {
    let _lock = super::acquire_path_write_lock_sync(path)?;
    write_atomic_unlocked(path, bytes)
}

fn write_atomic_unlocked(path: &Path, bytes: &[u8]) -> Result<(), ApiError> {
    super::atomic_replace_checked_with_limit(path, bytes, MAX_BACKUP_BYTES, false)
}

fn maybe_fail_backup_state_publish(path: &Path) -> Result<(), ApiError> {
    #[cfg(test)]
    if backup_test_should_fail_state_publish(path) {
        return Err(ApiError::unavailable());
    }
    let _ = path;
    Ok(())
}

fn write_json(path: &Path, value: &Value) -> Result<(), ApiError> {
    maybe_fail_backup_state_publish(path)?;
    let mut bytes = serde_json::to_vec_pretty(value).map_err(|_| ApiError::unavailable())?;
    bytes.push(b'\n');
    write_atomic(path, &bytes)
}

fn write_json_unlocked(path: &Path, value: &Value) -> Result<(), ApiError> {
    maybe_fail_backup_state_publish(path)?;
    let mut bytes = serde_json::to_vec_pretty(value).map_err(|_| ApiError::unavailable())?;
    bytes.push(b'\n');
    write_atomic_unlocked(path, &bytes)
}

fn object_or_empty(value: Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap_or_default()
}

fn walk_regular_files(root: &Path) -> Vec<PathBuf> {
    let mut pending = vec![root.to_owned()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                pending.push(path);
            } else if file_type.is_file() {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

fn relative_string(root: &Path, path: &Path) -> Option<String> {
    let relative = path
        .strip_prefix(root)
        .ok()?
        .to_string_lossy()
        .replace('\\', "/");
    safe_relative_path(&relative).map(|value| value.to_string_lossy().replace('\\', "/"))
}

fn image_index_path(state: &AppState) -> PathBuf {
    data_file(state, "image_index.json")
}

fn read_image_index_unlocked(state: &AppState) -> Result<Map<String, Value>, ApiError> {
    let bytes = match fs::read(image_index_path(state)) {
        Ok(bytes) if bytes.len() <= MAX_IMAGE_INDEX_BYTES => bytes,
        Ok(_) => return Err(ApiError::unavailable()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Map::new()),
        Err(_) => return Err(ApiError::unavailable()),
    };
    let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
        return Ok(Map::new());
    };
    let items = object_or_empty(value.get("items").cloned().unwrap_or_default());
    Ok(items
        .into_iter()
        .filter(|(relative, item)| {
            safe_relative_path(relative)
                .is_some_and(|path| is_image_path(&path) && item.is_object())
        })
        .collect())
}
fn update_image_index<F, T>(state: &AppState, update: F) -> Result<T, ApiError>
where
    F: FnOnce(&mut Map<String, Value>) -> Result<T, ApiError>,
{
    let _guard = IMAGE_INDEX_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut items = read_image_index_unlocked(state)?;
    let result = update(&mut items)?;
    write_json(&image_index_path(state), &json!({"items": items}))?;
    Ok(result)
}

fn local_image_index_item(
    relative: &str,
    path: &Path,
    previous: Option<&Map<String, Value>>,
) -> Option<Value> {
    let metadata = fs::metadata(path).ok()?;
    let created_at = previous
        .and_then(|item| item.get("created_at"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| local_timestamp(metadata.modified().unwrap_or(UNIX_EPOCH)));
    let date = previous
        .and_then(|item| item.get("date"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| created_at.get(..10).map(str::to_owned))
        .unwrap_or_else(|| "1970-01-01".to_owned());
    let webdav = previous
        .and_then(|item| item.get("webdav"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut item = previous.cloned().unwrap_or_default();
    item.insert("rel".to_owned(), json!(relative));
    item.insert("path".to_owned(), json!(relative));
    item.insert(
        "name".to_owned(),
        json!(
            path.file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("image")
        ),
    );
    item.insert("date".to_owned(), json!(date));
    item.insert("size".to_owned(), json!(metadata.len()));
    item.insert("created_at".to_owned(), json!(created_at));
    item.insert(
        "storage".to_owned(),
        json!(if webdav { "both" } else { "local" }),
    );
    item.insert("local".to_owned(), json!(true));
    item.insert("webdav".to_owned(), json!(webdav));
    Some(Value::Object(item))
}

fn cleanup_orphaned_image_thumbnails(data_dir: &Path) {
    let images = data_dir.join("images");
    let thumbnails = data_dir.join("image-thumbnails");
    let remote_images = {
        let _guard = IMAGE_INDEX_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        fs::read(data_dir.join("image_index.json"))
            .ok()
            .filter(|bytes| bytes.len() <= MAX_IMAGE_INDEX_BYTES)
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .and_then(|value| value.get("items").cloned())
            .map(object_or_empty)
            .unwrap_or_default()
    };
    for path in walk_regular_files(&thumbnails) {
        let Some(relative) = relative_string(&thumbnails, &path) else {
            continue;
        };
        let image_relative = relative.trim_end_matches(".png");
        let is_remote = remote_images
            .get(image_relative)
            .and_then(|item| item.get("webdav"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if relative.ends_with(".png") && !images.join(image_relative).is_file() && !is_remote {
            let _ = fs::remove_file(path);
        }
    }
}

pub(super) fn cleanup_old_images(state: &AppState) {
    let config = read_config(state);
    let retention_days = super::settings_u64(config.get("image_retention_days"), 30, 1);
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(retention_days.saturating_mul(86_400)))
        .unwrap_or(UNIX_EPOCH);
    let root = image_root(state);
    for path in walk_regular_files(&root) {
        let Ok(metadata) = fs::metadata(&path) else {
            continue;
        };
        if metadata
            .modified()
            .ok()
            .is_some_and(|modified| modified < cutoff)
        {
            let _ = fs::remove_file(path);
        }
    }
    remove_empty_image_dirs(&root);
    cleanup_orphaned_image_thumbnails(state.data_dir.as_ref());
}

fn is_image_path(path: &Path) -> bool {
    matches!(
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "png" | "jpg" | "jpeg" | "webp"
    )
}

fn image_files(state: &AppState) -> Vec<(String, PathBuf)> {
    let root = image_root(state);
    walk_regular_files(&root)
        .into_iter()
        .filter(|path| is_image_path(path))
        .filter_map(|path| relative_string(&root, &path).map(|relative| (relative, path)))
        .collect()
}

fn image_item(
    tags: &Map<String, Value>,
    relative: &str,
    path: Option<&Path>,
    indexed: &Value,
    public_base_url: Option<&str>,
    request_base_url: Option<&str>,
) -> Option<Value> {
    let metadata = match path {
        Some(path) => Some(fs::metadata(path).ok()?),
        None => None,
    };
    let local = metadata.is_some();
    let webdav = indexed
        .get("webdav")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !local && !webdav {
        return None;
    }
    let created_at = indexed
        .get("created_at")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| {
            metadata
                .as_ref()
                .and_then(|metadata| metadata.modified().ok())
                .map(local_timestamp)
        })
        .unwrap_or_default();
    let date = indexed
        .get("date")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| created_at.get(..10).map(str::to_owned))
        .unwrap_or_else(|| "1970-01-01".to_owned());
    let tags = tags.get(relative).cloned().unwrap_or_else(|| json!([]));
    let url = public_base_url
        .filter(|value| !value.is_empty())
        .map(|base| format!("{}/{}", base.trim_end_matches('/'), relative))
        .or_else(|| {
            request_base_url
                .filter(|value| !value.is_empty())
                .map(|base| format!("{}/images/{relative}", base.trim_end_matches('/')))
        })
        .unwrap_or_else(|| format!("/images/{relative}"));
    let thumbnail_url = request_base_url
        .filter(|value| !value.is_empty())
        .map(|base| format!("{}/image-thumbnails/{relative}", base.trim_end_matches('/')))
        .unwrap_or_else(|| format!("/image-thumbnails/{relative}"));
    let name = path
        .and_then(Path::file_name)
        .and_then(|value| value.to_str())
        .or_else(|| indexed.get("name").and_then(Value::as_str))
        .unwrap_or("image");
    let size = metadata
        .as_ref()
        .map(|metadata| metadata.len())
        .or_else(|| indexed.get("size").and_then(Value::as_u64))
        .unwrap_or_default();
    let storage = match (local, webdav) {
        (true, true) => "both",
        (false, true) => "webdav",
        _ => "local",
    };
    let mut item = json!({
        "rel": relative,
        "path": relative,
        "name": name,
        "date": date,
        "size": size,
        "storage": storage,
        "local": local,
        "webdav": webdav,
        "url": url,
        "thumbnail_url": thumbnail_url,
        "created_at": created_at,
        "tags": tags,
    });
    for key in ["remote_url", "width", "height"] {
        if let Some(value) = indexed.get(key) {
            item[key] = value.clone();
        }
    }
    Some(item)
}

pub(super) async fn list_images(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ImageListQuery>,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    cleanup_old_images(&state);
    let start_date = query.start_date.unwrap_or_default().trim().to_owned();
    let end_date = query.end_date.unwrap_or_default().trim().to_owned();
    cleanup_orphaned_image_thumbnails(state.data_dir.as_ref());
    let tags = read_image_tags(&state)?;
    let settings = image_storage_settings(&state);
    let public_base_url = settings
        .get("public_base_url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let request_base_url = super::native_image_base_url_from_headers(&state, &headers, None);
    let _index_guard = IMAGE_INDEX_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut index = read_image_index_unlocked(&state)?;
    let mut index_changed = false;
    let mut items = Vec::new();
    let mut local_paths = HashSet::new();
    for (relative, path) in image_files(&state) {
        local_paths.insert(relative.clone());
        let previous = index.get(&relative).and_then(Value::as_object);
        if let Some(next) = local_image_index_item(&relative, &path, previous) {
            if index.get(&relative) != Some(&next) {
                index.insert(relative.clone(), next.clone());
                index_changed = true;
            }
            if let Some(item) = image_item(
                &tags,
                &relative,
                Some(&path),
                &next,
                public_base_url,
                request_base_url.as_deref(),
            ) {
                let date = item.get("date").and_then(Value::as_str).unwrap_or_default();
                if (!start_date.is_empty() && date < start_date.as_str())
                    || (!end_date.is_empty() && date > end_date.as_str())
                {
                    continue;
                }
                items.push(item);
            }
        }
    }
    let indexed_items = index
        .iter()
        .map(|(relative, item)| (relative.clone(), item.clone()))
        .collect::<Vec<_>>();
    for (relative, indexed) in indexed_items {
        if local_paths.contains(&relative) {
            continue;
        }
        let Some(path) = safe_relative_path(&relative) else {
            continue;
        };
        if !is_image_path(&path) {
            continue;
        }
        if image_root(&state).join(&path).is_file() {
            continue;
        }
        let webdav = indexed
            .get("webdav")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !webdav {
            index.remove(&relative);
            index_changed = true;
            continue;
        }
        let mut normalized = indexed.clone();
        if let Some(object) = normalized.as_object_mut() {
            object.insert("local".to_owned(), json!(false));
            object.insert("storage".to_owned(), json!("webdav"));
        }
        if normalized != indexed {
            index.insert(relative.clone(), normalized.clone());
            index_changed = true;
        }
        if let Some(item) = image_item(
            &tags,
            &relative,
            None,
            &normalized,
            public_base_url,
            request_base_url.as_deref(),
        ) {
            let date = item.get("date").and_then(Value::as_str).unwrap_or_default();
            if (!start_date.is_empty() && date < start_date.as_str())
                || (!end_date.is_empty() && date > end_date.as_str())
            {
                continue;
            }
            items.push(item);
        }
    }
    if index_changed {
        write_json(&image_index_path(&state), &json!({"items": index}))?;
    }
    items.sort_by(|left, right| {
        right
            .get("created_at")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .cmp(
                left.get("created_at")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )
    });
    let mut groups: Vec<Value> = Vec::new();
    for item in &items {
        let date = item
            .get("date")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if let Some(group) = groups.last_mut()
            && group.get("date").and_then(Value::as_str) == Some(date.as_str())
        {
            group["items"]
                .as_array_mut()
                .expect("image group items")
                .push(item.clone());
        } else {
            groups.push(json!({"date": date, "items": [item]}));
        }
    }
    Ok(Json(json!({"items": items, "groups": groups})))
}

fn image_path_from_value(value: &Value) -> Result<PathBuf, ApiError> {
    value
        .as_str()
        .and_then(safe_relative_path)
        .ok_or_else(|| ApiError::management_not_found("image not found"))
}

fn pydantic_management_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::Number(value) if value.as_i64() == Some(0) => Some(false),
        Value::Number(value) if value.as_i64() == Some(1) => Some(true),
        Value::Number(value) if value.as_f64() == Some(0.0) => Some(false),
        Value::Number(value) if value.as_f64() == Some(1.0) => Some(true),
        Value::String(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "t" | "yes" | "y" | "on" => Some(true),
            "0" | "false" | "f" | "no" | "n" | "off" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

pub(super) async fn delete_images(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let value = super::account_json_body(body).await?;
    let object = value.as_object().ok_or_else(ApiError::validation)?;
    let all_matching = object
        .get("all_matching")
        .map(|value| pydantic_management_bool(value).ok_or_else(ApiError::validation))
        .transpose()?
        .unwrap_or(false);
    let start_date = match object.get("start_date") {
        None => "",
        Some(Value::String(value)) => value.trim(),
        Some(_) => return Err(ApiError::validation()),
    };
    let end_date = match object.get("end_date") {
        None => "",
        Some(Value::String(value)) => value.trim(),
        Some(_) => return Err(ApiError::validation()),
    };
    let mut targets = Vec::new();
    if all_matching {
        let listed = list_images(
            State(state.clone()),
            headers.clone(),
            Query(ImageListQuery::default()),
        )
        .await?
        .0;
        if let Some(items) = listed.get("items").and_then(Value::as_array) {
            for item in items {
                let date = item.get("date").and_then(Value::as_str).unwrap_or_default();
                if (start_date.is_empty() || date >= start_date)
                    && (end_date.is_empty() || date <= end_date)
                    && let Some(path) = item.get("path").and_then(Value::as_str)
                {
                    targets.push(path.to_owned());
                }
            }
        }
    } else if let Some(paths) = object.get("paths") {
        let paths = paths.as_array().ok_or_else(ApiError::validation)?;
        for path in paths {
            let raw = path.as_str().ok_or_else(ApiError::validation)?;
            if let Some(relative) = safe_relative_path(raw) {
                targets.push(relative.to_string_lossy().replace('\\', "/"));
            }
        }
    }
    let mut removed = 0usize;
    for relative in targets {
        if delete_stored_image(&state, &relative).await? {
            removed += 1;
        }
    }
    Ok(Json(json!({"removed": removed})))
}

fn remove_empty_image_dirs(root: &Path) {
    let mut directories = Vec::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                pending.push(path.clone());
                directories.push(path);
            }
        }
    }
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        let _ = fs::remove_dir(directory);
    }
}

fn zip_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn zip_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for byte in bytes {
        let mut value = (crc ^ u32::from(*byte)) & 0xff;
        for _ in 0..8 {
            value = if value & 1 != 0 {
                (value >> 1) ^ 0xedb8_8320
            } else {
                value >> 1
            };
        }
        crc = (crc >> 8) ^ value;
    }
    !crc
}

pub(super) fn zip_archive(files: Vec<(String, Vec<u8>)>) -> Result<Vec<u8>, ApiError> {
    if files.is_empty() {
        return Err(ApiError::not_found());
    }
    let mut output = Vec::new();
    let mut central = Vec::new();
    let mut file_count = 0usize;
    for (name, payload) in files {
        if file_count >= usize::from(u16::MAX) {
            return Err(ApiError::validation());
        }
        if output.len().saturating_add(payload.len()) > MAX_IMAGE_ARCHIVE_BYTES {
            return Err(ApiError::validation());
        }
        let name_bytes = name.as_bytes();
        let flags = if name.is_ascii() { 0 } else { 1 << 11 };
        let offset = u32::try_from(output.len()).map_err(|_| ApiError::validation())?;
        let uncompressed_size = u32::try_from(payload.len()).map_err(|_| ApiError::validation())?;
        let checksum = crc32(&payload);
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(&payload)
            .map_err(|_| ApiError::unavailable())?;
        let compressed = encoder.finish().map_err(|_| ApiError::unavailable())?;
        let compressed_size =
            u32::try_from(compressed.len()).map_err(|_| ApiError::validation())?;
        zip_u32(&mut output, 0x0403_4b50);
        zip_u16(&mut output, 20);
        zip_u16(&mut output, flags);
        zip_u16(&mut output, 8);
        zip_u16(&mut output, 0);
        zip_u16(&mut output, 0);
        zip_u32(&mut output, checksum);
        zip_u32(&mut output, compressed_size);
        zip_u32(&mut output, uncompressed_size);
        zip_u16(
            &mut output,
            u16::try_from(name_bytes.len()).map_err(|_| ApiError::validation())?,
        );
        zip_u16(&mut output, 0);
        output.extend_from_slice(name_bytes);
        output.extend_from_slice(&compressed);
        file_count = file_count.saturating_add(1);

        zip_u32(&mut central, 0x0201_4b50);
        zip_u16(&mut central, 20);
        zip_u16(&mut central, 20);
        zip_u16(&mut central, flags);
        zip_u16(&mut central, 8);
        zip_u16(&mut central, 0);
        zip_u16(&mut central, 0);
        zip_u32(&mut central, checksum);
        zip_u32(&mut central, compressed_size);
        zip_u32(&mut central, uncompressed_size);
        zip_u16(
            &mut central,
            u16::try_from(name_bytes.len()).map_err(|_| ApiError::validation())?,
        );
        zip_u16(&mut central, 0);
        zip_u16(&mut central, 0);
        zip_u16(&mut central, 0);
        zip_u16(&mut central, 0);
        zip_u32(&mut central, 0);
        zip_u32(&mut central, offset);
        central.extend_from_slice(name_bytes);
    }
    let central_offset = u32::try_from(output.len()).map_err(|_| ApiError::validation())?;
    let central_size = u32::try_from(central.len()).map_err(|_| ApiError::validation())?;
    output.extend_from_slice(&central);
    zip_u32(&mut output, 0x0605_4b50);
    zip_u16(&mut output, 0);
    zip_u16(&mut output, 0);
    let count = u16::try_from(file_count).map_err(|_| ApiError::validation())?;
    zip_u16(&mut output, count);
    zip_u16(&mut output, count);
    zip_u32(&mut output, central_size);
    zip_u32(&mut output, central_offset);
    zip_u16(&mut output, 0);
    Ok(output)
}

pub(super) async fn download_images(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let value = super::account_json_body(body).await?;
    let paths = value
        .get("paths")
        .and_then(Value::as_array)
        .ok_or_else(ApiError::validation)?;
    if paths.iter().any(|path| !path.is_string()) {
        return Err(ApiError::validation());
    }
    if paths.is_empty() {
        return Err(ApiError::management_not_found("no images found"));
    }
    let mut files = Vec::new();
    let mut used_names = HashSet::new();
    for item in paths {
        let relative = image_path_from_value(item)?;
        let mut name = relative
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("image")
            .to_owned();
        if used_names.contains(&name) {
            let stem = relative
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or("image");
            let extension = relative
                .extension()
                .and_then(|value| value.to_str())
                .unwrap_or_default();
            let mut counter = 2usize;
            loop {
                let candidate = if extension.is_empty() {
                    format!("{stem}_{counter}")
                } else {
                    format!("{stem}_{counter}.{extension}")
                };
                if !used_names.contains(&candidate) {
                    name = candidate;
                    break;
                }
                counter = counter.saturating_add(1);
            }
        }
        if !image_root(&state).join(&relative).is_file() {
            continue;
        }
        let payload = read_stored_image(&state, &relative).await?;
        used_names.insert(name.clone());
        files.push((name, payload));
    }
    if files.is_empty() {
        return Err(ApiError::management_not_found("no images found"));
    }
    let archive = zip_archive(files)?;
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/zip"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"images.zip\"",
            ),
        ],
        Body::from(archive),
    )
        .into_response())
}

pub(super) async fn download_single_image(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(image_path): AxumPath<String>,
) -> Result<Response, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let relative = safe_relative_path(&image_path).ok_or_else(ApiError::invalid_request)?;
    let content_type = if image_root(&state).join(&relative).is_file() {
        image_content_type(&relative)
    } else {
        "image/png"
    };
    let payload = read_stored_image(&state, &relative).await?;
    let filename = relative
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("image.bin")
        .replace('"', "");
    let disposition = format!("attachment; filename=\"{filename}\"");
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content_type),
            (header::CONTENT_DISPOSITION, disposition.as_str()),
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
            (header::ACCESS_CONTROL_ALLOW_METHODS, "GET, OPTIONS"),
            (header::ACCESS_CONTROL_ALLOW_HEADERS, "*"),
        ],
        Body::from(payload),
    )
        .into_response())
}

fn image_stats(state: &AppState) -> Value {
    let root = image_root(state);
    let (disk_total_mb, disk_used_mb, disk_free_mb) =
        match (fs2::total_space(&root), fs2::available_space(&root)) {
            (Ok(total), Ok(free)) => (
                total / 1024 / 1024,
                total.saturating_sub(free) / 1024 / 1024,
                free / 1024 / 1024,
            ),
            _ => (0, 0, 0),
        };
    let mut image_count = 0_u64;
    let mut image_size = 0_u64;
    for path in walk_regular_files(&root) {
        if let Ok(metadata) = fs::metadata(path) {
            image_count += 1;
            image_size = image_size.saturating_add(metadata.len());
        }
    }
    json!({
        "disk_total_mb": disk_total_mb,
        "disk_used_mb": disk_used_mb,
        "disk_free_mb": disk_free_mb,
        "image_count": image_count,
        "image_size_mb": image_size / 1024 / 1024,
        "image_size_bytes": image_size,
    })
}

pub(super) async fn image_storage(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    Ok(Json(image_stats(&state)))
}

fn compress_png(path: &Path) -> Result<Option<usize>, ApiError> {
    let original = fs::metadata(path)
        .map_err(|_| ApiError::unavailable())?
        .len() as usize;
    let reader = ImageReader::open(path)
        .map_err(|_| ApiError::unavailable())?
        .with_guessed_format()
        .map_err(|_| ApiError::unavailable())?;
    let mut decoder = reader.into_decoder().map_err(|_| ApiError::unavailable())?;
    let orientation = decoder.orientation().map_err(|_| ApiError::unavailable())?;
    let mut image =
        image::DynamicImage::from_decoder(decoder).map_err(|_| ApiError::unavailable())?;
    image.apply_orientation(orientation);
    image = if image.has_alpha() {
        image::DynamicImage::ImageRgba8(image.to_rgba8())
    } else {
        image::DynamicImage::ImageRgb8(image.to_rgb8())
    };
    let mut output = Cursor::new(Vec::new());
    image::codecs::png::PngEncoder::new_with_quality(
        &mut output,
        image::codecs::png::CompressionType::Best,
        image::codecs::png::FilterType::Adaptive,
    )
    .write_image(
        image.as_bytes(),
        image.width(),
        image.height(),
        image.color().into(),
    )
    .map_err(|_| ApiError::unavailable())?;
    let payload = output.into_inner();
    if payload.len() >= original {
        return Ok(None);
    }
    write_atomic(path, &payload)?;
    Ok(Some(original - payload.len()))
}

fn safe_regular_file(root: &Path, relative: &Path) -> Result<PathBuf, ApiError> {
    let mut current = root.to_owned();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err(ApiError::invalid_request());
        };
        current.push(name);
        let metadata = fs::symlink_metadata(&current).map_err(|_| ApiError::not_found())?;
        if metadata.file_type().is_symlink() {
            return Err(ApiError::invalid_request());
        }
    }
    if !fs::symlink_metadata(&current)
        .map_err(|_| ApiError::not_found())?
        .is_file()
    {
        return Err(ApiError::not_found());
    }
    Ok(current)
}

pub(super) async fn compress_images(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let mut compressed = 0_u64;
    let mut saved_bytes = 0_u64;
    for (_relative, path) in image_files(&state)
        .into_iter()
        .filter(|(_, path)| path.extension().and_then(|value| value.to_str()) == Some("png"))
    {
        if let Ok(Some(saved)) = compress_png(&path) {
            compressed += 1;
            saved_bytes = saved_bytes.saturating_add(saved as u64);
        }
    }
    Ok(Json(json!({
        "compressed": compressed,
        "saved_bytes": saved_bytes,
        "saved_mb": saved_bytes / 1024 / 1024,
    })))
}

fn cleanup_query_target(value: Option<&str>) -> Result<i64, ApiError> {
    match value {
        None => Ok(500),
        Some(value) => super::parse_python_integer_string(value).ok_or_else(ApiError::validation),
    }
}

fn cleanup_query_bool(value: Option<&str>) -> Result<bool, ApiError> {
    value
        .map(|value| {
            pydantic_management_bool(&Value::String(value.to_owned()))
                .ok_or_else(ApiError::validation)
        })
        .transpose()
        .map(|value| value.unwrap_or(false))
}

pub(super) async fn cleanup_images(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ImageCleanupQuery>,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let target_free_mb = cleanup_query_target(query.target_free_mb.as_deref())?;
    let dry_run = cleanup_query_bool(query.dry_run.as_deref())?;
    Ok(Json(cleanup_images_to_target(
        &state,
        target_free_mb,
        dry_run,
    )?))
}

fn cleanup_images_to_target(
    state: &AppState,
    target_free_mb: i64,
    dry_run: bool,
) -> Result<Value, ApiError> {
    let target_free = u64::try_from(target_free_mb).unwrap_or_default();
    let root = image_root(state);
    let current_free_mb = fs2::available_space(&root)
        .map(|value| value / 1024 / 1024)
        .map_err(|_| ApiError::unavailable())?;
    if current_free_mb >= target_free && !dry_run {
        return Ok(json!({
            "removed": 0,
            "current_free_mb": current_free_mb,
            "target_free_mb": target_free_mb,
            "done": true,
        }));
    }
    let mut candidates = image_files(state)
        .into_iter()
        .filter(|(_, path)| path.extension().and_then(|value| value.to_str()) == Some("png"))
        .filter_map(|(relative, path)| {
            let metadata = fs::metadata(&path).ok()?;
            let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
            Some((modified, relative, path, metadata.len()))
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|item| item.0);
    let mut removed = 0_u64;
    let mut freed_bytes = 0_u64;
    for (_modified, relative, path, size) in candidates {
        if current_free_mb.saturating_add(freed_bytes / 1024 / 1024) >= target_free {
            break;
        }
        if !dry_run {
            fs::remove_file(&path).map_err(|_| ApiError::unavailable())?;
            let safe = safe_relative_path(&relative).ok_or_else(ApiError::invalid_request)?;
            let thumbnail_root = state.data_dir.join("image-thumbnails");
            let _ = fs::remove_file(thumbnail_root.join(format!("{relative}.png")));
            let _ = fs::remove_file(thumbnail_root.join(&safe));
            super::remove_image_tag_for_path(state, &relative)?;
            update_image_index(state, |items| {
                let stored_webdav = items
                    .get(&relative)
                    .and_then(|item| item.get("webdav"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if stored_webdav {
                    if let Some(object) = items.get_mut(&relative).and_then(Value::as_object_mut) {
                        object.insert("local".to_owned(), json!(false));
                        object.insert("storage".to_owned(), json!("webdav"));
                    }
                } else {
                    items.remove(&relative);
                }
                Ok(())
            })?;
        }
        removed += 1;
        freed_bytes = freed_bytes.saturating_add(size);
    }
    if !dry_run {
        remove_empty_image_dirs(&root);
        remove_empty_image_dirs(&state.data_dir.join("image-thumbnails"));
    }
    let projected_free = current_free_mb.saturating_add(freed_bytes / 1024 / 1024);
    let done = projected_free >= target_free;
    Ok(json!({
        "removed": removed,
        "freed_mb": freed_bytes / 1024 / 1024,
        "target_free_mb": target_free_mb,
        "current_free_mb": projected_free,
        "done": done,
        "dry_run": dry_run,
    }))
}

pub(super) fn cleanup_periodic_image_storage(state: &AppState) {
    cleanup_old_images(state);
    let root = image_root(state);
    let Ok(free_bytes) = fs2::available_space(&root) else {
        return;
    };
    let free_mb = free_bytes / 1024 / 1024;
    if free_mb < 500 {
        let _ = cleanup_images_to_target(state, 500, false);
    }
}

fn public_log_detail(value: &Value) -> Value {
    let Some(object) = value.as_object() else {
        return json!({});
    };
    let allowed = [
        "source",
        "status",
        "rotated",
        "added",
        "skipped",
        "removed",
        "total",
        "refreshed",
        "failed",
        "auto_remove",
        "reason",
        "key_id",
        "key_name",
        "role",
        "endpoint",
        "model",
        "request_text",
        "account_email",
        "conversation_id",
        "error",
        "result",
        "started_at",
        "ended_at",
        "duration_ms",
        "request_shape",
        "urls",
    ];
    let mut projected = Map::new();
    for key in allowed {
        let Some(item) = object.get(key) else {
            continue;
        };
        match item {
            Value::Bool(_) | Value::Number(_) => {
                projected.insert(key.to_owned(), item.clone());
            }
            Value::String(value) => {
                projected.insert(
                    key.to_owned(),
                    Value::String(value.chars().take(4096).collect()),
                );
            }
            Value::Array(values) if key == "urls" => {
                let urls = values
                    .iter()
                    .filter_map(Value::as_str)
                    .take(100)
                    .map(|value| value.chars().take(2048).collect::<String>())
                    .map(Value::String)
                    .collect::<Vec<_>>();
                projected.insert(key.to_owned(), Value::Array(urls));
            }
            Value::Object(object) if key == "request_shape" => {
                let shape = object
                    .iter()
                    .filter(|(key, value)| {
                        matches!(
                            key.as_str(),
                            "response_message_items"
                                | "input_image_parts"
                                | "image_url_parts"
                                | "image_parts"
                                | "data_url_images"
                                | "remote_image_urls"
                                | "literal_image_placeholders"
                        ) && value.as_u64().is_some_and(|value| value <= 1_000_000_000)
                    })
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<Map<_, _>>();
                projected.insert(key.to_owned(), Value::Object(shape));
            }
            Value::Object(object) if key == "result" => {
                let result = object
                    .iter()
                    .filter(|(key, value)| {
                        matches!(key.as_str(), "primary_url" | "zip_url" | "conversation_id")
                            && value.as_str().is_some_and(|value| value.len() <= 4096)
                    })
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<Map<_, _>>();
                projected.insert(key.to_owned(), Value::Object(result));
            }
            _ => {}
        }
    }
    Value::Object(projected)
}

/// Append one public call/account log record. Logging is best-effort: a log
/// failure must never change the API response or make an upstream request fail.
pub(super) fn append_log(state: &AppState, log_type: &str, summary: &str, detail: Value) {
    let timestamp = image_local_timestamp();
    let mut record = Map::new();
    record.insert("id".to_owned(), Value::String(log_uuid_hex()));
    record.insert("time".to_owned(), Value::String(timestamp));
    record.insert("type".to_owned(), Value::String(log_type.to_owned()));
    record.insert("summary".to_owned(), Value::String(summary.to_owned()));
    record.insert("detail".to_owned(), detail);
    let Ok(bytes) = serde_json::to_vec(&Value::Object(record)) else {
        return;
    };
    let path = data_file(state, "logs.jsonl");
    let _guard = LOG_WRITE_GATE.lock().ok();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open(path) else {
        return;
    };
    let _ = file.write_all(&bytes);
    let _ = file.write_all(b"\n");
}

pub(super) fn append_call_log(state: &AppState, summary: &str, detail: Value) {
    append_log(state, "call", summary, detail);
}

pub(super) fn append_account_log(state: &AppState, summary: &str, detail: Value) {
    append_log(state, "account", summary, detail);
}

fn log_id(raw: &Map<String, Value>, line: &str, ordinal: usize) -> String {
    if let Some(value) = raw.get("id") {
        let id = super::protocol_anthropic::python_text(Some(value));
        if !id.trim().is_empty() {
            return id;
        }
    }
    let mut hasher = Sha1::new();
    hasher.update(ordinal.to_string().as_bytes());
    hasher.update(b":");
    hasher.update(line.as_bytes());
    let digest = hasher.finalize();
    digest[..12]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn project_log(line: &str, ordinal: usize) -> Option<Value> {
    let raw = serde_json::from_str::<Value>(line).ok()?;
    let object = raw.as_object()?;
    let mut public = Map::new();
    public.insert(
        "id".to_owned(),
        Value::String(log_id(object, line, ordinal)),
    );
    for key in ["time", "type", "summary"] {
        if let Some(value) = object.get(key).and_then(Value::as_str) {
            public.insert(
                key.to_owned(),
                Value::String(value.chars().take(4096).collect()),
            );
        }
    }
    if let Some(detail) = object.get("detail") {
        public.insert("detail".to_owned(), public_log_detail(detail));
    }
    Some(Value::Object(public))
}

fn log_values(state: &AppState) -> Result<Vec<Value>, ApiError> {
    let path = data_file(state, "logs.jsonl");
    if !path.is_file() {
        return Ok(Vec::new());
    }
    let bytes = read_bounded(&path, MAX_LOG_BYTES)?;
    let text = String::from_utf8_lossy(&bytes);
    let lines = text.lines().collect::<Vec<_>>();
    Ok(lines
        .into_iter()
        .enumerate()
        .rev()
        .filter_map(|(ordinal, line)| project_log(line.trim_end_matches('\r'), ordinal))
        .collect())
}

pub(super) async fn list_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<LogQuery>,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let type_filter = query.r#type.unwrap_or_default().trim().to_owned();
    let start_date = query.start_date.unwrap_or_default().trim().to_owned();
    let end_date = query.end_date.unwrap_or_default().trim().to_owned();
    let items = log_values(&state)?
        .into_iter()
        .filter(|item| {
            let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
            let day = item
                .get("time")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .get(..10)
                .unwrap_or_default();
            (type_filter.is_empty() || item_type == type_filter)
                && (start_date.is_empty() || day >= start_date.as_str())
                && (end_date.is_empty() || day <= end_date.as_str())
        })
        .take(MAX_LOG_ITEMS)
        .collect::<Vec<_>>();
    Ok(Json(json!({"items": items})))
}

pub(super) async fn delete_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let value = super::account_json_body(body).await?;
    let object = value.as_object().ok_or_else(ApiError::validation)?;
    let ids = match object.get("ids") {
        None => &[][..],
        Some(Value::Array(ids)) => ids.as_slice(),
        Some(_) => return Err(ApiError::validation()),
    };
    if ids.len() > MAX_LOG_ITEMS * 10 || ids.iter().any(|value| !value.is_string()) {
        return Err(ApiError::validation());
    }
    let wanted = ids
        .iter()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .map(ToOwned::to_owned)
        .collect::<std::collections::HashSet<_>>();
    let path = data_file(&state, "logs.jsonl");
    if wanted.is_empty() || !path.is_file() {
        return Ok(Json(json!({"removed": 0})));
    }
    let bytes = read_bounded(&path, MAX_LOG_BYTES)?;
    let mut kept = Vec::new();
    let mut removed = 0usize;
    for (ordinal, line) in String::from_utf8_lossy(&bytes).lines().enumerate() {
        let line = line.trim_end_matches('\r');
        let Ok(raw) = serde_json::from_str::<Value>(line) else {
            kept.extend_from_slice(line.as_bytes());
            kept.push(b'\n');
            continue;
        };
        let Some(object) = raw.as_object() else {
            kept.extend_from_slice(line.as_bytes());
            kept.push(b'\n');
            continue;
        };
        let id = log_id(object, line, ordinal);
        if wanted.contains(&id) {
            removed += 1;
        } else {
            let mut preserved = object.clone();
            preserved.insert("id".to_owned(), Value::String(id));
            let encoded = serde_json::to_vec(&Value::Object(preserved))
                .map_err(|_| ApiError::unavailable())?;
            kept.extend_from_slice(&encoded);
            kept.push(b'\n');
        }
    }
    write_atomic(&path, &kept)?;
    Ok(Json(json!({"removed": removed})))
}

fn config_path(state: &AppState) -> PathBuf {
    state.config_path.as_ref().clone()
}

fn read_config(state: &AppState) -> Value {
    fs::read(config_path(state))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}))
}

fn public_url(value: Option<&Value>) -> String {
    let Some(value) = value.and_then(Value::as_str) else {
        return String::new();
    };
    let Ok(mut url) = url::Url::parse(value.trim()) else {
        return String::new();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

fn secret_mask(value: Option<&Value>) -> Value {
    if value
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
    {
        Value::String("********".to_owned())
    } else {
        Value::String(String::new())
    }
}

fn bool_or(value: Option<&Value>, fallback: bool) -> bool {
    super::settings_bool(value, fallback)
}

fn merge_missing_object(target: &mut Map<String, Value>, defaults: &Map<String, Value>) {
    for (key, default_value) in defaults {
        match (target.get_mut(key), default_value) {
            (Some(Value::Object(target_object)), Value::Object(default_object)) => {
                merge_missing_object(target_object, default_object);
            }
            (Some(_), _) => {}
            (None, _) => {
                target.insert(key.clone(), default_value.clone());
            }
        }
    }
}

fn runtime_value(state: &AppState) -> Value {
    let config = read_config(state);
    let defaults = config::proxy_runtime_defaults_from_environment();
    let raw = config
        .get("proxy_runtime")
        .cloned()
        .unwrap_or_else(|| defaults.clone());
    let mut object = object_or_empty(raw);
    let default_object = object_or_empty(defaults);
    merge_missing_object(&mut object, &default_object);
    super::normalize_proxy_runtime(Some(&Value::Object(object)), true)
}

fn runtime_status(state: &AppState) -> Value {
    let runtime = runtime_value(state);
    let enabled = runtime
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let egress_mode = runtime
        .get("egress_mode")
        .and_then(Value::as_str)
        .unwrap_or("direct");
    let runtime_proxy = if enabled && egress_mode == "single_proxy" {
        runtime
            .get("proxy_url")
            .and_then(Value::as_str)
            .unwrap_or_default()
    } else {
        ""
    };
    let (proxy_source, has_proxy) = if !runtime_proxy.is_empty() {
        ("runtime", true)
    } else if !legacy_proxy_value(state).is_empty() {
        ("global", true)
    } else {
        ("direct", false)
    };
    let clearance = runtime.get("clearance").unwrap_or(&Value::Null);
    let clearance_enabled = enabled
        && clearance
            .get("enabled")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        && matches!(
            clearance.get("mode").and_then(Value::as_str),
            Some("manual" | "flaresolverr")
        );
    json!({
        "enabled": enabled,
        "egress_mode": egress_mode,
        "proxy_source": proxy_source,
        "has_proxy": has_proxy,
        "clearance_enabled": clearance_enabled,
        "clearance_mode": clearance.get("mode").cloned().unwrap_or_else(|| json!("none")),
        "has_clearance_bundle": !state.clearance_store.cached_hosts_now().is_empty(),
        "cached_clearance_hosts": state.clearance_store.cached_hosts_now(),
    })
}

pub(super) fn health_proxy_runtime(state: &AppState) -> Value {
    runtime_status(state)
}

fn legacy_proxy_value(state: &AppState) -> String {
    read_config(state)
        .get("proxy")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned()
}

pub(super) async fn proxy_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let raw = legacy_proxy_value(&state);
    let public = public_url(Some(&Value::String(raw)));
    Ok(Json(
        json!({"proxy": {"enabled": !public.is_empty(), "url": public}}),
    ))
}

pub(super) async fn update_proxy_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let value = super::account_json_body(body).await?;
    let object = value.as_object().ok_or_else(ApiError::invalid_request)?;
    let mut config = object_or_empty(read_config(&state));
    let mut raw = config
        .get("proxy")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    if let Some(url) = object.get("url") {
        raw = url
            .as_str()
            .map(str::trim)
            .filter(|value| value.len() <= 2048)
            .ok_or_else(ApiError::invalid_request)?
            .to_owned();
    }
    if object.get("enabled").and_then(Value::as_bool) == Some(false) {
        raw.clear();
    }
    if !raw.is_empty() && public_url(Some(&Value::String(raw.clone()))).is_empty() {
        return Err(ApiError::invalid_request());
    }
    config.insert("proxy".to_owned(), Value::String(raw));
    write_json(&config_path(&state), &Value::Object(config))?;
    let public = public_url(Some(&Value::String(legacy_proxy_value(&state))));
    Ok(Json(
        json!({"proxy": {"enabled": !public.is_empty(), "url": public}}),
    ))
}

pub(super) async fn proxy_runtime(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let mut status = runtime_status(&state);
    let cached_hosts = state.clearance_store.hosts().await;
    status["has_clearance_bundle"] = Value::Bool(!cached_hosts.is_empty());
    status["cached_clearance_hosts"] = json!(cached_hosts);
    Ok(Json(
        json!({"runtime": runtime_value(&state), "status": status}),
    ))
}

pub(super) async fn update_proxy_runtime(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let incoming = super::account_json_body(body).await?;
    let incoming = incoming.as_object().ok_or_else(ApiError::invalid_request)?;
    let mut config = object_or_empty(read_config(&state));
    let mut runtime = config
        .remove("proxy_runtime")
        .map(object_or_empty)
        .unwrap_or_default();
    for (key, value) in incoming {
        if key == "clearance" {
            let mut clearance = runtime
                .remove("clearance")
                .map(object_or_empty)
                .unwrap_or_default();
            if let Some(update) = value.as_object() {
                for (field, field_value) in update {
                    if matches!(field.as_str(), "cf_cookies" | "cf_clearance")
                        && field_value.as_str() == Some("")
                    {
                        continue;
                    }
                    clearance.insert(field.clone(), field_value.clone());
                }
            } else {
                return Err(ApiError::invalid_request());
            }
            runtime.insert(key.clone(), Value::Object(clearance));
        } else {
            runtime.insert(key.clone(), value.clone());
        }
    }
    let runtime = super::normalize_proxy_runtime(Some(&Value::Object(runtime)), false);
    config.insert("proxy_runtime".to_owned(), runtime);
    write_json(&config_path(&state), &Value::Object(config))?;
    Ok(Json(
        json!({"runtime": runtime_value(&state), "status": runtime_status(&state)}),
    ))
}

fn proxy_result_base(source: &str, candidate: &str) -> Map<String, Value> {
    [
        ("proxy_source".to_owned(), json!(source)),
        ("has_proxy".to_owned(), json!(!candidate.is_empty())),
    ]
    .into_iter()
    .collect()
}

pub(super) async fn test_proxy(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let value = super::account_json_body(body).await?;
    let object = value.as_object().ok_or_else(ApiError::validation)?;
    let input = match object.get("url") {
        None => "",
        Some(Value::String(value)) => value.trim(),
        Some(_) => return Err(ApiError::validation()),
    };
    let runtime = runtime_value(&state);
    let (candidate, source) = if input.is_empty() {
        let profile = super::proxy_service::profile_from_runtime(
            &runtime,
            None,
            None,
            Some(&legacy_proxy_value(&state)),
            false,
            true,
        );
        (profile.proxy_url, profile.proxy_source)
    } else {
        (
            super::proxy_service::normalize_proxy_url(input),
            "input".to_owned(),
        )
    };
    let mut result = proxy_result_base(&source, &candidate);
    if candidate.is_empty() {
        result.extend([
            ("ok".to_owned(), json!(false)),
            ("status".to_owned(), json!(0)),
            ("latency_ms".to_owned(), json!(0)),
            ("error".to_owned(), json!("no active proxy configured")),
        ]);
        return Ok(Json(json!({"result": Value::Object(result)})));
    }
    let started = std::time::Instant::now();
    let proxy = match reqwest::Proxy::all(candidate.clone()) {
        Ok(proxy) => proxy,
        Err(_) => {
            result.extend([
                ("ok".to_owned(), json!(false)),
                ("status".to_owned(), json!(0)),
                ("latency_ms".to_owned(), json!(0)),
                ("error".to_owned(), json!("invalid proxy url")),
            ]);
            return Ok(Json(json!({"result": Value::Object(result)})));
        }
    };
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .proxy(proxy)
        .build()
        .map_err(|_| ApiError::unavailable())?;
    let response = client
        .get("https://chatgpt.com/api/auth/csrf")
        .header(header::USER_AGENT, "Mozilla/5.0 (chatgpt2api proxy test)")
        .send()
        .await;
    let latency = started.elapsed().as_millis().min(u128::from(u32::MAX)) as u32;
    match response {
        Ok(response) => {
            let status = response.status().as_u16();
            result.extend([
                ("ok".to_owned(), json!(status < 500)),
                ("status".to_owned(), json!(status)),
                ("latency_ms".to_owned(), json!(latency)),
                (
                    "error".to_owned(),
                    if status < 500 {
                        Value::Null
                    } else {
                        json!(format!("HTTP {status}"))
                    },
                ),
            ]);
        }
        Err(_) => {
            result.extend([
                ("ok".to_owned(), json!(false)),
                ("status".to_owned(), json!(0)),
                ("latency_ms".to_owned(), json!(latency)),
                ("error".to_owned(), json!("代理测试失败，请稍后重试")),
            ]);
        }
    }
    Ok(Json(json!({"result": Value::Object(result)})))
}

pub(super) async fn test_clearance(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let request = super::account_json_body(body).await?;
    let request = request.as_object().ok_or_else(ApiError::validation)?;
    let runtime = runtime_status(&state);
    let enabled = runtime
        .get("clearance_enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let target_url = match request.get("target_url") {
        None => "https://chatgpt.com".to_owned(),
        Some(Value::String(value)) => {
            let value = value.trim();
            if value.is_empty() {
                "https://chatgpt.com".to_owned()
            } else {
                value.to_owned()
            }
        }
        Some(_) => return Err(ApiError::validation()),
    };
    let config = runtime_value(&state);
    let clearance = config.get("clearance").unwrap_or(&Value::Null);
    let mode = clearance
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("none");
    let legacy_proxy = legacy_proxy_value(&state);
    let profile = super::proxy_service::profile_from_runtime(
        &config,
        None,
        None,
        Some(&legacy_proxy),
        false,
        true,
    );
    let proxy_url = profile.proxy_url;
    let timeout_sec = clearance
        .get("timeout_sec")
        .and_then(Value::as_u64)
        .unwrap_or(60)
        .clamp(1, 300);
    let started = std::time::Instant::now();
    let result = if !enabled {
        json!({
            "ok": false, "status": "disabled", "latency_ms": 0,
            "has_cookies": false, "user_agent": "", "error": "clearance is disabled",
            "runtime": runtime,
        })
    } else if mode == "manual" {
        let cookies = clearance
            .get("cf_cookies")
            .and_then(Value::as_str)
            .unwrap_or("");
        let cf_clearance = clearance
            .get("cf_clearance")
            .and_then(Value::as_str)
            .unwrap_or("");
        let user_agent = clearance
            .get("user_agent")
            .and_then(Value::as_str)
            .unwrap_or("");
        let mut cookies_map = super::proxy_service::parse_cookie_header(cookies);
        if !cf_clearance.trim().is_empty() {
            cookies_map
                .entry("cf_clearance".to_owned())
                .or_insert_with(|| cf_clearance.trim().to_owned());
        }
        if !cookies_map.is_empty() || !user_agent.trim().is_empty() {
            state
                .clearance_store
                .put(
                    &proxy_url,
                    &target_url,
                    super::proxy_service::ClearanceBundle {
                        target_host: super::proxy_service::normalize_host(&target_url),
                        proxy_url: super::proxy_service::normalize_proxy_url(&proxy_url),
                        cookies: cookies_map,
                        user_agent: user_agent.trim().to_owned(),
                        expires_at: None,
                    },
                    clearance
                        .get("refresh_interval")
                        .and_then(Value::as_u64)
                        .unwrap_or(3600),
                )
                .await;
        }
        json!({
            "ok": !cookies.trim().is_empty() || !cf_clearance.trim().is_empty() || !user_agent.trim().is_empty(),
            "status": "ok",
            "latency_ms": started.elapsed().as_millis(),
            "has_cookies": !cookies.trim().is_empty() || !cf_clearance.trim().is_empty(),
            "user_agent": user_agent,
            "error": Value::Null,
            "runtime": runtime,
        })
    } else {
        let flaresolverr_url = clearance
            .get("flaresolverr_url")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim_end_matches('/');
        if flaresolverr_url.is_empty() {
            return Ok(Json(json!({"result": {
                "ok": false, "status": "failed", "latency_ms": 0,
                "has_cookies": false, "user_agent": "",
                "error": "FlareSolverr URL is not configured", "runtime": runtime,
            }})));
        }
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_sec + 5))
            .build()
            .map_err(|_| ApiError::unavailable())?;
        let response = client
            .post(format!("{flaresolverr_url}/v1"))
            .json(&flaresolverr_payload(&target_url, &proxy_url, timeout_sec))
            .send()
            .await;
        let latency_ms = started.elapsed().as_millis();
        match response {
            Ok(response) if response.status().is_success() => {
                let payload = response.json::<Value>().await.unwrap_or(Value::Null);
                if let Some(bundle) = parse_flaresolverr_bundle(&payload, &target_url, &proxy_url) {
                    state
                        .clearance_store
                        .put(
                            &proxy_url,
                            &target_url,
                            bundle.clone(),
                            clearance
                                .get("refresh_interval")
                                .and_then(Value::as_u64)
                                .unwrap_or(3600),
                        )
                        .await;
                    json!({
                        "ok": true, "status": "ok", "latency_ms": latency_ms,
                        "has_cookies": !bundle.cookies.is_empty(),
                        "user_agent": bundle.user_agent, "error": Value::Null,
                        "runtime": runtime,
                    })
                } else {
                    json!({
                        "ok": false, "status": "failed", "latency_ms": latency_ms,
                        "has_cookies": false, "user_agent": "",
                        "error": "FlareSolverr returned no clearance bundle", "runtime": runtime,
                    })
                }
            }
            Ok(response) => json!({
                "ok": false, "status": "failed", "latency_ms": latency_ms,
                "has_cookies": false, "user_agent": "",
                "error": format!("FlareSolverr HTTP {}", response.status()), "runtime": runtime,
            }),
            Err(error) => json!({
                "ok": false, "status": "error", "latency_ms": latency_ms,
                "has_cookies": false, "user_agent": "",
                "error": error.to_string(), "runtime": runtime,
            }),
        }
    };
    Ok(Json(json!({"result": result})))
}

fn image_storage_settings(state: &AppState) -> Map<String, Value> {
    let config = read_config(state);
    object_or_empty(super::normalize_image_storage(
        config.get("image_storage"),
        false,
    ))
}

fn normalized_remote_url(value: Option<&Value>) -> Result<String, ApiError> {
    let text = value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= 2048)
        .ok_or_else(ApiError::invalid_request)?;
    let url = url::Url::parse(text).map_err(|_| ApiError::invalid_request())?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ApiError::invalid_request());
    }
    Ok(text.trim_end_matches('/').to_owned())
}

fn registry_items(state: &AppState, kind: &str) -> Vec<Value> {
    super::read_server_registry(state, kind)
}

pub(crate) fn recover_unfinished_import_jobs(state: &AppState) {
    for kind in ["cpa_pools", "sub2api", "ccload"] {
        let values = super::read_server_registry(state, kind);
        if !values.iter().any(import_job_is_unfinished) {
            continue;
        }
        let _ = super::mutate_server_registry(state, kind, |items| {
            for item in items {
                let Some(job) = item.get_mut("import_job").and_then(Value::as_object_mut) else {
                    continue;
                };
                if !matches!(
                    job.get("status").and_then(Value::as_str),
                    Some("pending" | "running")
                ) {
                    continue;
                }
                mark_interrupted_import_job(job, kind == "ccload");
            }
            Ok(())
        });
    }
}

fn mark_interrupted_import_job(job: &mut Map<String, Value>, rust_extension: bool) {
    job.insert("status".to_owned(), Value::String("failed".to_owned()));
    if !rust_extension {
        return;
    }
    let total = job.get("total").and_then(Value::as_u64).unwrap_or_default();
    let added = job.get("added").and_then(Value::as_u64).unwrap_or_default();
    let skipped = job
        .get("skipped")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let failed = total.saturating_sub(added.saturating_add(skipped));
    job.insert("completed".to_owned(), Value::from(total));
    job.insert("failed".to_owned(), Value::from(failed));
    job.insert(
        "updated_at".to_owned(),
        Value::String(iso_timestamp(SystemTime::now())),
    );
    job.insert(
        "errors".to_owned(),
        json!([{"name":"import","error":"导入任务在服务更新时中断"}]),
    );
}

fn import_job_is_unfinished(value: &Value) -> bool {
    matches!(
        value
            .get("import_job")
            .and_then(Value::as_object)
            .and_then(|job| job.get("status"))
            .and_then(Value::as_str),
        Some("pending" | "running")
    )
}

fn registry_item(state: &AppState, kind: &str, id: &str) -> Option<Value> {
    registry_items(state, kind)
        .into_iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(id))
}

fn public_import_job(value: Option<&Value>) -> Value {
    let Some(object) = value.and_then(Value::as_object) else {
        return Value::Null;
    };
    let mut output = Map::new();
    for key in [
        "job_id",
        "status",
        "phase",
        "phase_completed",
        "phase_total",
        "created_at",
        "updated_at",
        "total",
        "completed",
        "added",
        "skipped",
        "refreshed",
        "failed",
        "model_fetch_count",
        "model_attempt_count",
        "model_cache_hit_count",
        "model_fetch_by_type",
        "model_cache_hit_by_type",
        "model_retry_count",
        "model_fetch_count_batch",
        "model_attempt_count_batch",
        "model_cache_hit_count_batch",
        "model_retry_count_batch",
        "model_fetch_by_type_batch",
        "model_cache_hit_by_type_batch",
        "model_retry_by_type_batch",
        "errors",
    ] {
        if let Some(value) = object.get(key) {
            output.insert(key.to_owned(), value.clone());
        }
    }
    let now = python_iso_timestamp(SystemTime::now());
    if output.get("job_id").is_none_or(Value::is_null) {
        output.insert("job_id".to_owned(), Value::String(random_hex_id(16)));
    }
    if output.get("status").is_none_or(Value::is_null) {
        output.insert("status".to_owned(), Value::String("failed".to_owned()));
    }
    if output.get("created_at").is_none_or(Value::is_null) {
        output.insert("created_at".to_owned(), Value::String(now.clone()));
    }
    if output.get("updated_at").is_none_or(Value::is_null) {
        let created = output
            .get("created_at")
            .cloned()
            .unwrap_or_else(|| Value::String(now.clone()));
        output.insert("updated_at".to_owned(), created);
    }
    for key in [
        "total",
        "completed",
        "added",
        "skipped",
        "refreshed",
        "failed",
    ] {
        if output.get(key).is_none_or(Value::is_null) {
            output.insert(key.to_owned(), Value::from(0));
        }
    }
    if !output.get("errors").is_some_and(Value::is_array) {
        output.insert("errors".to_owned(), Value::Array(Vec::new()));
    }
    Value::Object(output)
}

async fn add_model_catalog_stats(
    job: &mut Value,
    state: &AppState,
    batch: &super::ImportedModelCatalogBatchStats,
) {
    let (fetch_count, cache_hit_count) = state.imported_model_catalog.stats();
    let attempt_count = state.imported_model_catalog.attempt_count();
    let retry_count = state.imported_model_catalog.retry_count();
    let by_type = state.imported_model_catalog.stats_by_type().await;
    let batch_snapshot = batch.snapshot();
    let batch_attempt_count = batch_snapshot.attempts;
    let batch_retry_count = batch_snapshot.retries;
    let batch_fetch_count = batch_snapshot.fetches;
    let batch_cache_hit_count = batch_snapshot.cache_hits;
    let batch_by_type = batch_snapshot.by_type;
    let batch_by_type = batch_by_type
        .into_iter()
        .filter(|(_, values)| values.iter().any(|value| *value > 0))
        .collect::<Vec<_>>();
    if let Some(object) = job.as_object_mut() {
        object.insert("model_fetch_count".to_owned(), Value::from(fetch_count));
        object.insert("model_attempt_count".to_owned(), Value::from(attempt_count));
        object.insert(
            "model_cache_hit_count".to_owned(),
            Value::from(cache_hit_count),
        );
        object.insert(
            "model_fetch_by_type".to_owned(),
            Value::Array(
                by_type
                    .iter()
                    .map(|(account_type, fetches, _, _, _)| {
                        json!({"account_type": account_type, "count": fetches})
                    })
                    .collect(),
            ),
        );
        object.insert(
            "model_cache_hit_by_type".to_owned(),
            Value::Array(
                by_type
                    .iter()
                    .map(|(account_type, _, _, hits, _)| {
                        json!({"account_type": account_type, "count": hits})
                    })
                    .collect(),
            ),
        );
        object.insert("model_retry_count".to_owned(), Value::from(retry_count));
        object.insert(
            "model_fetch_count_batch".to_owned(),
            Value::from(batch_fetch_count),
        );
        object.insert(
            "model_attempt_count_batch".to_owned(),
            Value::from(batch_attempt_count),
        );
        object.insert(
            "model_cache_hit_count_batch".to_owned(),
            Value::from(batch_cache_hit_count),
        );
        object.insert(
            "model_retry_count_batch".to_owned(),
            Value::from(batch_retry_count),
        );
        object.insert(
            "model_fetch_by_type_batch".to_owned(),
            Value::Array(
                batch_by_type
                    .iter()
                    .map(|(account_type, values)| {
                        json!({"account_type": account_type, "count": values[2]})
                    })
                    .collect(),
            ),
        );
        object.insert(
            "model_cache_hit_by_type_batch".to_owned(),
            Value::Array(
                batch_by_type
                    .iter()
                    .map(|(account_type, values)| {
                        json!({"account_type": account_type, "count": values[3]})
                    })
                    .collect(),
            ),
        );
        object.insert(
            "model_retry_by_type_batch".to_owned(),
            Value::Array(
                batch_by_type
                    .iter()
                    .map(|(account_type, values)| {
                        json!({"account_type": account_type, "count": values[1]})
                    })
                    .collect(),
            ),
        );
    }
}

fn public_registry_item(kind: &str, value: &Value) -> Value {
    let object = value.as_object().cloned().unwrap_or_default();
    let id = Value::String(bounded_public_text(object.get("id"), 256));
    let name = Value::String(bounded_public_text(object.get("name"), 256));
    let base_url_text = bounded_public_text(object.get("base_url"), 16 * 1024);
    let base_url = Value::String(public_url(Some(&Value::String(base_url_text))));
    let job = public_import_job(object.get("import_job"));
    match kind {
        "cpa_pools" => json!({
            "id": id,
            "name": name,
            "base_url": base_url,
            "import_job": job,
        }),
        "sub2api" => json!({
            "id": id,
            "name": name,
            "base_url": base_url,
            "email": bounded_public_text(object.get("email"), 256),
            "has_api_key": !bounded_public_text(object.get("api_key"), 16 * 1024).is_empty(),
            "group_id": bounded_public_text(object.get("group_id"), 128),
            "import_job": job,
        }),
        "ccload" => json!({
            "id": id,
            "name": name,
            "base_url": base_url,
            "has_password": !bounded_public_text(object.get("password"), 16 * 1024).is_empty(),
            "import_job": job,
        }),
        _ => json!({}),
    }
}

fn public_registry(kind: &str, values: Vec<Value>) -> Vec<Value> {
    values
        .iter()
        .map(|value| public_registry_item(kind, value))
        .collect()
}

fn new_registry_id(kind: &str) -> String {
    format!("{kind}-{}-{}", std::process::id(), now_nanos())
}

fn save_registry_item(
    state: &AppState,
    kind: &str,
    object: Map<String, Value>,
) -> Result<Value, ApiError> {
    let id = object
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::invalid_request)?
        .to_owned();
    let value = Value::Object(object);
    super::mutate_server_registry(state, kind, |values| {
        values.retain(|item| item.get("id").and_then(Value::as_str) != Some(id.as_str()));
        values.push(value.clone());
        Ok(value.clone())
    })
}

fn required_name(object: &Map<String, Value>) -> Result<String, ApiError> {
    object
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .map(ToOwned::to_owned)
        .ok_or_else(ApiError::invalid_request)
}

fn optional_display_name(value: Option<&Value>) -> Result<String, ApiError> {
    match value {
        None => Ok(String::new()),
        Some(Value::String(value)) => {
            let value = value.trim();
            if value.chars().count() > 256 {
                return Err(ApiError::validation());
            }
            Ok(value.to_owned())
        }
        Some(_) => Err(ApiError::validation()),
    }
}

fn registry_response(kind: &str, item_key: &str, item: Value, state: &AppState) -> Json<Value> {
    let values = public_registry(kind, registry_items(state, kind));
    let mut output = Map::new();
    output.insert(item_key.to_owned(), public_registry_item(kind, &item));
    output.insert(
        if kind == "cpa_pools" {
            "pools".to_owned()
        } else {
            "servers".to_owned()
        },
        Value::Array(values),
    );
    Json(Value::Object(output))
}

async fn remote_json(
    _state: &AppState,
    request: reqwest::RequestBuilder,
) -> Result<Value, ApiError> {
    let response = tokio::time::timeout(std::time::Duration::from_secs(30), request.send())
        .await
        .map_err(|_| ApiError::upstream())?
        .map_err(|_| ApiError::upstream())?;
    if !response.status().is_success() {
        return Err(ApiError::upstream());
    }
    let body = super::bounded_response_body(response).await?;
    serde_json::from_slice(&body).map_err(|_| ApiError::upstream())
}

fn cpa_remote_proxy_profile(state: &AppState) -> ProxyProfile {
    super::legacy_global_proxy_profile(state)
}

fn cpa_remote_client(state: &AppState) -> Client {
    super::legacy_global_proxy_client(state)
}

async fn remote_import_json(
    _state: &AppState,
    request: reqwest::RequestBuilder,
) -> Result<Value, String> {
    let response = tokio::time::timeout(std::time::Duration::from_secs(30), request.send())
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status().as_u16()));
    }
    let body = super::bounded_response_body(response)
        .await
        .map_err(|_| "invalid export payload".to_owned())?;
    serde_json::from_slice(&body).map_err(|error| error.to_string())
}

fn remote_array(value: &Value, keys: &[&str]) -> Option<Vec<Value>> {
    if let Some(array) = value.as_array() {
        return Some(array.clone());
    }
    let object = value.as_object()?;
    for key in keys {
        if let Some(array) = object.get(*key).and_then(Value::as_array) {
            return Some(array.clone());
        }
    }
    if let Some(data) = object.get("data") {
        return remote_array(data, keys);
    }
    None
}

fn sub2api_page_items(payload: &Value) -> Result<(Vec<Value>, i128), ()> {
    let inner = match payload.as_object() {
        Some(object) if object.contains_key("code") && object.contains_key("data") => {
            &object["data"]
        }
        _ => payload,
    };
    if let Some(items) = inner.as_array() {
        return Ok((items.clone(), items.len() as i128));
    }
    let Some(object) = inner.as_object() else {
        return Ok((Vec::new(), 0));
    };
    for key in ["items", "data", "list"] {
        let Some(items) = object.get(key).and_then(Value::as_array) else {
            continue;
        };
        let total = match object.get("total") {
            Some(value) if python_truthy(value) => python_integer(value).ok_or(())?,
            _ => items.len() as i128,
        };
        return Ok((items.clone(), total));
    }
    Ok((Vec::new(), 0))
}

fn python_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}

fn python_integer(value: &Value) -> Option<i128> {
    match value {
        Value::Bool(value) => Some(i128::from(*value)),
        Value::Number(value) => value
            .as_i64()
            .map(i128::from)
            .or_else(|| value.as_u64().map(i128::from))
            .or_else(|| value.as_f64().map(|value| value as i128)),
        Value::String(value) => value.trim().parse().ok(),
        _ => None,
    }
}
fn python_integer_or_zero(value: Option<&Value>) -> Result<i128, ()> {
    let Some(value) = value else {
        return Ok(0);
    };
    if !python_truthy(value) {
        return Ok(0);
    }
    python_integer(value).ok_or(())
}

fn import_job(
    job_id: &str,
    total: usize,
    added: usize,
    skipped: usize,
    refreshed: usize,
    failed: usize,
    errors: Vec<Value>,
) -> Value {
    let completed = added.saturating_add(skipped).saturating_add(failed);
    debug_assert_eq!(completed, total);
    progress_job_with_created(
        job_id,
        ImportProgress {
            total,
            completed,
            added,
            skipped,
            refreshed,
            failed,
        },
        if failed > 0 { "failed" } else { "completed" },
        errors,
        None,
    )
}

fn access_token_import_job(
    job_id: &str,
    progress: ImportProgress,
    errors: Vec<Value>,
    imported_count: usize,
    snapshot_saved: bool,
) -> Value {
    progress_job_with_created(
        job_id,
        progress,
        if imported_count > 0 && snapshot_saved {
            "completed"
        } else {
            "failed"
        },
        errors,
        None,
    )
}

struct ImportProgress {
    total: usize,
    completed: usize,
    added: usize,
    skipped: usize,
    refreshed: usize,
    failed: usize,
}

fn progress_job_with_created(
    job_id: &str,
    progress: ImportProgress,
    status: &str,
    errors: Vec<Value>,
    created_at: Option<&str>,
) -> Value {
    let phase = match status {
        "pending" => "pending",
        "completed" => "completed",
        "failed" => "failed",
        _ => "processing",
    };
    let phase_completed = progress.completed;
    let phase_total = progress.total;
    progress_job_with_phase(
        job_id,
        progress,
        status,
        errors,
        created_at,
        phase,
        phase_completed,
        phase_total,
    )
}

#[allow(clippy::too_many_arguments)]
fn progress_job_with_phase(
    job_id: &str,
    progress: ImportProgress,
    status: &str,
    errors: Vec<Value>,
    created_at: Option<&str>,
    phase: &str,
    phase_completed: usize,
    phase_total: usize,
) -> Value {
    let timestamp = python_iso_timestamp(SystemTime::now());
    let created_at = created_at
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(timestamp.as_str());
    json!({
        "job_id": job_id,
        "status": status,
        "created_at": created_at,
        "updated_at": python_iso_timestamp(SystemTime::now()),
        "total": progress.total,
        "completed": progress.completed,
        "phase": phase,
        "phase_completed": phase_completed.min(phase_total),
        "phase_total": phase_total,
        "added": progress.added,
        "skipped": progress.skipped,
        "refreshed": progress.refreshed,
        "failed": progress.failed,
        "errors": errors,
    })
}

const MAX_IMPORT_ERRORS: usize = 100;

fn push_import_error(errors: &mut Vec<Value>, value: Value) {
    if errors.len() < MAX_IMPORT_ERRORS {
        errors.push(value);
    }
}

async fn remote_json_until(
    state: &AppState,
    request: reqwest::RequestBuilder,
    deadline: std::time::Instant,
) -> Result<Value, ApiError> {
    let remaining = deadline
        .checked_duration_since(std::time::Instant::now())
        .ok_or_else(ApiError::upstream)?;
    tokio::time::timeout(remaining, remote_json(state, request))
        .await
        .map_err(|_| ApiError::upstream())?
}

fn begin_registry_job(
    state: &AppState,
    kind: &str,
    id: &str,
    job: Value,
) -> Result<Value, ApiError> {
    super::mutate_server_registry(state, kind, |values| {
        let Some(item) = values
            .iter_mut()
            .find(|item| item.get("id").and_then(Value::as_str) == Some(id))
        else {
            return Err(ApiError::not_found());
        };
        if item
            .get("import_job")
            .and_then(Value::as_object)
            .and_then(|job| job.get("status"))
            .and_then(Value::as_str)
            .is_some_and(|status| matches!(status, "pending" | "running"))
        {
            return Err(ApiError::validation());
        }
        item.as_object_mut()
            .ok_or_else(ApiError::unavailable)?
            .insert("import_job".to_owned(), job);
        Ok(item.clone())
    })
}

type CpaDownloadFuture = Pin<Box<dyn Future<Output = (String, Result<String, String>)> + Send>>;

#[allow(clippy::too_many_arguments)]
fn update_registry_job_progress(
    state: &AppState,
    kind: &str,
    id: &str,
    expected_job_id: &str,
    completed: usize,
    total: usize,
    added: usize,
    skipped: usize,
    failed: usize,
    errors: &[Value],
) -> Result<bool, ApiError> {
    let job = progress_job_with_phase(
        expected_job_id,
        ImportProgress {
            total,
            completed,
            added,
            skipped,
            refreshed: 0,
            failed,
        },
        "running",
        errors.to_vec(),
        None,
        "fetching_credentials",
        completed,
        total,
    );
    set_registry_job(state, kind, id, job, Some(expected_job_id))
}

fn set_registry_job(
    state: &AppState,
    kind: &str,
    id: &str,
    job: Value,
    expected_job_id: Option<&str>,
) -> Result<bool, ApiError> {
    super::mutate_server_registry(state, kind, |values| {
        let Some(item) = values
            .iter_mut()
            .find(|item| item.get("id").and_then(Value::as_str) == Some(id))
        else {
            return Err(ApiError::not_found());
        };
        let current_job_id = item
            .get("import_job")
            .and_then(Value::as_object)
            .and_then(|job| job.get("job_id"))
            .and_then(Value::as_str);
        if expected_job_id.is_some_and(|expected| current_job_id != Some(expected)) {
            return Ok(false);
        }
        item.as_object_mut()
            .ok_or_else(ApiError::unavailable)?
            .insert("import_job".to_owned(), job);
        Ok(true)
    })
}

pub(super) async fn login(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authenticated(&headers, &state)
        .await
        .map_err(|_| ApiError::management_unauthorized())?;
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .map(|(_, token)| token.trim())
        .unwrap_or_default();
    let (role, subject_id, name) = if state.config.auth_key.as_deref().is_some_and(|expected| {
        super::constant_time_equal(token.as_bytes(), expected.trim().as_bytes())
    }) {
        ("admin".to_owned(), "admin".to_owned(), "管理员".to_owned())
    } else {
        let _ = state.auth_store.reload().await;
        state
            .auth_store
            .identity(token)
            .ok_or_else(ApiError::unauthorized)?
    };
    Ok(Json(json!({
        "ok": true,
        "version": state.config.version,
        "role": role,
        "subject_id": subject_id,
        "name": name,
    })))
}

pub(super) async fn cpa_pools(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    Ok(Json(
        json!({"pools": public_registry("cpa_pools", registry_items(&state, "cpa_pools"))}),
    ))
}

pub(super) async fn create_cpa_pool(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let mut object = super::account_json_body(body)
        .await?
        .as_object()
        .cloned()
        .ok_or_else(ApiError::invalid_request)?;
    let name = optional_display_name(object.get("name"))?;
    object.insert("name".to_owned(), Value::String(name));
    let base_url = normalized_remote_url(object.get("base_url"))?;
    let secret = object
        .get("secret_key")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(ApiError::invalid_request)?;
    if secret.len() > 16 * 1024 {
        return Err(ApiError::validation());
    }
    object.insert("base_url".to_owned(), Value::String(base_url));
    object.insert("secret_key".to_owned(), Value::String(secret));
    object.insert("id".to_owned(), Value::String(new_registry_id("cpa")));
    object.insert("import_job".to_owned(), Value::Null);
    let item = save_registry_item(&state, "cpa_pools", object)?;
    Ok(registry_response("cpa_pools", "pool", item, &state))
}

pub(super) async fn update_cpa_pool(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(pool_id): AxumPath<String>,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let updates = super::account_json_body(body)
        .await?
        .as_object()
        .cloned()
        .ok_or_else(ApiError::invalid_request)?;
    let mut object = registry_item(&state, "cpa_pools", &pool_id)
        .and_then(|value| value.as_object().cloned())
        .ok_or_else(ApiError::not_found)?;
    if let Some(name) = updates.get("name").filter(|value| !value.is_null()) {
        object.insert(
            "name".to_owned(),
            Value::String(optional_display_name(Some(name))?),
        );
    }
    if let Some(base_url) = updates.get("base_url").filter(|value| !value.is_null()) {
        object.insert(
            "base_url".to_owned(),
            Value::String(normalized_remote_url(Some(base_url))?),
        );
    }
    if let Some(secret) = updates.get("secret_key").filter(|value| !value.is_null()) {
        let secret = secret
            .as_str()
            .map(str::trim)
            .filter(|value| value.len() <= 16 * 1024)
            .ok_or_else(ApiError::invalid_request)?;
        object.insert("secret_key".to_owned(), Value::String(secret.to_owned()));
    }
    object.insert("id".to_owned(), Value::String(pool_id));
    let item = save_registry_item(&state, "cpa_pools", object)?;
    Ok(registry_response("cpa_pools", "pool", item, &state))
}

pub(super) async fn delete_cpa_pool(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(pool_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let values = super::mutate_server_registry(&state, "cpa_pools", |values| {
        let before = values.len();
        values.retain(|value| value.get("id").and_then(Value::as_str) != Some(pool_id.as_str()));
        if before == values.len() {
            return Err(ApiError::not_found());
        }
        Ok(values.clone())
    })?;
    Ok(Json(json!({"pools": public_registry("cpa_pools", values)})))
}

pub(super) fn cpa_download_future(
    state: AppState,
    base: String,
    secret: String,
    name: String,
) -> CpaDownloadFuture {
    Box::pin(async move {
        let request = cpa_remote_client(&state)
            .get(format!("{base}/v0/management/auth-files/download"))
            .query(&[("name", name.clone())])
            .bearer_auth(secret)
            .header("Accept", "application/json");
        let result =
            match tokio::time::timeout(std::time::Duration::from_secs(30), request.send()).await {
                Err(error) => Err(error.to_string()),
                Ok(Err(error)) => Err(error.to_string()),
                Ok(Ok(response)) if !response.status().is_success() => {
                    Err(format!("HTTP {}", response.status().as_u16()))
                }
                Ok(Ok(response)) => match super::bounded_response_body(response).await {
                    Err(_) => Err("invalid payload".to_owned()),
                    Ok(body) => match serde_json::from_slice::<Value>(&body) {
                        Err(error) => Err(error.to_string()),
                        Ok(value) if !value.is_object() => Err("invalid payload".to_owned()),
                        Ok(value) => {
                            let token =
                                super::protocol_anthropic::python_text(value.get("access_token"));
                            let token = token.trim();
                            if token.is_empty() {
                                Err("missing access_token".to_owned())
                            } else {
                                Ok(token.to_owned())
                            }
                        }
                    },
                },
            };
        (name, result)
    })
}
pub(super) async fn cpa_pool_files(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(pool_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let pool = registry_item(&state, "cpa_pools", &pool_id)
        .and_then(|value| value.as_object().cloned())
        .ok_or_else(ApiError::not_found)?;
    let base = pool
        .get("base_url")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let secret = pool
        .get("secret_key")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if base.is_empty() || secret.is_empty() {
        return Ok(Json(json!({"pool_id": pool_id, "files": []})));
    }
    let value = remote_json(
        &state,
        cpa_remote_client(&state)
            .get(format!("{base}/v0/management/auth-files"))
            .bearer_auth(secret)
            .header("Accept", "application/json"),
    )
    .await?;
    let files = remote_array(&value, &["files"])
        .ok_or_else(ApiError::upstream)?
        .into_iter()
        .filter_map(|item| {
            let object = item.as_object()?;
            let name = bounded_public_text(object.get("name"), 16 * 1024);
            if name.is_empty() {
                return None;
            }
            let email_value = object
                .get("email")
                .filter(|value| super::account_pool::account_value_truthy(Some(value)))
                .or_else(|| object.get("account"));
            Some(json!({
                "name": name,
                "email": bounded_public_text(email_value, 16 * 1024),
            }))
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({"pool_id": pool_id, "files": files})))
}

pub(super) async fn execute_cpa_import(
    state: AppState,
    pool_id: String,
    base: String,
    secret: String,
    names: Vec<String>,
    expected_job_id: String,
) {
    let batch_stats = Arc::new(super::ImportedModelCatalogBatchStats::default());
    let mut errors = Vec::new();
    let mut successful = 0usize;
    let mut failed = 0usize;
    let mut imported = Vec::new();
    let total = names.len();
    let _ = set_registry_job(
        &state,
        "cpa_pools",
        &pool_id,
        progress_job_with_phase(
            &expected_job_id,
            ImportProgress {
                total,
                completed: 0,
                added: 0,
                skipped: 0,
                refreshed: 0,
                failed: 0,
            },
            "running",
            Vec::new(),
            None,
            "downloading_credentials",
            0,
            total,
        ),
        Some(&expected_job_id),
    );
    let concurrency = total.clamp(1, 16);
    let mut queue = names.into_iter();
    let mut active: FuturesUnordered<CpaDownloadFuture> = FuturesUnordered::new();
    for _ in 0..concurrency {
        if let Some(name) = queue.next() {
            active.push(cpa_download_future(
                state.clone(),
                base.clone(),
                secret.clone(),
                name,
            ));
        }
    }
    while let Some((name, result)) = active.next().await {
        match result {
            Ok(token) => {
                imported.push(json!({"access_token": token, "source_type":"codex"}));
                successful += 1;
            }
            Err(error) => {
                failed += 1;
                errors.push(json!({"name": name, "error": error}));
            }
        }
        let completed = successful + failed;
        let _ = update_registry_job_progress(
            &state,
            "cpa_pools",
            &pool_id,
            &expected_job_id,
            completed,
            total,
            successful,
            0,
            failed,
            &errors,
        );
        if let Some(next_name) = queue.next() {
            active.push(cpa_download_future(
                state.clone(),
                base.clone(),
                secret.clone(),
                next_name,
            ));
        }
    }
    let imported_tokens = imported
        .iter()
        .filter_map(|item| item.get("access_token").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let (added, skipped, failed, snapshot_saved) =
        match state.account_store.merge_import_records(imported).await {
            Ok((added, skipped)) => (added, skipped, failed, true),
            Err(_) => {
                errors.push(json!({"name": "accounts", "error": "账号快照写入失败"}));
                (0, 0, failed.saturating_add(successful), false)
            }
        };
    let _ = set_registry_job(
        &state,
        "cpa_pools",
        &pool_id,
        progress_job_with_phase(
            &expected_job_id,
            ImportProgress {
                total,
                completed: successful.saturating_add(failed),
                added,
                skipped,
                refreshed: 0,
                failed,
            },
            "running",
            errors.clone(),
            None,
            "refreshing_accounts",
            0,
            imported_tokens.len(),
        ),
        Some(&expected_job_id),
    );
    let refresh_result = super::refresh_imported_accounts_with_batch_until(
        &state,
        &imported_tokens,
        batch_stats.clone(),
        std::time::Instant::now() + CCLOAD_IMPORT_DEADLINE,
    )
    .await;
    let refreshed = refresh_result
        .get("refreshed")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or_default();
    let mut job = access_token_import_job(
        &expected_job_id,
        ImportProgress {
            total,
            completed: total,
            added,
            skipped,
            refreshed,
            failed,
        },
        errors,
        successful,
        snapshot_saved,
    );
    add_model_catalog_stats(&mut job, &state, &batch_stats).await;
    let _ = set_registry_job(&state, "cpa_pools", &pool_id, job, Some(&expected_job_id));
}

pub(super) async fn start_cpa_import(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(pool_id): AxumPath<String>,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let pool = registry_item(&state, "cpa_pools", &pool_id)
        .and_then(|value| value.as_object().cloned())
        .ok_or_else(ApiError::not_found)?;
    let value = super::account_json_body(body).await?;
    let object = value.as_object().ok_or_else(ApiError::validation)?;
    let names = match object.get("names") {
        None => Vec::new(),
        Some(Value::Array(values)) => {
            if values.iter().any(|value| !value.is_string()) {
                return Err(ApiError::validation());
            }
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        }
        Some(_) => return Err(ApiError::validation()),
    };
    if names.is_empty() {
        return Err(ApiError::invalid_request());
    }
    let job_id = format!("job-{}-{}", std::process::id(), now_nanos());
    let job = json!({
        "job_id": job_id.clone(),
        "status": "pending",
        "created_at": python_iso_timestamp(SystemTime::now()),
        "updated_at": python_iso_timestamp(SystemTime::now()),
        "total": names.len(),
        "completed": 0,
        "added": 0,
        "skipped": 0,
        "refreshed": 0,
        "failed": 0,
        "errors": [],
    });
    let saved = begin_registry_job(&state, "cpa_pools", &pool_id, job)?;
    let base = pool
        .get("base_url")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let secret = pool
        .get("secret_key")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    tokio::spawn(execute_cpa_import(
        state, pool_id, base, secret, names, job_id,
    ));
    Ok(Json(
        json!({"import_job": public_import_job(saved.get("import_job"))}),
    ))
}

pub(super) async fn cpa_import_progress(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(pool_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let pool = registry_item(&state, "cpa_pools", &pool_id).ok_or_else(ApiError::not_found)?;
    Ok(Json(
        json!({"import_job": public_import_job(pool.get("import_job"))}),
    ))
}

fn bounded_public_text(value: Option<&Value>, limit: usize) -> String {
    let text = super::protocol_anthropic::python_text(value);
    let text = text.trim();
    if !text.is_empty() && text.chars().count() <= limit {
        text.to_owned()
    } else {
        String::new()
    }
}
fn python_remote_string_if_present(value: Option<&Value>, limit: usize) -> Option<String> {
    let value = value.filter(|value| !value.is_null())?;
    let text = super::account_pool::python_account_value_string(value);
    (text.chars().count() <= limit).then_some(text)
}

const MAX_CCLOAD_CHANNEL_ID_LENGTH: usize = 64;
const CCLOAD_CHANNEL_MODEL_DEADLINE: Duration = Duration::from_secs(90);

fn valid_ccload_expired_text(text: &str) -> bool {
    let (local, zone) = if let Some(local) = text.strip_suffix('Z') {
        (local, None)
    } else if text.len() >= 6 {
        let split = text.len() - 6;
        let (local, zone) = text.split_at(split);
        (local, Some(zone))
    } else {
        return false;
    };
    let Some((date, time)) = local.split_once('T') else {
        return false;
    };
    if date.len() != 10
        || !date.bytes().enumerate().all(|(index, byte)| {
            matches!(index, 4 | 7) && byte == b'-'
                || !matches!(index, 4 | 7) && byte.is_ascii_digit()
        })
    {
        return false;
    }
    let year = date[0..4].parse::<u16>().ok();
    let month = date[5..7].parse::<u8>().ok();
    let day = date[8..10].parse::<u8>().ok();
    let Some((year, month, day)) = year
        .zip(month)
        .zip(day)
        .map(|((year, month), day)| (year, month, day))
    else {
        return false;
    };
    let leap_year = year % 400 == 0 || (year % 4 == 0 && year % 100 != 0);
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap_year => 29,
        2 => 28,
        _ => 0,
    };
    if year == 0 || day == 0 || day > days_in_month {
        return false;
    }
    let (clock, fraction) = match time.split_once('.') {
        Some((clock, fraction)) => (clock, fraction),
        None => (time, ""),
    };
    if clock.len() != 8
        || !clock.bytes().enumerate().all(|(index, byte)| {
            matches!(index, 2 | 5) && byte == b':'
                || !matches!(index, 2 | 5) && byte.is_ascii_digit()
        })
        || (!fraction.is_empty() && !fraction.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return false;
    }
    let hours = clock[0..2].parse::<u8>().ok();
    let minutes = clock[3..5].parse::<u8>().ok();
    let seconds = clock[6..8].parse::<u8>().ok();
    if hours.is_none_or(|value| value > 23)
        || minutes.is_none_or(|value| value > 59)
        || seconds.is_none_or(|value| value > 59)
    {
        return false;
    }
    let Some(zone) = zone else {
        return true;
    };
    zone.len() == 6
        && matches!(zone.as_bytes()[0], b'+' | b'-')
        && zone.as_bytes()[3] == b':'
        && zone[1..3].parse::<u8>().is_ok_and(|value| value <= 23)
        && zone[4..6].parse::<u8>().is_ok_and(|value| value <= 59)
        && zone[1..3].bytes().all(|byte| byte.is_ascii_digit())
        && zone[4..6].bytes().all(|byte| byte.is_ascii_digit())
}

fn clean_ccload_channel_id(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::Number(number)) => number.as_u64().and_then(|number| {
            if number == 0 {
                return None;
            }
            let text = number.to_string();
            (text.len() <= MAX_CCLOAD_CHANNEL_ID_LENGTH).then_some(text)
        }),
        Some(Value::String(raw)) => {
            let text = raw.trim();
            if text.is_empty()
                || text.len() > MAX_CCLOAD_CHANNEL_ID_LENGTH
                || text.starts_with('0')
                || !text.bytes().all(|byte| byte.is_ascii_digit())
            {
                return None;
            }
            Some(text.to_owned())
        }
        _ => None,
    }
}

fn clean_ccload_channel_ids(
    value: Option<&Value>,
    maximum: usize,
) -> Result<Vec<String>, ApiError> {
    let values = value
        .and_then(Value::as_array)
        .filter(|values| !values.is_empty())
        .ok_or_else(ApiError::invalid_request)?;
    let mut seen = HashSet::new();
    let mut selected = Vec::with_capacity(values.len());
    for value in values {
        let channel_id =
            clean_ccload_channel_id(Some(value)).ok_or_else(ApiError::invalid_request)?;
        if seen.insert(channel_id.clone()) {
            selected.push(channel_id);
        }
    }
    if selected.is_empty() || selected.len() > maximum {
        return Err(ApiError::invalid_request());
    }
    Ok(selected)
}

fn normalized_ccload_credential(value: Option<&Value>) -> Option<String> {
    value?
        .as_object()?
        .get("access_token")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|token| !token.is_empty() && token.len() <= 16 * 1024)
        .map(ToOwned::to_owned)
}

fn normalized_ccload_refresh_time(value: Option<&Value>) -> Option<String> {
    let value = value?;
    let seconds = match value {
        Value::Number(number) => number.as_i64().map(i128::from),
        Value::String(text) => text.trim().parse::<i128>().ok(),
        _ => None,
    }?;
    let seconds = if seconds > 100_000_000_000 {
        seconds / 1_000
    } else {
        seconds
    };
    let seconds = i64::try_from(seconds).ok()?;
    let timestamp = UNIX_EPOCH.checked_add(Duration::from_secs(u64::try_from(seconds).ok()?))?;
    Some(
        iso_timestamp(timestamp)
            .trim_end_matches('Z')
            .replace('T', " "),
    )
}

fn normalized_ccload_refresh_text(value: Option<&Value>) -> Option<String> {
    if let Some(timestamp) = normalized_ccload_refresh_time(value) {
        return Some(timestamp);
    }
    let text = value?.as_str()?.trim();
    if text.is_empty() || text.len() > 128 {
        return None;
    }
    if text.len() >= 19
        && text.as_bytes().get(4) == Some(&b'-')
        && text.as_bytes().get(7) == Some(&b'-')
        && matches!(text.as_bytes().get(10), Some(b'T' | b' '))
    {
        return Some(text[..19].replace('T', " "));
    }
    Some(text.to_owned())
}

fn ccload_recent_refresh_time(
    channel: Option<&Value>,
    credential: Option<&Value>,
) -> Option<String> {
    const TIME_KEYS: &[&str] = &[
        "refresh_time",
        "refresh_at",
        "last_refresh",
        "last_refresh_at",
        "last_refreshed_at",
        "refreshed_at",
        "updated_at",
        "updatedAt",
    ];
    const NESTED_KEYS: &[&str] = &["metadata", "meta", "oauth_credential"];
    let mut objects = Vec::new();
    for value in [credential, channel].into_iter().flatten() {
        objects.push(value);
        if let Some(object) = value.as_object() {
            for key in NESTED_KEYS {
                if let Some(nested) = object.get(*key) {
                    objects.push(nested);
                }
            }
        }
    }
    for value in objects {
        let Some(object) = value.as_object() else {
            continue;
        };
        for key in TIME_KEYS {
            if let Some(timestamp) = normalized_ccload_refresh_text(object.get(*key)) {
                return Some(timestamp);
            }
        }
    }
    None
}

#[derive(Clone)]
struct CcLoadModelEntry {
    id: String,
    provenance: ModelProvenance,
}

fn ccload_model_allowed(provenance: ModelProvenance) -> bool {
    matches!(provenance, ModelProvenance::Web | ModelProvenance::Image)
}

fn ccload_model_entries(value: Option<&Value>) -> Vec<CcLoadModelEntry> {
    project_imported_model_entries(value, ModelProvenance::Unknown)
        .into_iter()
        .map(|(id, provenance)| CcLoadModelEntry { id, provenance })
        .collect()
}

fn ccload_model_ids(value: Option<&Value>) -> Vec<String> {
    ccload_model_entries(value)
        .into_iter()
        .filter(|entry| ccload_model_allowed(entry.provenance))
        .map(|entry| entry.id)
        .collect()
}

fn ccload_model_payload(entries: Vec<CcLoadModelEntry>) -> (Value, Value) {
    let models = entries
        .iter()
        .filter(|entry| ccload_model_allowed(entry.provenance))
        .map(|entry| Value::String(entry.id.clone()))
        .collect::<Vec<_>>();
    let sources = entries
        .into_iter()
        .filter(|entry| ccload_model_allowed(entry.provenance))
        .map(|entry| {
            (
                entry.id,
                Value::String(model_provenance_label(entry.provenance).to_owned()),
            )
        })
        .collect::<Map<_, _>>();
    (Value::Array(models), Value::Object(sources))
}

fn apply_ccload_image_capability(catalog: &mut Value, image_models: &[String]) {
    let Some(object) = catalog.as_object_mut() else {
        return;
    };
    let image_models = image_models
        .iter()
        .filter(|id| is_web_image_model_id(id))
        .collect::<Vec<_>>();
    let models = object
        .entry("models".to_owned())
        .or_insert_with(|| Value::Array(Vec::new()));
    if !models.is_array() {
        *models = Value::Array(Vec::new());
    }
    let models = models.as_array_mut().expect("ccLoad models array");
    models.retain(|model| {
        !model
            .as_str()
            .is_some_and(|value| value.to_ascii_lowercase().starts_with("gpt-image-"))
    });
    for image_model in &image_models {
        if !models
            .iter()
            .any(|model| model.as_str() == Some(image_model.as_str()))
        {
            models.push(Value::String((*image_model).clone()));
        }
    }
    if !image_models.is_empty() {
        object.insert("models_loaded".to_owned(), Value::Bool(true));
    }
    let sources = object
        .entry("model_sources".to_owned())
        .or_insert_with(|| Value::Object(Map::new()));
    if !sources.is_object() {
        *sources = Value::Object(Map::new());
    }
    if let Some(sources) = sources.as_object_mut() {
        sources.retain(|id, _| !id.to_ascii_lowercase().starts_with("gpt-image-"));
        for image_model in &image_models {
            sources.insert((*image_model).clone(), Value::String("image".to_owned()));
        }
    }
}

fn merge_ccload_model_catalog(
    _models: Option<&Value>,
    _sources: Option<&Value>,
    fetched: Option<&[super::model_pool::PublicModel]>,
) -> (Value, Value) {
    let mut entries: Vec<(String, ModelProvenance)> = Vec::new();
    let mut indexes: HashMap<String, usize> = HashMap::new();
    if let Some(fetched) = fetched {
        for model in fetched {
            if !ccload_model_allowed(model.provenance) {
                continue;
            }
            if let Some(index) = indexes.get(&model.id).copied() {
                if model_provenance_rank(model.provenance) > model_provenance_rank(entries[index].1)
                {
                    entries[index].1 = model.provenance;
                }
            } else if entries.len() < super::MAX_MODELS {
                indexes.insert(model.id.clone(), entries.len());
                entries.push((model.id.clone(), model.provenance));
            }
        }
    }
    ccload_model_payload(
        entries
            .into_iter()
            .map(|(id, provenance)| CcLoadModelEntry { id, provenance })
            .collect(),
    )
}

fn merge_ccload_account_catalog(
    _models: Option<&Value>,
    _sources: Option<&Value>,
    snapshot: &Value,
) -> (Value, Value) {
    let mut entries: Vec<(String, ModelProvenance)> = Vec::new();
    let mut indexes = HashMap::<String, usize>::new();
    let snapshot_entries = snapshot
        .as_object()
        .map(|object| project_account_model_entries(object, ModelProvenance::Unknown))
        .unwrap_or_default();
    for (id, provenance) in snapshot_entries {
        if !ccload_model_allowed(provenance) {
            continue;
        }
        if let Some(index) = indexes.get(&id).copied() {
            if model_provenance_rank(provenance) > model_provenance_rank(entries[index].1) {
                entries[index].1 = provenance;
            }
        } else if entries.len() < super::MAX_MODELS {
            indexes.insert(id.clone(), entries.len());
            entries.push((id, provenance));
        }
    }
    ccload_model_payload(
        entries
            .into_iter()
            .map(|(id, provenance)| CcLoadModelEntry { id, provenance })
            .collect(),
    )
}

fn public_sub2api_item(value: &Value) -> Value {
    let object = value.as_object().cloned().unwrap_or_default();
    json!({
        "id": bounded_public_text(object.get("id"), 128),
        "name": bounded_public_text(object.get("name"), 256),
        "base_url": public_url(object.get("base_url")),
        "email": bounded_public_text(object.get("email"), 256),
        "group_id": bounded_public_text(object.get("group_id"), 128),
        "has_api_key": !bounded_public_text(object.get("api_key"), 16 * 1024).is_empty(),
        "import_job": public_import_job(object.get("import_job")),
    })
}

fn public_ccload_item(value: &Value) -> Value {
    let object = value.as_object().cloned().unwrap_or_default();
    json!({
        "id": bounded_public_text(object.get("id"), 128),
        "name": bounded_public_text(object.get("name"), 256),
        "base_url": public_url(object.get("base_url")),
        "has_password": !bounded_public_text(object.get("password"), 16 * 1024).is_empty(),
        "import_job": public_import_job(object.get("import_job")),
    })
}

fn public_sub2api_items(values: Vec<Value>) -> Vec<Value> {
    values.iter().map(public_sub2api_item).collect()
}

fn public_ccload_items(values: Vec<Value>) -> Vec<Value> {
    values.iter().map(public_ccload_item).collect()
}

fn registry_value(state: &AppState, kind: &str, id: &str) -> Result<Map<String, Value>, ApiError> {
    registry_item(state, kind, id)
        .and_then(|value| value.as_object().cloned())
        .ok_or_else(ApiError::not_found)
}

fn registry_update(
    state: &AppState,
    kind: &str,
    id: &str,
    updates: Map<String, Value>,
) -> Result<Value, ApiError> {
    super::mutate_server_registry(state, kind, |values| {
        let Some(item) = values
            .iter_mut()
            .find(|item| item.get("id").and_then(Value::as_str) == Some(id))
        else {
            return Err(ApiError::not_found());
        };
        let object = item.as_object_mut().ok_or_else(ApiError::unavailable)?;
        for (key, value) in updates {
            if matches!(key.as_str(), "id" | "import_job") {
                continue;
            }
            object.insert(key, value);
        }
        Ok(item.clone())
    })
}

fn sub2api_credentials_present(object: &Map<String, Value>) -> bool {
    !bounded_public_text(object.get("api_key"), 16 * 1024).is_empty()
        || (!bounded_public_text(object.get("email"), 256).is_empty()
            && !bounded_public_text(object.get("password"), 16 * 1024).is_empty())
}
async fn sub2api_request(
    state: &AppState,
    server: &Map<String, Value>,
    request: reqwest::RequestBuilder,
) -> Result<reqwest::RequestBuilder, ApiError> {
    if let Some(api_key) = server
        .get("api_key")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(request.header("x-api-key", api_key));
    }
    let base = server
        .get("base_url")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::invalid_request)?;
    let email = server
        .get("email")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::invalid_request)?;
    let password = server
        .get("password")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::invalid_request)?;
    let server_id = server
        .get("id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or(base);
    let identity: [u8; 32] =
        Sha256::digest(format!("{base}\0{email}\0{password}").as_bytes()).into();
    if let Some(token) = state
        .sub2api_login_cache
        .lock()
        .await
        .get(server_id)
        .filter(|(_, cached_identity, expires_at)| {
            cached_identity == &identity && *expires_at > std::time::Instant::now()
        })
        .map(|(token, _, _)| token.clone())
    {
        return Ok(request.bearer_auth(token));
    }
    let login = remote_json(
        state,
        state
            .client
            .post(format!("{base}/api/v1/auth/login"))
            .json(&json!({"email": email, "password": password})),
    )
    .await?;
    let login_body = if login.get("code").is_some() && login.get("data").is_some() {
        login.get("data").unwrap_or(&login)
    } else {
        &login
    };
    let token = login_body
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(ApiError::upstream)?
        .to_owned();
    let expires_in = login_body
        .get("expires_in")
        .filter(|value| super::account_pool::account_value_truthy(Some(value)))
        .and_then(super::python_integer_value)
        .unwrap_or(3600)
        .max(60) as u64;
    let refresh_after = expires_in.saturating_sub(5 * 60);
    let expires_at = std::time::Instant::now() + Duration::from_secs(refresh_after);
    {
        let mut cache = state.sub2api_login_cache.lock().await;
        if cache.len() >= 128
            && !cache.contains_key(server_id)
            && let Some(oldest_key) = cache.keys().next().cloned()
        {
            cache.remove(&oldest_key);
        }
        cache.insert(server_id.to_owned(), (token.clone(), identity, expires_at));
    }
    Ok(request.bearer_auth(token))
}

pub(super) async fn sub2api_servers(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    Ok(Json(
        json!({"servers": public_sub2api_items(registry_items(&state, "sub2api"))}),
    ))
}

pub(super) async fn create_sub2api_server(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let mut object = super::account_json_body(body)
        .await?
        .as_object()
        .cloned()
        .ok_or_else(ApiError::invalid_request)?;
    let name = optional_display_name(object.get("name"))?;
    let base_url = normalized_remote_url(object.get("base_url"))?;
    object.insert("name".to_owned(), Value::String(name));
    object.insert("base_url".to_owned(), Value::String(base_url));
    if !sub2api_credentials_present(&object) {
        return Err(ApiError::invalid_request());
    }
    for key in ["email", "password", "api_key", "group_id"] {
        if let Some(value) = object.get(key) {
            let value = value.as_str().ok_or_else(ApiError::validation)?;
            if value.len() > 16 * 1024 {
                return Err(ApiError::validation());
            }
        }
    }
    object.insert("id".to_owned(), Value::String(new_registry_id("sub2api")));
    object.insert("import_job".to_owned(), Value::Null);
    let item = save_registry_item(&state, "sub2api", object)?;
    let servers = public_sub2api_items(registry_items(&state, "sub2api"));
    Ok(Json(
        json!({"server": public_sub2api_item(&item), "servers": servers}),
    ))
}

pub(super) async fn update_sub2api_server(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(server_id): AxumPath<String>,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let updates = super::account_json_body(body)
        .await?
        .as_object()
        .cloned()
        .ok_or_else(ApiError::invalid_request)?;
    for key in [
        "name", "base_url", "email", "password", "api_key", "group_id",
    ] {
        if let Some(value) = updates.get(key).filter(|value| !value.is_null()) {
            let value = value.as_str().ok_or_else(ApiError::validation)?;
            if value.len() > 16 * 1024 {
                return Err(ApiError::validation());
            }
        }
    }
    let mut candidate = registry_value(&state, "sub2api", &server_id)?;
    for key in [
        "name", "base_url", "email", "password", "api_key", "group_id",
    ] {
        if let Some(value) = updates.get(key).filter(|value| !value.is_null()) {
            candidate.insert(key.to_owned(), value.clone());
        }
    }
    if updates.get("name").is_some_and(|value| !value.is_null()) {
        let name = optional_display_name(candidate.get("name"))?;
        candidate.insert("name".to_owned(), Value::String(name));
    }
    if updates
        .get("base_url")
        .is_some_and(|value| !value.is_null())
    {
        candidate.insert(
            "base_url".to_owned(),
            Value::String(normalized_remote_url(candidate.get("base_url"))?),
        );
    }
    if !sub2api_credentials_present(&candidate) {
        return Err(ApiError::invalid_request());
    }
    let item = registry_update(&state, "sub2api", &server_id, candidate)?;
    Ok(Json(
        json!({"server": public_sub2api_item(&item), "servers": public_sub2api_items(registry_items(&state, "sub2api"))}),
    ))
}

pub(super) async fn delete_sub2api_server(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(server_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let values = super::mutate_server_registry(&state, "sub2api", |values| {
        let before = values.len();
        values.retain(|value| value.get("id").and_then(Value::as_str) != Some(server_id.as_str()));
        if before == values.len() {
            return Err(ApiError::not_found());
        }
        Ok(values.clone())
    })?;
    Ok(Json(json!({"servers": public_sub2api_items(values)})))
}

pub(super) async fn sub2api_groups(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(server_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let server = registry_value(&state, "sub2api", &server_id)?;
    let base = server
        .get("base_url")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::invalid_request)?;
    let mut groups = Vec::new();
    let mut page = 1usize;
    loop {
        let request = state
            .client
            .get(format!("{base}/api/v1/admin/groups"))
            .query(&[("page", page.to_string()), ("page_size", "200".to_owned())]);
        let value = remote_json(&state, sub2api_request(&state, &server, request).await?).await?;
        let (page_items, total) = sub2api_page_items(&value).map_err(|_| ApiError::upstream())?;
        let page_len = page_items.len();
        for item in page_items {
            let Some(object) = item.as_object() else {
                continue;
            };
            let Some(id) = python_remote_string_if_present(object.get("id"), 128) else {
                continue;
            };
            let account_count = python_integer_or_zero(object.get("account_count"))
                .map_err(|_| ApiError::upstream())?;
            let active_account_count = python_integer_or_zero(object.get("active_account_count"))
                .map_err(|_| ApiError::upstream())?;
            groups.push(json!({
                "id": id,
                "name": bounded_public_text(object.get("name"), 256),
                "description": bounded_public_text(object.get("description"), 256),
                "platform": bounded_public_text(object.get("platform"), 64),
                "status": bounded_public_text(object.get("status"), 64),
                "account_count": account_count,
                "active_account_count": active_account_count,
            }));
        }
        if (page as i128).saturating_mul(200) >= total || page_len < 200 {
            break;
        }
        page += 1;
    }
    Ok(Json(json!({"server_id": server_id, "groups": groups})))
}

pub(super) async fn sub2api_accounts(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(server_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let server = registry_value(&state, "sub2api", &server_id)?;
    let base = server
        .get("base_url")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::invalid_request)?;
    let group = server
        .get("group_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned);
    let mut accounts = Vec::new();
    let mut page = 1usize;
    loop {
        let mut request = state
            .client
            .get(format!("{base}/api/v1/admin/accounts"))
            .query(&[
                ("platform", "openai"),
                ("type", "oauth"),
                ("page", &page.to_string()),
                ("page_size", "200"),
            ]);
        if let Some(group) = group.as_deref() {
            request = request.query(&[("group", group)]);
        }
        let value = remote_json(&state, sub2api_request(&state, &server, request).await?).await?;
        let (page_items, total) = sub2api_page_items(&value).map_err(|_| ApiError::upstream())?;
        let page_len = page_items.len();
        accounts.extend(page_items.into_iter().filter_map(|item| {
            let object = item.as_object()?;
            let id = python_remote_string_if_present(object.get("id"), 128)?;
            let credentials = object.get("credentials").and_then(Value::as_object);
            let name = bounded_public_text(object.get("name"), 256);
            let email = bounded_public_text(credentials.and_then(|value| value.get("email")), 256);
            let has_refresh_token = credentials
                .and_then(|value| value.get("refresh_token"))
                .map(|value| !super::protocol_anthropic::python_text(Some(value)).trim().is_empty())
                .unwrap_or(false);
            Some(json!({
                "id": id,
                "name": name,
                "email": if email.is_empty() { name } else { email },
                "plan_type": bounded_public_text(credentials.and_then(|value| value.get("plan_type")), 64),
                "status": bounded_public_text(object.get("status"), 64),
                "expires_at": bounded_public_text(credentials.and_then(|value| value.get("expires_at")), 64),
                "has_refresh_token": has_refresh_token,
            }))
        }));
        if (page as i128).saturating_mul(200) >= total || page_len < 200 {
            break;
        }
        page += 1;
    }
    Ok(Json(json!({"server_id": server_id, "accounts": accounts})))
}

pub(super) async fn execute_sub2api_import(
    state: AppState,
    server_id: String,
    server: Map<String, Value>,
    ids: Vec<String>,
    expected_job_id: String,
) {
    let batch_stats = Arc::new(super::ImportedModelCatalogBatchStats::default());
    let _ = set_registry_job(
        &state,
        "sub2api",
        &server_id,
        progress_job_with_phase(
            &expected_job_id,
            ImportProgress {
                total: ids.len(),
                completed: 0,
                added: 0,
                skipped: 0,
                refreshed: 0,
                failed: 0,
            },
            "running",
            Vec::new(),
            None,
            "fetching_credentials",
            0,
            ids.len(),
        ),
        Some(&expected_job_id),
    );
    let base = server
        .get("base_url")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let request = state
        .client
        .get(format!("{base}/api/v1/admin/accounts/data"))
        .query(&[
            ("ids", ids.join(",")),
            ("timezone", "Asia/Shanghai".to_owned()),
        ]);
    let result = match sub2api_request(&state, &server, request).await {
        Ok(request) => remote_import_json(&state, request).await,
        Err(error) => Err(error.code().to_owned()),
    };
    let mut errors = Vec::new();
    let mut successful = 0usize;
    let mut imported = Vec::new();
    match result {
        Ok(value) => {
            let Some(accounts) = remote_array(&value, &["accounts", "data"]) else {
                errors.extend(
                    ids.iter()
                        .map(|id| json!({"name": id, "error": "invalid export payload"})),
                );
                let job = access_token_import_job(
                    &expected_job_id,
                    ImportProgress {
                        total: ids.len(),
                        completed: ids.len(),
                        added: 0,
                        skipped: 0,
                        refreshed: 0,
                        failed: ids.len(),
                    },
                    errors,
                    0,
                    true,
                );
                let _ =
                    set_registry_job(&state, "sub2api", &server_id, job, Some(&expected_job_id));
                return;
            };
            let returned_count = accounts.len();
            for account in &accounts {
                let Some(object) = account.as_object() else {
                    continue;
                };
                let credentials = object
                    .get("credentials")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                let account_id = [
                    object.get("id"),
                    credentials.get("chatgpt_account_id"),
                    object.get("name"),
                ]
                .into_iter()
                .flatten()
                .map(|value| super::protocol_anthropic::python_text(Some(value)))
                .map(|value| value.trim().to_owned())
                .find(|value| !value.is_empty())
                .unwrap_or_else(|| "unknown".to_owned());
                let token = ["access_token", "accessToken", "token"]
                    .into_iter()
                    .filter_map(|key| credentials.get(key))
                    .map(|value| super::protocol_anthropic::python_text(Some(value)))
                    .map(|value| value.trim().to_owned())
                    .find(|value| !value.is_empty());
                match token {
                    Some(token) => {
                        imported.push(json!({"access_token": token, "source_type":"codex"}));
                        successful += 1;
                    }
                    None => {
                        errors.push(json!({"name": account_id, "error": "missing access_token"}));
                    }
                }
            }
            if returned_count < ids.len() {
                errors.push(json!({
                    "name": ids.join(","),
                    "error": format!("exported {returned_count}/{} accounts", ids.len())
                }));
            }
        }
        Err(error) => {
            errors.extend(ids.iter().map(|id| json!({"name": id, "error": error})));
        }
    }
    // Match 1.7: failed is the number of concrete errors recorded above. A
    // short export contributes one aggregate "exported x/y" error; it is not
    // additionally expanded into synthetic per-account failures.
    let failed = errors.len();
    let imported_tokens = imported
        .iter()
        .filter_map(|item| item.get("access_token").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let (added, skipped, failed, snapshot_saved) =
        match state.account_store.merge_import_records(imported).await {
            Ok((added, skipped)) => (added, skipped, failed, true),
            Err(_) => {
                errors.push(json!({"name": "accounts", "error": "账号快照写入失败"}));
                (0, 0, failed.saturating_add(successful), false)
            }
        };
    let _ = set_registry_job(
        &state,
        "sub2api",
        &server_id,
        progress_job_with_phase(
            &expected_job_id,
            ImportProgress {
                total: ids.len(),
                completed: ids.len(),
                added,
                skipped,
                refreshed: 0,
                failed,
            },
            "running",
            errors.clone(),
            None,
            "refreshing_accounts",
            0,
            imported_tokens.len(),
        ),
        Some(&expected_job_id),
    );
    let refresh_result = super::refresh_imported_accounts_with_batch_until(
        &state,
        &imported_tokens,
        batch_stats.clone(),
        std::time::Instant::now() + CCLOAD_IMPORT_DEADLINE,
    )
    .await;
    let refreshed = refresh_result
        .get("refreshed")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or_default();
    let mut job = access_token_import_job(
        &expected_job_id,
        ImportProgress {
            total: ids.len(),
            completed: ids.len(),
            added,
            skipped,
            refreshed,
            failed,
        },
        errors,
        successful,
        snapshot_saved,
    );
    add_model_catalog_stats(&mut job, &state, &batch_stats).await;
    let _ = set_registry_job(&state, "sub2api", &server_id, job, Some(&expected_job_id));
}

pub(super) async fn start_sub2api_import(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(server_id): AxumPath<String>,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let server = registry_value(&state, "sub2api", &server_id)?;
    let value = super::account_json_body(body).await?;
    let object = value.as_object().ok_or_else(ApiError::validation)?;
    let ids = match object.get("account_ids") {
        None => Vec::new(),
        Some(Value::Array(values)) => {
            if values.iter().any(|value| !value.is_string()) {
                return Err(ApiError::validation());
            }
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        }
        Some(_) => return Err(ApiError::validation()),
    };
    if ids.is_empty() {
        return Err(ApiError::invalid_request());
    }
    let job_id = format!("job-{}-{}", std::process::id(), now_nanos());
    let job = json!({
        "job_id": job_id.clone(),
        "status": "pending",
        "created_at": python_iso_timestamp(SystemTime::now()),
        "updated_at": python_iso_timestamp(SystemTime::now()),
        "total": ids.len(),
        "completed": 0,
        "added": 0,
        "skipped": 0,
        "refreshed": 0,
        "failed": 0,
        "errors": [],
    });
    let saved = begin_registry_job(&state, "sub2api", &server_id, job)?;
    tokio::spawn(execute_sub2api_import(
        state, server_id, server, ids, job_id,
    ));
    Ok(Json(
        json!({"import_job": public_import_job(saved.get("import_job"))}),
    ))
}

pub(super) async fn sub2api_import_progress(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(server_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let server = registry_value(&state, "sub2api", &server_id)?;
    Ok(Json(
        json!({"import_job": public_import_job(server.get("import_job"))}),
    ))
}

async fn ccload_login(
    state: &AppState,
    server: &Map<String, Value>,
) -> Result<(String, String), ApiError> {
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    ccload_login_until(state, server, deadline).await
}

async fn ccload_login_until(
    state: &AppState,
    server: &Map<String, Value>,
    deadline: std::time::Instant,
) -> Result<(String, String), ApiError> {
    let base = server
        .get("base_url")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::invalid_request)?;
    let password = server
        .get("password")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(ApiError::invalid_request)?;
    let remaining = deadline
        .checked_duration_since(std::time::Instant::now())
        .ok_or_else(ApiError::upstream)?;
    let value = tokio::time::timeout(
        remaining,
        remote_json(
            state,
            state
                .client
                .post(format!("{base}/login"))
                .json(&json!({"mode": "admin", "password": password})),
        ),
    )
    .await
    .map_err(|_| ApiError::upstream())??;
    let data = value
        .get("data")
        .and_then(Value::as_object)
        .ok_or_else(ApiError::upstream)?;
    let token = data
        .get("token")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(ApiError::upstream)?;
    if data.get("role").and_then(Value::as_str) != Some("admin") {
        return Err(ApiError::unauthorized());
    }
    Ok((base.to_owned(), token.to_owned()))
}

pub(super) async fn ccload_servers(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    Ok(Json(
        json!({"servers": public_ccload_items(registry_items(&state, "ccload"))}),
    ))
}

pub(super) async fn create_ccload_server(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let mut object = super::account_json_body(body)
        .await?
        .as_object()
        .cloned()
        .ok_or_else(ApiError::invalid_request)?;
    let name = required_name(&object)?;
    let base = normalized_remote_url(object.get("base_url"))?;
    let password = object
        .get("password")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(ApiError::invalid_request)?;
    if password.len() > 16 * 1024 {
        return Err(ApiError::validation());
    }
    object.insert("name".to_owned(), Value::String(name));
    object.insert("base_url".to_owned(), Value::String(base));
    object.insert("password".to_owned(), Value::String(password));
    object.insert("id".to_owned(), Value::String(new_registry_id("ccload")));
    object.insert("import_job".to_owned(), Value::Null);
    let item = save_registry_item(&state, "ccload", object)?;
    Ok(Json(
        json!({"server": public_ccload_item(&item), "servers": public_ccload_items(registry_items(&state, "ccload"))}),
    ))
}

pub(super) async fn update_ccload_server(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(server_id): AxumPath<String>,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let updates = super::account_json_body(body)
        .await?
        .as_object()
        .cloned()
        .ok_or_else(ApiError::invalid_request)?;
    let mut candidate = registry_value(&state, "ccload", &server_id)?;
    for key in ["name", "base_url", "password"] {
        if let Some(value) = updates.get(key) {
            candidate.insert(key.to_owned(), value.clone());
        }
    }
    if updates.contains_key("name") {
        candidate.insert("name".to_owned(), Value::String(required_name(&candidate)?));
    }
    if updates.contains_key("base_url") {
        candidate.insert(
            "base_url".to_owned(),
            Value::String(normalized_remote_url(candidate.get("base_url"))?),
        );
    }
    if candidate
        .get("password")
        .and_then(Value::as_str)
        .is_none_or(|value| value.trim().is_empty())
    {
        return Err(ApiError::invalid_request());
    }
    let item = registry_update(&state, "ccload", &server_id, candidate)?;
    Ok(Json(
        json!({"server": public_ccload_item(&item), "servers": public_ccload_items(registry_items(&state, "ccload"))}),
    ))
}

pub(super) async fn delete_ccload_server(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(server_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let values = super::mutate_server_registry(&state, "ccload", |values| {
        let before = values.len();
        values.retain(|value| value.get("id").and_then(Value::as_str) != Some(server_id.as_str()));
        if before == values.len() {
            return Err(ApiError::not_found());
        }
        Ok(values.clone())
    })?;
    Ok(Json(json!({"servers": public_ccload_items(values)})))
}

pub(super) async fn ccload_channels(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(server_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let server = registry_value(&state, "ccload", &server_id)?;
    let (base, token) = ccload_login(&state, &server).await?;
    let deadline = std::time::Instant::now() + CCLOAD_CHANNEL_BROWSE_DEADLINE;
    let mut channels = Vec::new();
    let mut offset = 0usize;
    let mut page_count = 0usize;
    let mut expected_count = None;
    loop {
        page_count = page_count.saturating_add(1);
        if page_count > CCLOAD_MAX_CHANNEL_PAGES {
            return Err(ApiError::upstream());
        }
        let remaining = deadline
            .checked_duration_since(std::time::Instant::now())
            .ok_or_else(ApiError::upstream)?;
        let value = tokio::time::timeout(
            remaining,
            remote_json(
                &state,
                state
                    .client
                    .get(format!("{base}/admin/channels"))
                    .query(&[
                        ("auth_type", "codex_oauth"),
                        ("limit", "200"),
                        ("offset", &offset.to_string()),
                    ])
                    .bearer_auth(&token),
            ),
        )
        .await
        .map_err(|_| ApiError::upstream())??;
        let data = value
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(ApiError::upstream)?;
        let count = match value.get("count") {
            None => None,
            Some(Value::Number(number)) => number
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(ApiError::upstream)
                .map(Some)?,
            Some(_) => return Err(ApiError::upstream()),
        };
        if let Some(count) = count {
            if let Some(expected) = expected_count {
                if expected != count {
                    return Err(ApiError::upstream());
                }
            } else {
                if page_count != 1 {
                    return Err(ApiError::upstream());
                }
                expected_count = Some(count);
            }
            if count < offset || count > CCLOAD_MAX_CHANNELS {
                return Err(ApiError::upstream());
            }
        } else if expected_count.is_some() {
            return Err(ApiError::upstream());
        }
        let next_offset = offset
            .checked_add(data.len())
            .filter(|value| *value <= CCLOAD_MAX_CHANNELS)
            .ok_or_else(ApiError::upstream)?;
        if count.is_some_and(|count| next_offset > count) {
            return Err(ApiError::upstream());
        }
        for value in data {
            let object = value.as_object().ok_or_else(ApiError::upstream)?;
            if object
                .get("auth_type")
                .and_then(Value::as_str)
                .map(str::trim)
                != Some("codex_oauth")
            {
                continue;
            }
            let id = clean_ccload_channel_id(object.get("id")).ok_or_else(ApiError::upstream)?;
            let enabled = object
                .get("enabled")
                .and_then(Value::as_bool)
                .ok_or_else(ApiError::upstream)?;
            channels.push(json!({
                "id": id,
                "name": bounded_public_text(object.get("name"), 256),
                "enabled": enabled,
                "plan_type": bounded_public_text(object.get("codex_plan_type"), 256),
                "subscription_active_until": bounded_public_text(object.get("codex_subscription_active_until"), 256),
                "models": [],
                "models_loaded": false,
                "model_load_status": if enabled { "pending" } else { "disabled" },
            }));
        }
        offset = next_offset;
        match count {
            Some(count) if offset >= count => break,
            Some(_) if data.is_empty() => return Err(ApiError::upstream()),
            Some(_) => {}
            None if data.len() < 200 => break,
            None if data.is_empty() => break,
            None => {}
        }
    }
    Ok(Json(json!({"server_id": server_id, "channels": channels})))
}

pub(super) async fn ccload_channel_models(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(server_id): AxumPath<String>,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let server = registry_value(&state, "ccload", &server_id)?;
    let request = super::account_json_body(body).await?;
    let ids = clean_ccload_channel_ids(request.get("channel_ids"), 50)?;
    let requested_ids = ids.clone();
    let catalogs = match tokio::time::timeout(
        CCLOAD_CHANNEL_MODEL_DEADLINE,
        load_ccload_channel_models(&state, &server_id, &server, ids),
    )
    .await
    {
        Ok(Ok(catalogs)) => catalogs,
        Ok(Err(_)) | Err(_) => requested_ids
            .iter()
            .map(|id| terminal_ccload_channel_catalog(id, "failed"))
            .collect(),
    };
    Ok(Json(json!({"server_id": server_id, "channels": catalogs})))
}

fn terminal_ccload_channel_catalog(id: &str, status: &str) -> Value {
    json!({
        "id": id,
        "plan_type": "",
        "models": [],
        "model_sources": {},
        "models_loaded": false,
        "model_load_status": status,
    })
}

async fn load_ccload_channel_models(
    state: &AppState,
    _server_id: &str,
    server: &Map<String, Value>,
    ids: Vec<String>,
) -> Result<Vec<Value>, ApiError> {
    let login_deadline = std::time::Instant::now() + CCLOAD_CHANNEL_MODEL_LOGIN_DEADLINE;
    let (base, token) = ccload_login_until(state, server, login_deadline).await?;
    let deadline = std::time::Instant::now() + CCLOAD_CHANNEL_MODEL_DEADLINE;
    let permits = Arc::new(Semaphore::new(CCLOAD_CHANNEL_MODEL_CONCURRENCY));
    let catalog_count = ids.len();
    let mut requests = FuturesUnordered::new();

    for (index, id) in ids.into_iter().enumerate() {
        let request_state = state.clone();
        let request_client = state.client.clone();
        let request_base = base.clone();
        let request_token = token.clone();
        let request_permits = permits.clone();
        requests.push(async move {
            let mut catalog = json!({
                "id": id,
                "plan_type": "",
                "models": [],
                "model_sources": {},
                "models_loaded": false,
                "model_load_status": "pending",
            });
            let _permit = match tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                request_permits.acquire_owned(),
            )
            .await
            {
                Ok(Ok(permit)) => permit,
                _ => {
                    catalog["model_load_status"] = Value::String("timeout".to_owned());
                    return (index, catalog);
                }
            };
            let editor = match tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                remote_json(
                    &request_state,
                    request_client
                        .get(format!("{request_base}/admin/channels/{id}/editor"))
                        .bearer_auth(&request_token),
                ),
            )
            .await
            {
                Ok(Ok(editor)) => editor,
                Ok(Err(_)) => {
                    catalog["model_load_status"] = Value::String("failed".to_owned());
                    return (index, catalog);
                }
                Err(_) => {
                    catalog["model_load_status"] = Value::String("timeout".to_owned());
                    return (index, catalog);
                }
            };
            let channel = editor.get("data").and_then(|value| value.get("channel"));
            let channel_matches = channel
                .and_then(|value| clean_ccload_channel_id(value.get("id")))
                .is_some_and(|value| value == id)
                && channel
                    .and_then(|value| value.get("enabled"))
                    .and_then(Value::as_bool)
                    != Some(false)
                && channel
                    .and_then(|value| value.get("auth_type"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    == Some("codex_oauth");
            if !channel_matches {
                catalog["model_load_status"] = Value::String("failed".to_owned());
                return (index, catalog);
            }
            let credential_value = editor
                .get("data")
                .and_then(|value| value.get("oauth_credential"));
            let Some(access) = normalized_ccload_credential(credential_value) else {
                catalog["model_load_status"] = Value::String("failed".to_owned());
                return (index, catalog);
            };
            let account_type = credential_value
                .and_then(|value| value.get("plan_type"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| value.chars().count() <= 256)
                .or_else(|| {
                    channel
                        .and_then(|value| value.get("codex_plan_type"))
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| value.chars().count() <= 256)
                })
                .map(ToOwned::to_owned);
            if let Some(group) = account_type.as_deref().map(str::to_ascii_lowercase) {
                catalog["model_group"] = Value::String(group);
            }
            let account_id = credential_value
                .and_then(|value| value.get("account_id"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty() && value.chars().count() <= 256)
                .map(ToOwned::to_owned);
            let account = json!({
                "access_token": access,
                "type": account_type.clone(),
                "chatgpt_account_id": account_id,
            });
            let refresh = tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                super::refresh_access_token_account(&request_state, &account),
            )
            .await;
            let (snapshot, refresh_status) = match refresh {
                Ok(Ok(snapshot)) => (Some(snapshot), "loaded"),
                Ok(Err(_)) => (None, "failed"),
                Err(_) => (None, "timeout"),
            };
            let fetched = if let Some(snapshot) = snapshot.as_ref() {
                let refreshed_type = snapshot
                    .get("type")
                    .and_then(Value::as_str)
                    .or(account_type.as_deref())
                    .unwrap_or("free");
                let account_proxy = credential_value
                    .and_then(|value| value.get("proxy"))
                    .and_then(Value::as_str);
                super::fetch_imported_account_model_catalog(
                    &request_state,
                    refreshed_type,
                    &access,
                    account_id.as_deref(),
                    account_proxy,
                    deadline,
                    None,
                )
                .await
            } else {
                None
            };
            let (models, sources) = merge_ccload_model_catalog(None, None, fetched.as_deref());
            let has_web = sources.as_object().is_some_and(|sources| {
                sources
                    .values()
                    .any(|source| source.as_str() == Some("web"))
            });
            catalog["models"] = models;
            catalog["model_sources"] = sources;
            catalog["models_loaded"] = Value::Bool(has_web);
            let image_models = snapshot
                .as_ref()
                .filter(|snapshot| {
                    snapshot
                        .get("_verified_image_capability")
                        .and_then(Value::as_bool)
                        == Some(true)
                        && snapshot
                            .get("quota")
                            .and_then(Value::as_u64)
                            .is_some_and(|quota| quota > 0)
                })
                .map(|_| {
                    super::WEB_IMAGE_MODELS
                        .iter()
                        .map(|model| (*model).to_owned())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            apply_ccload_image_capability(&mut catalog, &image_models);
            let has_image = !image_models.is_empty();
            catalog["model_load_status"] = Value::String(
                if has_web {
                    "loaded"
                } else if has_image {
                    "partial"
                } else {
                    refresh_status
                }
                .to_owned(),
            );
            (index, catalog)
        });
    }

    let mut catalogs = vec![Value::Null; catalog_count];
    while let Some((index, catalog)) = requests.next().await {
        catalogs[index] = catalog;
    }
    Ok(catalogs)
}

pub(super) async fn start_ccload_import(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(server_id): AxumPath<String>,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let request = super::account_json_body(body).await?;
    let ids = clean_ccload_channel_ids(request.get("channel_ids"), 5_000)?;
    registry_value(&state, "ccload", &server_id)?;
    let job_id = format!("job-{}-{}", std::process::id(), now_nanos());
    let job = json!({
        "job_id": job_id.clone(),
        "status": "pending",
        "created_at": python_iso_timestamp(SystemTime::now()),
        "updated_at": python_iso_timestamp(SystemTime::now()),
        "total": ids.len(),
        "completed": 0,
        "added": 0,
        "skipped": 0,
        "refreshed": 0,
        "failed": 0,
        "errors": [],
    });
    let saved = begin_registry_job(&state, "ccload", &server_id, job)?;
    tokio::spawn(execute_ccload_import(state, server_id, ids, job_id));
    Ok(Json(
        json!({"import_job": public_import_job(saved.get("import_job"))}),
    ))
}

async fn execute_ccload_import(
    state: AppState,
    server_id: String,
    ids: Vec<String>,
    expected_job_id: String,
) {
    let batch_stats = Arc::new(super::ImportedModelCatalogBatchStats::default());
    let deadline = std::time::Instant::now() + CCLOAD_IMPORT_DEADLINE;
    let Ok(server) = registry_value(&state, "ccload", &server_id) else {
        return;
    };
    let created_at = server
        .get("import_job")
        .and_then(Value::as_object)
        .and_then(|job| job.get("created_at"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let publish_progress = |completed: usize,
                            phase: &str,
                            phase_completed: usize,
                            phase_total: usize,
                            added: usize,
                            skipped: usize,
                            refreshed: usize,
                            failed: usize,
                            status: &str,
                            errors: &[Value]| {
        let job = progress_job_with_phase(
            &expected_job_id,
            ImportProgress {
                total: ids.len(),
                completed,
                added,
                skipped,
                refreshed,
                failed,
            },
            status,
            errors.to_vec(),
            created_at.as_deref(),
            phase,
            phase_completed,
            phase_total,
        );
        let _ = set_registry_job(&state, "ccload", &server_id, job, Some(&expected_job_id));
    };
    publish_progress(0, "connecting", 0, 1, 0, 0, 0, 0, "running", &[]);
    let Ok((base, token)) = ccload_login_until(&state, &server, deadline).await else {
        let job = import_job(
            &expected_job_id,
            ids.len(),
            0,
            0,
            0,
            ids.len(),
            vec![json!({"name": "ccLoad", "error": "ccLoad 登录失败"})],
        );
        let _ = set_registry_job(&state, "ccload", &server_id, job, Some(&expected_job_id));
        return;
    };
    let mut errors = Vec::new();
    let mut failed = 0usize;
    let mut candidates = Vec::new();
    for (index, id) in ids.iter().enumerate() {
        let result = remote_json_until(
            &state,
            state
                .client
                .get(format!("{base}/admin/channels/{id}/editor"))
                .bearer_auth(&token),
            deadline,
        )
        .await;
        match result {
            Ok(value) => {
                let channel = value.get("data").and_then(|item| item.get("channel"));
                let credential = value
                    .get("data")
                    .and_then(|item| item.get("oauth_credential"));
                let channel_matches = channel
                    .and_then(|item| clean_ccload_channel_id(item.get("id")))
                    .is_some_and(|item| item == *id)
                    && channel
                        .and_then(|item| item.get("auth_type"))
                        .and_then(Value::as_str)
                        .map(str::trim)
                        == Some("codex_oauth");
                let mut accepted = false;
                if channel_matches
                    && let Some(access_token) = normalized_ccload_credential(credential)
                {
                    let mut candidate = json!({
                            "access_token": access_token,
                            "source_type": "web",
                    });
                    let plan_type = credential
                        .and_then(|value| value.get("plan_type"))
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .or_else(|| {
                            channel
                                .and_then(|value| value.get("codex_plan_type"))
                                .and_then(Value::as_str)
                                .map(str::trim)
                                .filter(|value| !value.is_empty())
                        });
                    if let Some(plan_type) = plan_type {
                        candidate["type"] = Value::String(plan_type.to_owned());
                    }
                    if let Some(account_id) = credential
                        .and_then(|value| value.get("account_id"))
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                    {
                        candidate["chatgpt_account_id"] = Value::String(account_id.to_owned());
                    }
                    if let Some(created_at) = ccload_recent_refresh_time(channel, credential) {
                        candidate["created_at"] = Value::String(created_at);
                    }
                    candidates.push(candidate);
                    accepted = true;
                }
                if !accepted {
                    failed += 1;
                    push_import_error(
                        &mut errors,
                        json!({"name": id, "error": "ccLoad OAuth 凭据无效"}),
                    );
                }
            }
            Err(_) => {
                failed += 1;
                push_import_error(
                    &mut errors,
                    json!({"name": id, "error": "ccLoad 凭据获取失败"}),
                );
            }
        }
        publish_progress(
            index.saturating_add(1),
            "fetching_credentials",
            index.saturating_add(1),
            ids.len(),
            0,
            0,
            0,
            failed,
            "running",
            &errors,
        );
    }
    let fetch_failed = failed;
    if candidates.is_empty() {
        if errors.is_empty() {
            push_import_error(
                &mut errors,
                json!({"name": "ccLoad", "error": "账号凭据不可用"}),
            );
        }
        let job = progress_job_with_created(
            &expected_job_id,
            ImportProgress {
                total: ids.len(),
                completed: ids.len(),
                added: 0,
                skipped: 0,
                refreshed: 0,
                failed: ids.len(),
            },
            "failed",
            errors,
            created_at.as_deref(),
        );
        let _ = set_registry_job(&state, "ccload", &server_id, job, Some(&expected_job_id));
        return;
    }

    let imported_tokens = candidates
        .iter()
        .filter_map(|item| item.get("access_token").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    publish_progress(
        ids.len(),
        "merging_accounts",
        0,
        1,
        0,
        0,
        0,
        fetch_failed,
        "running",
        &errors,
    );
    let (added, skipped) = match tokio::time::timeout(
        deadline
            .checked_duration_since(std::time::Instant::now())
            .unwrap_or_default(),
        state.account_store.merge_import_records(candidates.clone()),
    )
    .await
    {
        Ok(Ok(counts)) => counts,
        Ok(Err(_)) | Err(_) => {
            push_import_error(
                &mut errors,
                json!({"name": "accounts", "error": "账号快照写入失败或超时"}),
            );
            let job = progress_job_with_created(
                &expected_job_id,
                ImportProgress {
                    total: ids.len(),
                    completed: ids.len(),
                    added: 0,
                    skipped: 0,
                    refreshed: 0,
                    failed: ids.len(),
                },
                "failed",
                errors,
                created_at.as_deref(),
            );
            let _ = set_registry_job(&state, "ccload", &server_id, job, Some(&expected_job_id));
            return;
        }
    };
    publish_progress(
        ids.len(),
        "refreshing_accounts",
        0,
        imported_tokens.len(),
        added,
        skipped,
        0,
        fetch_failed,
        "running",
        &errors,
    );
    let refresh_result = super::refresh_imported_accounts_with_batch_until(
        &state,
        &imported_tokens,
        batch_stats.clone(),
        deadline,
    )
    .await;
    let refreshed = refresh_result
        .get("refreshed")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or_default();
    let refresh_errors = refresh_result
        .get("errors")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for error in refresh_errors {
        push_import_error(&mut errors, error);
    }
    let refreshed_records = state.account_store.raw_records();
    let mut catalog_fetches = FuturesUnordered::new();
    for candidate in &candidates {
        let Some(token) = candidate.get("access_token").and_then(Value::as_str) else {
            continue;
        };
        let current = refreshed_records
            .iter()
            .find(|record| super::account_token(record).as_deref() == Some(token))
            .unwrap_or(candidate);
        let account_type = current
            .get("type")
            .and_then(Value::as_str)
            .or_else(|| candidate.get("type").and_then(Value::as_str))
            .unwrap_or("free")
            .to_owned();
        let account_id = current
            .get("chatgpt_account_id")
            .and_then(Value::as_str)
            .or_else(|| candidate.get("chatgpt_account_id").and_then(Value::as_str))
            .map(ToOwned::to_owned);
        let account_proxy = current
            .get("proxy")
            .and_then(Value::as_str)
            .or_else(|| candidate.get("proxy").and_then(Value::as_str))
            .map(ToOwned::to_owned);
        let request_state = state.clone();
        let token = token.to_owned();
        let batch = batch_stats.clone();
        catalog_fetches.push(async move {
            super::fetch_imported_account_model_catalog(
                &request_state,
                &account_type,
                &token,
                account_id.as_deref(),
                account_proxy.as_deref(),
                deadline,
                Some(batch),
            )
            .await
        });
    }
    while catalog_fetches.next().await.is_some() {}
    let refresh_failed = imported_tokens.len().saturating_sub(refreshed);
    publish_progress(
        ids.len(),
        "refreshing_accounts",
        refreshed,
        imported_tokens.len(),
        added,
        skipped,
        refreshed,
        fetch_failed.saturating_add(refresh_failed),
        "running",
        &errors,
    );

    let failed = fetch_failed.saturating_add(refresh_failed).min(ids.len());
    let status = if failed > 0 { "failed" } else { "completed" };
    let mut job = progress_job_with_created(
        &expected_job_id,
        ImportProgress {
            total: ids.len(),
            completed: ids.len(),
            added,
            skipped,
            refreshed,
            failed,
        },
        status,
        errors,
        created_at.as_deref(),
    );
    add_model_catalog_stats(&mut job, &state, &batch_stats).await;
    let _ = set_registry_job(&state, "ccload", &server_id, job, Some(&expected_job_id));
}

pub(super) async fn ccload_import_progress(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(server_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let server = registry_value(&state, "ccload", &server_id)?;
    Ok(Json(
        json!({"import_job": public_import_job(server.get("import_job"))}),
    ))
}

fn webdav_url(settings: &Map<String, Value>, relative: &str) -> Result<Url, ApiError> {
    let raw = settings
        .get("webdav_url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(ApiError::invalid_request)?;
    let mut url = Url::parse(raw).map_err(|_| ApiError::invalid_request())?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(ApiError::invalid_request());
    }
    let root = settings
        .get("webdav_root_path")
        .and_then(Value::as_str)
        .unwrap_or("chatgpt2api/images")
        .trim_matches('/');
    let mut segments = root.split('/').filter(|value| !value.is_empty());
    let relative = relative.trim_matches('/');
    let mut path = url
        .path_segments_mut()
        .map_err(|_| ApiError::invalid_request())?;
    path.pop_if_empty();
    for segment in segments.by_ref() {
        path.push(segment);
    }
    for segment in relative.split('/').filter(|value| !value.is_empty()) {
        path.push(segment);
    }
    drop(path);
    Ok(url)
}

fn webdav_auth(
    request: reqwest::RequestBuilder,
    settings: &Map<String, Value>,
) -> reqwest::RequestBuilder {
    match (
        settings.get("webdav_username").and_then(Value::as_str),
        settings.get("webdav_password").and_then(Value::as_str),
    ) {
        (Some(username), Some(password)) if !username.is_empty() || !password.is_empty() => {
            request.basic_auth(username, Some(password))
        }
        _ => request,
    }
}

async fn webdav_mkcol_tree(
    client: &Client,
    settings: &Map<String, Value>,
    relative: &str,
) -> Result<(), ApiError> {
    async fn create_directory(
        client: &Client,
        settings: &Map<String, Value>,
        url: Url,
    ) -> Result<(), ApiError> {
        let method = Method::from_bytes(b"MKCOL").map_err(|_| ApiError::invalid_request())?;
        let response = webdav_auth(client.request(method, url.to_string()), settings)
            .send()
            .await
            .map_err(|_| ApiError::unavailable())?;
        if response.status().is_success() || response.status() == StatusCode::METHOD_NOT_ALLOWED {
            Ok(())
        } else {
            Err(ApiError::upstream_detail(format!(
                "WebDAV MKCOL failed: HTTP {}",
                response.status().as_u16()
            )))
        }
    }

    let parent = Path::new(relative)
        .parent()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .trim_matches('/');
    let raw = settings
        .get("webdav_url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(ApiError::invalid_request)?;
    let mut current = Url::parse(raw).map_err(|_| ApiError::invalid_request())?;
    let root = settings
        .get("webdav_root_path")
        .and_then(Value::as_str)
        .unwrap_or("chatgpt2api/images")
        .trim_matches('/');
    for segment in root
        .split('/')
        .chain(parent.split('/'))
        .filter(|value| !value.is_empty())
    {
        {
            let mut path = current
                .path_segments_mut()
                .map_err(|_| ApiError::invalid_request())?;
            path.pop_if_empty();
            path.push(segment);
        }
        create_directory(client, settings, current.clone()).await?;
    }
    Ok(())
}

async fn webdav_put_image(
    client: &Client,
    settings: &Map<String, Value>,
    relative: &str,
    bytes: Vec<u8>,
) -> Result<String, ApiError> {
    webdav_mkcol_tree(client, settings, relative).await?;
    let url = webdav_url(settings, relative)?;
    let response = webdav_auth(client.put(url.to_string()).body(bytes), settings)
        .header(
            header::CONTENT_TYPE,
            image_content_type(Path::new(relative)),
        )
        .send()
        .await
        .map_err(|_| ApiError::unavailable())?;
    if response.status().is_success() {
        Ok(url.to_string())
    } else {
        Err(ApiError::upstream_detail(format!(
            "WebDAV PUT failed: HTTP {}",
            response.status().as_u16()
        )))
    }
}

pub(super) async fn store_generated_image(
    state: &AppState,
    relative: &str,
    bytes: Vec<u8>,
) -> Result<Option<String>, ApiError> {
    cleanup_old_images(state);
    let settings = image_storage_settings(state);
    let enabled = settings
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let raw_mode = settings
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("local");
    let mode = if enabled {
        match raw_mode {
            "webdav" => "webdav",
            "both" => "both",
            _ => "local",
        }
    } else {
        "local"
    };
    let stored_local = matches!(mode, "local" | "both");
    let stored_webdav = matches!(mode, "webdav" | "both");
    let relative = safe_relative_path(relative).ok_or_else(ApiError::invalid_request)?;
    let relative = relative.to_string_lossy().replace('\\', "/");
    if stored_local {
        let path = image_root(state).join(&relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|_| ApiError::unavailable())?;
        }
        super::atomic_replace_checked_with_limit(
            &path,
            &bytes,
            super::MAX_NATIVE_IMAGE_BYTES as u64,
            false,
        )?;
    }
    let remote_url = if stored_webdav {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|_| ApiError::unavailable())?;
        webdav_put_image(&client, &settings, &relative, bytes.clone()).await?
    } else {
        String::new()
    };
    let created_at = image_local_timestamp();
    let date = relative.split('/').take(3).collect::<Vec<_>>().join("-");
    let date = if relative.split('/').count() >= 4 {
        date
    } else {
        created_at.get(..10).unwrap_or("1970-01-01").to_owned()
    };
    let dimensions = ImageReader::new(Cursor::new(bytes.as_slice()))
        .with_guessed_format()
        .ok()
        .and_then(|reader| reader.decode().ok())
        .map(|image| (image.width(), image.height()));
    let mut item = json!({
        "rel": relative,
        "path": relative,
        "name": Path::new(&relative).file_name().and_then(|value| value.to_str()).unwrap_or("image"),
        "date": date,
        "size": bytes.len(),
        "created_at": created_at,
        "storage": match (stored_local, stored_webdav) {
            (true, true) => "both",
            (false, true) => "webdav",
            _ => "local",
        },
        "local": stored_local,
        "webdav": stored_webdav,
        "remote_url": remote_url,
    });
    if let Some((width, height)) = dimensions {
        item["width"] = json!(width);
        item["height"] = json!(height);
    }
    update_image_index(state, |items| {
        items.insert(relative.clone(), item);
        Ok(())
    })?;
    Ok(settings
        .get("public_base_url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|base| format!("{}/{}", base.trim_end_matches('/'), relative)))
}

pub(super) async fn read_stored_image(
    state: &AppState,
    relative: &Path,
) -> Result<Vec<u8>, ApiError> {
    let relative = relative.to_string_lossy().replace('\\', "/");
    let root = image_root(state);
    let local_path = root.join(&relative);
    if fs::symlink_metadata(&local_path).is_ok() {
        let path = safe_regular_file(&root, Path::new(&relative))?;
        return read_bounded(&path, MAX_IMAGE_ARCHIVE_BYTES as u64);
    }
    let indexed = {
        let _guard = IMAGE_INDEX_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        read_image_index_unlocked(state)?.get(&relative).cloned()
    };
    if !indexed
        .as_ref()
        .and_then(|item| item.get("webdav"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(ApiError::not_found());
    }
    let settings = image_storage_settings(state);
    let url = webdav_url(&settings, &relative)?;
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|_| ApiError::unavailable())?;
    let response = webdav_auth(client.get(url.to_string()), &settings)
        .send()
        .await
        .map_err(|_| ApiError::unavailable())?;
    if response.status() == StatusCode::NOT_FOUND {
        return Err(ApiError::not_found());
    }
    if !response.status().is_success()
        || response
            .content_length()
            .is_some_and(|length| length > MAX_IMAGE_ARCHIVE_BYTES as u64)
    {
        return Err(ApiError::unavailable());
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|_| ApiError::unavailable())?;
    if bytes.len() > MAX_IMAGE_ARCHIVE_BYTES {
        return Err(ApiError::unavailable());
    }
    Ok(bytes.to_vec())
}

async fn delete_stored_image(state: &AppState, relative: &str) -> Result<bool, ApiError> {
    let safe = safe_relative_path(relative).ok_or_else(ApiError::invalid_request)?;
    let relative = safe.to_string_lossy().replace('\\', "/");
    let root = image_root(state);
    let path = root.join(&safe);
    let mut removed = false;
    if fs::symlink_metadata(&path).is_ok() {
        let path = safe_regular_file(&root, &safe)?;
        fs::remove_file(path).map_err(|_| ApiError::unavailable())?;
        removed = true;
    }
    let indexed = {
        let _guard = IMAGE_INDEX_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        read_image_index_unlocked(state)?.get(&relative).cloned()
    };
    let stored_webdav = indexed
        .as_ref()
        .and_then(|item| item.get("webdav"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if stored_webdav {
        let settings = image_storage_settings(state);
        let url = webdav_url(&settings, &relative)?;
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|_| ApiError::unavailable())?;
        let response = webdav_auth(client.delete(url.to_string()), &settings)
            .send()
            .await
            .map_err(|_| ApiError::unavailable());
        match response {
            Ok(response)
                if response.status().is_success() || response.status() == StatusCode::NOT_FOUND =>
            {
                removed |= response.status() != StatusCode::NOT_FOUND;
            }
            Ok(_) | Err(_) if removed => {}
            Ok(_) | Err(_) => return Err(ApiError::unavailable()),
        }
    }
    update_image_index(state, |items| {
        items.remove(&relative);
        Ok(())
    })?;
    super::remove_image_tag_for_path(state, &relative)?;
    let thumbnail_root = state.data_dir.join("image-thumbnails");
    let _ = fs::remove_file(thumbnail_root.join(format!("{relative}.png")));
    let _ = fs::remove_file(thumbnail_root.join(&safe));
    remove_empty_image_dirs(&root);
    remove_empty_image_dirs(&state.data_dir.join("image-thumbnails"));
    Ok(removed)
}

pub(super) async fn test_image_storage(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let settings = image_storage_settings(&state);
    let url = settings
        .get("webdav_url")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();
    if url.is_empty() {
        return Ok(Json(
            json!({"result": {"ok": false, "status": 0, "error": "WebDAV URL is required"}}),
        ));
    }
    if !Url::parse(url).is_ok_and(|url| matches!(url.scheme(), "http" | "https")) {
        return Ok(Json(
            json!({"result": {"ok": false, "status": 0, "error": "invalid WebDAV URL"}}),
        ));
    }
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|_| ApiError::unavailable())?;
    let test_path = ".chatgpt2api_webdav_test.txt";
    let result = async {
        webdav_mkcol_tree(&client, &settings, test_path).await?;
        let put_url = webdav_url(&settings, test_path)?;
        let put = webdav_auth(
            client
                .put(put_url.to_string())
                .body("chatgpt2api webdav test\n"),
            &settings,
        )
        .header(header::CONTENT_TYPE, "text/plain")
        .send()
        .await
        .map_err(|_| ApiError::unavailable())?;
        if !put.status().is_success() {
            return Err(ApiError::upstream_detail(format!(
                "WebDAV PUT failed: HTTP {}",
                put.status().as_u16()
            )));
        }
        let delete_url = webdav_url(&settings, test_path)?;
        let delete = webdav_auth(client.delete(delete_url.to_string()), &settings)
            .send()
            .await
            .map_err(|_| ApiError::unavailable())?;
        if delete.status().is_success() || delete.status() == StatusCode::NOT_FOUND {
            Ok(json!({"ok": true, "status": 200, "error": null}))
        } else {
            Err(ApiError::upstream_detail(format!(
                "WebDAV DELETE failed: HTTP {}",
                delete.status().as_u16()
            )))
        }
    }
    .await;
    let result = result.unwrap_or_else(
        |error: ApiError| json!({"ok": false, "status": 0, "error": error.message()}),
    );
    Ok(Json(json!({"result": result})))
}

pub(super) async fn sync_image_storage(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let settings = image_storage_settings(&state);
    let enabled = settings
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mode = settings
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("local");
    if !enabled || !matches!(mode, "webdav" | "both") {
        return Err(ApiError::management_bad_request(
            "image_storage_disabled",
            "WebDAV 图片存储未启用",
        ));
    }
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|_| ApiError::unavailable())?;
    let mut uploaded = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;
    let indexed = {
        let _guard = IMAGE_INDEX_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        read_image_index_unlocked(&state)?
    };
    for (relative, path) in image_files(&state) {
        if indexed
            .get(&relative)
            .and_then(|item| item.get("webdav"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            skipped += 1;
            continue;
        }
        match fs::read(&path) {
            Ok(bytes) => match webdav_put_image(&client, &settings, &relative, bytes).await {
                Ok(remote_url) => {
                    let result = update_image_index(&state, |items| {
                        let previous = items.get(&relative).and_then(Value::as_object).cloned();
                        let mut item = local_image_index_item(&relative, &path, previous.as_ref())
                            .ok_or_else(ApiError::unavailable)?;
                        let object = item.as_object_mut().ok_or_else(ApiError::unavailable)?;
                        object.insert("storage".to_owned(), json!("both"));
                        object.insert("webdav".to_owned(), json!(true));
                        object.insert("remote_url".to_owned(), json!(remote_url));
                        items.insert(relative.clone(), item);
                        Ok(())
                    });
                    if result.is_ok() {
                        uploaded += 1;
                    } else {
                        failed += 1;
                    }
                }
                Err(_) => failed += 1,
            },
            Err(_) => failed += 1,
        }
    }
    Ok(Json(
        json!({"result": {"uploaded": uploaded, "skipped": skipped, "failed": failed}}),
    ))
}

fn backup_dir(state: &AppState) -> PathBuf {
    data_file(state, "backups")
}

fn backup_state_path(state: &AppState) -> PathBuf {
    data_file(state, "backup_state.json")
}
fn backup_settings(state: &AppState) -> Value {
    super::normalize_backup(read_config(state).get("backup"), true)
}

fn backup_raw_settings(state: &AppState) -> Map<String, Value> {
    object_or_empty(
        read_config(state)
            .get("backup")
            .cloned()
            .unwrap_or_else(|| json!({})),
    )
}

fn backup_raw_text(settings: &Map<String, Value>, key: &str) -> String {
    settings
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned()
}

fn backup_target_fingerprint(state: &AppState) -> String {
    let settings = backup_raw_settings(state);
    let prefix = {
        let value = backup_raw_text(&settings, "prefix");
        value.trim_matches('/').to_owned()
    };
    let value = json!({
        "account_id": backup_raw_text(&settings, "account_id"),
        "access_key_id": backup_raw_text(&settings, "access_key_id"),
        "bucket": backup_raw_text(&settings, "bucket"),
        "encrypt": bool_or(settings.get("encrypt"), false),
        "passphrase": backup_raw_text(&settings, "passphrase"),
        "prefix": if prefix.is_empty() { "backups".to_owned() } else { prefix },
        "secret_access_key": backup_raw_text(&settings, "secret_access_key"),
    });
    let bytes = serde_json::to_vec(&value).expect("backup fingerprint JSON");
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn backup_encryption_enabled(state: &AppState) -> bool {
    let settings = backup_raw_settings(state);
    bool_or(settings.get("encrypt"), false)
}

#[cfg(test)]
pub(super) fn backup_target_fingerprint_for_test(state: &AppState) -> String {
    backup_target_fingerprint(state)
}

fn backup_state_map(state: &AppState) -> Result<Map<String, Value>, ApiError> {
    let bytes = match fs::read(backup_state_path(state)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Map::new()),
        Err(_) => return Err(ApiError::backup_state_invalid()),
    };
    let value =
        serde_json::from_slice::<Value>(&bytes).map_err(|_| ApiError::backup_state_invalid())?;
    value
        .as_object()
        .cloned()
        .ok_or_else(ApiError::backup_state_invalid)
}
fn backup_state_after_restart(current: &Map<String, Value>) -> Option<Value> {
    let stale_running = current
        .get("running")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || current.get("last_status").and_then(Value::as_str) == Some("running");
    if !stale_running {
        return None;
    }
    let mut recovered = current.clone();
    recovered.remove("running");
    recovered.insert("last_status".to_owned(), json!("idle"));
    recovered.insert("last_error".to_owned(), Value::Null);
    recovered.remove("last_error_code");
    recovered.remove("last_error_status");
    Some(Value::Object(recovered))
}

pub(super) fn recover_backup_state_after_restart(state: &AppState) {
    let path = backup_state_path(state);
    let Ok(Some(_lock)) = super::try_acquire_path_write_lock_sync(&path) else {
        return;
    };
    let Ok(current) = backup_state_map(state) else {
        return;
    };
    let Some(recovered) = backup_state_after_restart(&current) else {
        return;
    };
    let _ = write_json_unlocked(&path, &recovered);
}

fn backup_schedule_due(settings: &Value, state: &Map<String, Value>, now: SystemTime) -> bool {
    if !bool_or(settings.get("enabled"), false)
        || state
            .get("running")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    {
        return false;
    }
    let interval_minutes = settings
        .get("interval_minutes")
        .and_then(Value::as_u64)
        .unwrap_or(360)
        .max(1);
    let Some(last_finished) = state.get("last_finished_at").and_then(Value::as_str) else {
        return true;
    };
    let Ok(finished) = time::OffsetDateTime::parse(
        last_finished,
        &time::format_description::well_known::Rfc3339,
    ) else {
        return true;
    };
    let Ok(now) = time::OffsetDateTime::from_unix_timestamp(
        now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64,
    ) else {
        return true;
    };
    (now - finished).whole_seconds() >= (interval_minutes * 60) as i64
}

fn backup_state_value(current: &Map<String, Value>, key: &str) -> Value {
    current.get(key).cloned().unwrap_or(Value::Null)
}

fn backup_running_state(
    current: &Map<String, Value>,
    started: &str,
    object_key: &str,
    target_fingerprint: &str,
) -> Value {
    let mut next = current.clone();
    next.remove("running");
    next.insert("last_started_at".to_owned(), json!(started));
    next.insert(
        "last_finished_at".to_owned(),
        backup_state_value(current, "last_finished_at"),
    );
    next.insert("last_status".to_owned(), json!("idle"));
    next.insert("last_error".to_owned(), Value::Null);
    next.remove("last_error_code");
    next.remove("last_error_status");
    next.insert(
        "last_object_key".to_owned(),
        backup_state_value(current, "last_object_key"),
    );
    next.insert("pending_object_key".to_owned(), json!(object_key));
    next.insert(
        "pending_target_fingerprint".to_owned(),
        json!(target_fingerprint),
    );
    Value::Object(next)
}

fn backup_error_state(
    current: &Map<String, Value>,
    started: &str,
    pending_key: &str,
    target_fingerprint: &str,
    error_code: &str,
) -> Value {
    let mut next = current.clone();
    next.remove("running");
    next.insert("last_started_at".to_owned(), json!(started));
    next.insert(
        "last_finished_at".to_owned(),
        json!(iso_timestamp(SystemTime::now())),
    );
    next.insert("last_status".to_owned(), json!("error"));
    next.insert("last_error".to_owned(), Value::Null);
    next.insert("last_error_code".to_owned(), json!(error_code));
    next.remove("last_error_status");
    next.insert(
        "last_object_key".to_owned(),
        backup_state_value(current, "last_object_key"),
    );
    next.insert("pending_object_key".to_owned(), json!(pending_key));
    next.insert(
        "pending_target_fingerprint".to_owned(),
        json!(target_fingerprint),
    );
    Value::Object(next)
}

fn backup_error_state_from_api(
    current: &Map<String, Value>,
    started: &str,
    pending_key: &str,
    target_fingerprint: &str,
    error: &ApiError,
) -> Value {
    let code = if error.code().starts_with("r2_") || error.code().starts_with("backup_") {
        error.code()
    } else {
        "backup_failed"
    };
    let mut state = backup_error_state(current, started, pending_key, target_fingerprint, code);
    if let Some(status) = error.detail_status() {
        state["last_error_status"] = json!(status);
    }
    state
}

fn backup_is_configured(state: &AppState) -> bool {
    let settings = backup_raw_settings(state);
    ["account_id", "access_key_id", "secret_access_key", "bucket"]
        .iter()
        .all(|key| !backup_raw_text(&settings, key).is_empty())
}

fn backup_is_remote(state: &AppState) -> bool {
    backup_raw_settings(state)
        .get("provider")
        .and_then(Value::as_str)
        .unwrap_or("cloudflare_r2")
        .eq_ignore_ascii_case("cloudflare_r2")
}

#[derive(Clone, Debug)]
struct R2Object {
    key: String,
    size: u64,
    updated_at: String,
}

struct R2Client {
    client: Client,
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    bucket: String,
    prefix: String,
}

impl R2Client {
    fn from_state(state: &AppState) -> Result<Self, ApiError> {
        Self::from_settings(&backup_raw_settings(state))
    }

    fn from_settings(settings: &Map<String, Value>) -> Result<Self, ApiError> {
        let account_id = backup_raw_text(settings, "account_id");
        let access_key_id = backup_raw_text(settings, "access_key_id");
        let secret_access_key = backup_raw_text(settings, "secret_access_key");
        let bucket = backup_raw_text(settings, "bucket");
        let prefix = backup_raw_text(settings, "prefix")
            .trim_matches('/')
            .to_owned();
        let mut missing = Vec::new();
        if account_id.is_empty() {
            missing.push("Account ID");
        }
        if access_key_id.is_empty() {
            missing.push("Access Key ID");
        }
        if secret_access_key.is_empty() {
            missing.push("Secret Access Key");
        }
        if bucket.is_empty() {
            missing.push("Bucket");
        }
        if !missing.is_empty() {
            return Err(ApiError::backup_r2_config_incomplete(missing.join("、")));
        }
        if account_id.len() > 128
            || access_key_id.len() > 256
            || secret_access_key.len() > 512
            || bucket.len() > 256
            || account_id.chars().any(char::is_whitespace)
            || access_key_id.chars().any(char::is_whitespace)
            || secret_access_key.chars().any(char::is_whitespace)
            || bucket.contains('/')
        {
            return Err(ApiError::backup_r2_message(
                "r2_config_incomplete",
                "R2 配置不完整",
            ));
        }
        let prefix = if prefix.is_empty() {
            "backups".to_owned()
        } else {
            prefix
        };
        if prefix.len() > 1024
            || prefix.starts_with('/')
            || prefix.ends_with('/')
            || prefix
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == ".." || !part.is_ascii())
        {
            return Err(ApiError::invalid_request());
        }
        let endpoint = {
            #[cfg(test)]
            if let Some(endpoint) = BACKUP_R2_ENDPOINT
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
            {
                endpoint
            } else {
                format!("https://{account_id}.r2.cloudflarestorage.com")
            }
            #[cfg(not(test))]
            {
                format!("https://{account_id}.r2.cloudflarestorage.com")
            }
        };
        let endpoint = endpoint.trim_end_matches('/').to_owned();
        let parsed_endpoint = url::Url::parse(&endpoint)
            .map_err(|_| ApiError::backup_r2_message("r2_config_incomplete", "R2 配置不完整"))?;
        if !matches!(parsed_endpoint.scheme(), "http" | "https")
            || parsed_endpoint.host_str().is_none()
            || parsed_endpoint.username() != ""
            || parsed_endpoint.password().is_some()
            || parsed_endpoint.query().is_some()
            || parsed_endpoint.fragment().is_some()
        {
            return Err(ApiError::backup_r2_message(
                "r2_config_incomplete",
                "R2 配置不完整",
            ));
        }
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(R2_REQUEST_TIMEOUT)
            .build()
            .map_err(|_| ApiError::backup_r2_message("r2_connection_failed", "连接 R2 失败"))?;
        Ok(Self {
            client,
            endpoint,
            access_key_id,
            secret_access_key,
            bucket,
            prefix,
        })
    }

    fn object_url(&self, key: &str) -> Result<url::Url, ApiError> {
        let mut url = url::Url::parse(&self.endpoint).map_err(|_| ApiError::unavailable())?;
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| ApiError::unavailable())?;
            segments.push(&self.bucket);
            for segment in key.split('/') {
                if !segment.is_empty() {
                    segments.push(segment);
                }
            }
        }
        Ok(url)
    }

    fn signed_request(
        &self,
        method: &str,
        key: &str,
        query: &[(String, String)],
        body: &[u8],
        extra_headers: &[(String, String)],
    ) -> Result<reqwest::RequestBuilder, ApiError> {
        let method = Method::from_bytes(method.as_bytes()).map_err(|_| ApiError::unavailable())?;
        let mut url = self.object_url(key)?;
        let canonical_query = query
            .iter()
            .map(|(name, value)| (aws_encode(name), aws_encode(value)))
            .collect::<Vec<_>>();
        let canonical_query = {
            let mut pairs = canonical_query;
            pairs.sort();
            pairs
                .into_iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect::<Vec<_>>()
                .join("&")
        };
        if !canonical_query.is_empty() {
            url.set_query(Some(&canonical_query));
        }
        let (amz_date, date_stamp) = r2_timestamp();
        let payload_hash = hex_sha256(body);
        let host = url.host_str().ok_or_else(ApiError::unavailable)?.to_owned();
        let host = match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host,
        };
        let mut headers = BTreeMap::<String, String>::new();
        headers.insert("host".to_owned(), host);
        headers.insert("x-amz-content-sha256".to_owned(), payload_hash.clone());
        headers.insert("x-amz-date".to_owned(), amz_date.clone());
        for (name, value) in extra_headers {
            headers.insert(name.to_ascii_lowercase(), collapse_header_value(value));
        }
        let canonical_headers = headers
            .iter()
            .map(|(name, value)| format!("{name}:{value}\n"))
            .collect::<String>();
        let signed_headers = headers.keys().cloned().collect::<Vec<_>>().join(";");
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method.as_str(),
            url.path(),
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash,
        );
        let credential_scope = format!("{date_stamp}/auto/s3/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
            hex_sha256(canonical_request.as_bytes())
        );
        let date_key = hmac_sha256(
            format!("AWS4{}", self.secret_access_key).as_bytes(),
            date_stamp.as_bytes(),
        );
        let region_key = hmac_sha256(&date_key, b"auto");
        let service_key = hmac_sha256(&region_key, b"s3");
        let signing_key = hmac_sha256(&service_key, b"aws4_request");
        let signature = hex_bytes(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key_id
        );
        let mut request = self.client.request(method, url.to_string());
        for (name, value) in headers {
            request = request.header(name, value);
        }
        request = request.header("authorization", authorization);
        for (name, value) in extra_headers {
            request = request.header(name, value);
        }
        Ok(request.body(body.to_owned()))
    }

    async fn response_body(
        response: reqwest::Response,
        limit: u64,
        code: &'static str,
        size_invalid_message: &'static str,
        too_large_message: &'static str,
        read_message: &'static str,
    ) -> Result<Vec<u8>, ApiError> {
        if let Some(raw) = response.headers().get("content-length") {
            let value = raw
                .to_str()
                .map_err(|_| ApiError::backup_r2_message(code, size_invalid_message))?
                .trim();
            if value.is_empty()
                || value.len() > 19
                || !value.is_ascii()
                || !value.chars().all(|character| character.is_ascii_digit())
            {
                return Err(ApiError::backup_r2_message(code, size_invalid_message));
            }
            let length = value
                .parse::<u64>()
                .map_err(|_| ApiError::backup_r2_message(code, size_invalid_message))?;
            if length > limit {
                return Err(ApiError::backup_r2_message(code, too_large_message));
            }
        }
        let mut total = 0u64;
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| ApiError::backup_r2_message(code, read_message))?;
            total = total
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| ApiError::backup_r2_message(code, too_large_message))?;
            if total > limit {
                return Err(ApiError::backup_r2_message(code, too_large_message));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    async fn test_connection(&self) -> Result<Value, ApiError> {
        let request = self.signed_request(
            "GET",
            "",
            &[
                ("list-type".to_owned(), "2".to_owned()),
                ("max-keys".to_owned(), "1".to_owned()),
            ],
            &[],
            &[],
        )?;
        let response = request
            .send()
            .await
            .map_err(|_| ApiError::backup_r2_message("r2_connection_failed", "连接 R2 失败"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(ApiError::backup_r2_status(
                "r2_connection_failed",
                "连接 R2 失败",
                status.as_u16(),
            ));
        }
        drop(response);
        Ok(json!({"ok": true, "status": status.as_u16()}))
    }

    async fn upload_bytes(
        &self,
        key: &str,
        payload: &[u8],
        metadata: &[(String, String)],
    ) -> Result<(), ApiError> {
        let mut extra_headers = vec![(
            "content-type".to_owned(),
            "application/octet-stream".to_owned(),
        )];
        extra_headers.extend(
            metadata
                .iter()
                .map(|(name, value)| (format!("x-amz-meta-{name}"), value.clone())),
        );
        let request = self.signed_request("PUT", key, &[], payload, &extra_headers)?;
        let response = request
            .send()
            .await
            .map_err(|_| ApiError::backup_r2_message("r2_upload_failed", "上传备份失败"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(ApiError::backup_r2_status(
                "r2_upload_failed",
                "上传备份失败",
                status.as_u16(),
            ));
        }
        drop(response);
        Ok(())
    }

    async fn delete_object(&self, key: &str) -> Result<(), ApiError> {
        let request = self.signed_request("DELETE", key, &[], &[], &[])?;
        let response = request
            .send()
            .await
            .map_err(|_| ApiError::backup_r2_message("r2_delete_failed", "删除备份失败"))?;
        let status = response.status();
        if !status.is_success() && status != reqwest::StatusCode::NOT_FOUND {
            return Err(ApiError::backup_r2_status(
                "r2_delete_failed",
                "删除备份失败",
                status.as_u16(),
            ));
        }
        drop(response);
        Ok(())
    }

    async fn download_bytes(&self, key: &str) -> Result<Vec<u8>, ApiError> {
        let request = self.signed_request("GET", key, &[], &[], &[])?;
        let response = request
            .send()
            .await
            .map_err(|_| ApiError::backup_r2_message("r2_read_failed", "读取备份失败"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(ApiError::backup_r2_status(
                "r2_read_failed",
                "读取备份失败",
                status.as_u16(),
            ));
        }
        Self::response_body(
            response,
            MAX_R2_DOWNLOAD_BYTES,
            "r2_read_payload_invalid",
            "备份响应大小无效",
            "备份响应过大",
            "读取备份响应失败",
        )
        .await
    }

    async fn list_objects(&self) -> Result<Vec<R2Object>, ApiError> {
        let mut result = Vec::new();
        let mut continuation = None::<String>;
        for _ in 0..MAX_R2_LIST_PAGES {
            let mut query = vec![
                ("list-type".to_owned(), "2".to_owned()),
                ("max-keys".to_owned(), "1000".to_owned()),
                ("prefix".to_owned(), format!("{}/", self.prefix)),
            ];
            if let Some(token) = continuation.as_ref() {
                query.push(("continuation-token".to_owned(), token.clone()));
            }
            let request = self.signed_request("GET", "", &query, &[], &[])?;
            let response = request
                .send()
                .await
                .map_err(|_| ApiError::backup_r2_message("r2_list_failed", "获取备份列表失败"))?;
            let status = response.status();
            if !status.is_success() {
                return Err(ApiError::backup_r2_status(
                    "r2_list_failed",
                    "获取备份列表失败",
                    status.as_u16(),
                ));
            }
            let body = Self::response_body(
                response,
                MAX_R2_LIST_RESPONSE_BYTES,
                "r2_list_payload_invalid",
                "备份响应大小无效",
                "备份响应过大",
                "读取备份响应失败",
            )
            .await?;
            let xml = String::from_utf8(body).map_err(|_| {
                ApiError::backup_r2_message("r2_list_payload_invalid", "备份列表格式无效")
            })?;
            let (page, is_truncated, next_token) = parse_r2_list_xml(&xml)?;
            if result.len().saturating_add(page.len()) > MAX_R2_LIST_OBJECTS {
                return Err(ApiError::backup_r2_message(
                    "r2_list_limit_exceeded",
                    "备份列表过大",
                ));
            }
            result.extend(page);
            if !is_truncated || next_token.is_none() {
                break;
            }
            continuation = next_token;
        }
        if continuation.is_some() {
            return Err(ApiError::backup_r2_message(
                "r2_list_limit_exceeded",
                "备份列表过大",
            ));
        }
        result.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
        Ok(result)
    }
}

fn aws_encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push_str(&format!("{byte:02X}"));
        }
    }
    encoded
}

fn collapse_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_sha256(value: &[u8]) -> String {
    hex_bytes(&Sha256::digest(value))
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut normalized_key = [0u8; 64];
    if key.len() > 64 {
        normalized_key[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        normalized_key[..key.len()].copy_from_slice(key);
    }
    let mut inner = Vec::with_capacity(64 + value.len());
    let mut outer = Vec::with_capacity(64 + 32);
    for byte in normalized_key {
        inner.push(byte ^ 0x36);
        outer.push(byte ^ 0x5c);
    }
    inner.extend_from_slice(value);
    let inner_hash = Sha256::digest(inner);
    outer.extend_from_slice(&inner_hash);
    Sha256::digest(outer).to_vec()
}

fn r2_timestamp() -> (String, String) {
    let now = unix_seconds(SystemTime::now());
    let date = date_from_unix(now);
    let day_seconds = now.rem_euclid(86_400);
    let hour = day_seconds / 3_600;
    let minute = (day_seconds % 3_600) / 60;
    let second = day_seconds % 60;
    let amz_date = format!("{}T{hour:02}{minute:02}{second:02}Z", date.replace('-', ""));
    (amz_date, date.replace('-', ""))
}

fn xml_tag<'a>(block: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = block.find(open.as_str())? + open.len();
    let end = block[start..].find(close.as_str())? + start;
    Some(&block[start..end])
}

fn parse_r2_list_xml(xml: &str) -> Result<(Vec<R2Object>, bool, Option<String>), ApiError> {
    let mut items = Vec::new();
    for block in xml.split("<Contents>").skip(1) {
        let Some(block) = block.split("</Contents>").next() else {
            return Err(ApiError::backup_r2_message(
                "r2_list_payload_invalid",
                "备份列表格式无效",
            ));
        };
        let Some(key) = xml_tag(block, "Key").map(str::trim) else {
            continue;
        };
        if key.is_empty() {
            continue;
        }
        let size = match xml_tag(block, "Size").map(str::trim) {
            None | Some("") => 0,
            Some(value)
                if value.len() <= 19
                    && value.is_ascii()
                    && value.chars().all(|c| c.is_ascii_digit()) =>
            {
                value
                    .parse::<u64>()
                    .ok()
                    .filter(|value| *value <= MAX_R2_DOWNLOAD_BYTES)
                    .ok_or_else(|| {
                        ApiError::backup_r2_message("r2_list_payload_invalid", "备份列表格式无效")
                    })?
            }
            Some(_) => {
                return Err(ApiError::backup_r2_message(
                    "r2_list_payload_invalid",
                    "备份列表格式无效",
                ));
            }
        };
        let updated_at = xml_tag(block, "LastModified")
            .unwrap_or_default()
            .trim()
            .to_owned();
        items.push(R2Object {
            key: key.to_owned(),
            size,
            updated_at,
        });
    }
    let is_truncated = xml_tag(xml, "IsTruncated") == Some("true");
    let next_token = xml_tag(xml, "NextContinuationToken")
        .filter(|value| !value.is_empty() && value.len() <= 4096)
        .map(ToOwned::to_owned);
    Ok((items, is_truncated, next_token))
}

async fn openssl_backup_crypt(
    payload: Vec<u8>,
    passphrase: String,
    decrypt: bool,
) -> Result<Vec<u8>, ApiError> {
    openssl_backup_crypt_with_limit(payload, passphrase, decrypt, MAX_BACKUP_BYTES).await
}

async fn openssl_backup_crypt_with_limit(
    payload: Vec<u8>,
    passphrase: String,
    decrypt: bool,
    max_bytes: u64,
) -> Result<Vec<u8>, ApiError> {
    preflight_backup_crypt_budget(payload.len(), decrypt, max_bytes)?;
    if passphrase.is_empty() {
        return Err(ApiError::invalid_request());
    }
    let permit = tokio::time::timeout(
        BACKUP_CRYPT_ADMISSION_TIMEOUT,
        BACKUP_CRYPT_SEMAPHORE.clone().acquire_owned(),
    )
    .await
    .map_err(|_| ApiError::unavailable())?
    .map_err(|_| ApiError::unavailable())?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        openssl_backup_crypt_sync(payload, passphrase, decrypt, max_bytes)
    })
    .await
    .map_err(|_| ApiError::unavailable())?
}

fn preflight_backup_crypt_budget(
    input_len: usize,
    decrypt: bool,
    max_bytes: u64,
) -> Result<(), ApiError> {
    if input_len as u64 > max_bytes {
        return Err(ApiError::unavailable());
    }
    if decrypt {
        return Ok(());
    }
    let padding = BACKUP_CRYPT_BLOCK_BYTES - (input_len % BACKUP_CRYPT_BLOCK_BYTES);
    let ciphertext_len = input_len
        .checked_add(padding)
        .ok_or_else(ApiError::unavailable)?;
    let output_len = BACKUP_CRYPT_HEADER_BYTES
        .checked_add(ciphertext_len)
        .ok_or_else(ApiError::unavailable)?;
    if output_len as u64 > max_bytes {
        return Err(ApiError::unavailable());
    }
    Ok(())
}

fn openssl_backup_crypt_sync(
    mut payload: Vec<u8>,
    passphrase: String,
    decrypt: bool,
    max_bytes: u64,
) -> Result<Vec<u8>, ApiError> {
    const PBKDF2_ITERATIONS: usize = 10_000;
    #[cfg(test)]
    let _test_guard = backup_crypt_test_enter(&passphrase);
    let passphrase = passphrase.into_bytes();
    if decrypt {
        if payload.len() < BACKUP_CRYPT_HEADER_BYTES || &payload[..8] != b"Salted__" {
            return Err(ApiError::unavailable());
        }
        let salt = payload[8..BACKUP_CRYPT_HEADER_BYTES].to_owned();
        let ciphertext_len = payload.len() - BACKUP_CRYPT_HEADER_BYTES;
        if ciphertext_len == 0 || !ciphertext_len.is_multiple_of(BACKUP_CRYPT_BLOCK_BYTES) {
            return Err(ApiError::unavailable());
        }
        payload.copy_within(BACKUP_CRYPT_HEADER_BYTES.., 0);
        payload.truncate(ciphertext_len);
        let (key, iv) = pbkdf2_backup_key(&passphrase, &salt, PBKDF2_ITERATIONS);
        let cipher = Aes256::new(GenericArray::from_slice(&key));
        let mut previous = iv;
        for offset in (0..ciphertext_len).step_by(BACKUP_CRYPT_BLOCK_BYTES) {
            let ciphertext =
                GenericArray::clone_from_slice(&payload[offset..offset + BACKUP_CRYPT_BLOCK_BYTES]);
            let mut block = ciphertext;
            cipher.decrypt_block(&mut block);
            for (byte, previous_byte) in block.iter_mut().zip(previous) {
                *byte ^= previous_byte;
            }
            payload[offset..offset + BACKUP_CRYPT_BLOCK_BYTES].copy_from_slice(&block);
            previous.copy_from_slice(&ciphertext);
        }
        let padding = *payload.last().ok_or_else(ApiError::unavailable)? as usize;
        if !(1..=BACKUP_CRYPT_BLOCK_BYTES).contains(&padding)
            || payload.len() < padding
            || !payload[payload.len() - padding..]
                .iter()
                .all(|byte| usize::from(*byte) == padding)
        {
            return Err(ApiError::unavailable());
        }
        payload.truncate(payload.len() - padding);
        if payload.len() as u64 > max_bytes {
            return Err(ApiError::unavailable());
        }
        return Ok(payload);
    }

    let input_len = payload.len();
    let padding = BACKUP_CRYPT_BLOCK_BYTES - (input_len % BACKUP_CRYPT_BLOCK_BYTES);
    let ciphertext_len = input_len
        .checked_add(padding)
        .ok_or_else(ApiError::unavailable)?;
    let output_len = BACKUP_CRYPT_HEADER_BYTES
        .checked_add(ciphertext_len)
        .ok_or_else(ApiError::unavailable)?;
    if output_len as u64 > max_bytes {
        return Err(ApiError::unavailable());
    }
    let mut salt = [0u8; 8];
    getrandom::getrandom(&mut salt).map_err(|_| ApiError::unavailable())?;
    let (key, iv) = pbkdf2_backup_key(&passphrase, &salt, PBKDF2_ITERATIONS);
    payload.resize(output_len, padding as u8);
    payload.copy_within(0..ciphertext_len, BACKUP_CRYPT_HEADER_BYTES);
    payload[..8].copy_from_slice(b"Salted__");
    payload[8..BACKUP_CRYPT_HEADER_BYTES].copy_from_slice(&salt);
    let cipher = Aes256::new(GenericArray::from_slice(&key));
    let mut previous = iv;
    for offset in (BACKUP_CRYPT_HEADER_BYTES..output_len).step_by(BACKUP_CRYPT_BLOCK_BYTES) {
        let mut block =
            GenericArray::clone_from_slice(&payload[offset..offset + BACKUP_CRYPT_BLOCK_BYTES]);
        for (byte, previous_byte) in block.iter_mut().zip(previous) {
            *byte ^= previous_byte;
        }
        cipher.encrypt_block(&mut block);
        payload[offset..offset + BACKUP_CRYPT_BLOCK_BYTES].copy_from_slice(&block);
        previous.copy_from_slice(&block);
    }
    Ok(payload)
}

#[cfg(test)]
struct BackupCryptTestActiveGuard {
    active: Arc<AtomicUsize>,
}

#[cfg(test)]
impl Drop for BackupCryptTestActiveGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
fn backup_crypt_test_enter(passphrase: &str) -> Option<BackupCryptTestActiveGuard> {
    if passphrase != "bounded-admission-test" {
        return None;
    }
    let hook = BACKUP_CRYPT_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()?;
    let active = hook.active.fetch_add(1, Ordering::SeqCst) + 1;
    hook.max_active.fetch_max(active, Ordering::SeqCst);
    hook.entered.notify_waiters();
    let (release_lock, release_signal) = &*hook.release;
    let mut released = release_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    while !*released {
        released = release_signal
            .wait(released)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
    Some(BackupCryptTestActiveGuard {
        active: hook.active,
    })
}

fn pbkdf2_backup_key(passphrase: &[u8], salt: &[u8], iterations: usize) -> ([u8; 32], [u8; 16]) {
    let mut derived = [0u8; 48];
    for block_index in 0..2 {
        let mut salt_block = Vec::with_capacity(salt.len() + 4);
        salt_block.extend_from_slice(salt);
        salt_block.extend_from_slice(&((block_index + 1) as u32).to_be_bytes());
        let mut u = hmac_sha256(passphrase, &salt_block);
        let mut t = u.clone();
        for _ in 1..iterations {
            u = hmac_sha256(passphrase, &u);
            for (left, right) in t.iter_mut().zip(&u) {
                *left ^= *right;
            }
        }
        let start = block_index * 32;
        let end = (start + 32).min(derived.len());
        derived[start..end].copy_from_slice(&t[..end - start]);
    }
    let mut key = [0u8; 32];
    let mut iv = [0u8; 16];
    key.copy_from_slice(&derived[..32]);
    iv.copy_from_slice(&derived[32..]);
    (key, iv)
}

async fn decrypt_backup_if_needed(
    state: &AppState,
    key: &str,
    payload: Vec<u8>,
    missing_code: &'static str,
    missing_message: &'static str,
) -> Result<Vec<u8>, ApiError> {
    if !key.ends_with(".enc") {
        return Ok(payload);
    }
    let passphrase = backup_raw_text(&backup_raw_settings(state), "passphrase");
    if passphrase.is_empty() {
        return Err(ApiError::backup_r2_message(missing_code, missing_message));
    }
    openssl_backup_crypt(payload, passphrase, true)
        .await
        .map_err(|_| {
            ApiError::backup_r2_message("backup_decrypt_failed", "解密备份失败：openssl 执行失败")
        })
}

#[cfg(test)]
pub(super) async fn backup_crypt_for_test(
    payload: Vec<u8>,
    passphrase: String,
    decrypt: bool,
) -> Result<Vec<u8>, ApiError> {
    openssl_backup_crypt(payload, passphrase, decrypt).await
}

#[cfg(test)]
pub(super) async fn backup_crypt_for_test_with_limit(
    payload: Vec<u8>,
    passphrase: String,
    decrypt: bool,
    max_bytes: u64,
) -> Result<Vec<u8>, ApiError> {
    openssl_backup_crypt_with_limit(payload, passphrase, decrypt, max_bytes).await
}

fn public_backup_state_text(value: Option<&Value>, max_length: usize) -> Value {
    let Some(value) = value.and_then(Value::as_str) else {
        return Value::Null;
    };
    let value = value.trim();
    if value.is_empty() {
        Value::Null
    } else {
        Value::String(value.chars().take(max_length).collect())
    }
}

fn public_backup_error(raw: &Map<String, Value>) -> Value {
    const FALLBACK: &str = "备份执行失败，请稍后重试";
    let raw_error = raw
        .get("last_error")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty());
    let error_code = raw
        .get("last_error_code")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    let error_status = match raw.get("last_error_status") {
        Some(Value::Number(value)) => value
            .as_u64()
            .filter(|value| (400..=599).contains(value))
            .map(|value| value as u16),
        _ => None,
    };
    let has_invalid_status = raw.contains_key("last_error_status") && error_status.is_none();
    if has_invalid_status {
        return Value::String(FALLBACK.to_owned());
    }
    let message = match error_code {
        "backup_failed" => Some(FALLBACK.to_owned()),
        "r2_config_incomplete" => Some("R2 配置不完整".to_owned()),
        "backup_encrypt_unavailable" => Some("当前环境缺少 openssl，无法执行加密备份".to_owned()),
        "backup_encrypt_failed" => Some("加密备份失败：openssl 执行失败".to_owned()),
        "backup_decrypt_unavailable" => Some("当前环境缺少 openssl，无法解密备份内容".to_owned()),
        "backup_decrypt_failed" => Some("解密备份失败：openssl 执行失败".to_owned()),
        "backup_key_required" => Some("备份对象 key 不能为空".to_owned()),
        "backup_download_passphrase_missing" => {
            Some("当前未配置加密口令，无法下载并解密已加密备份".to_owned())
        }
        "backup_detail_passphrase_missing" => {
            Some("当前未配置加密口令，无法查看已加密备份".to_owned())
        }
        "backup_busy" => Some("当前已有备份任务正在执行".to_owned()),
        "backup_encrypt_passphrase_missing" => Some("已启用备份加密，但未设置加密口令".to_owned()),
        "backup_archive_invalid" => Some("解析备份压缩包失败，备份可能已损坏".to_owned()),
        "backup_state_invalid" => Some("上一次备份状态无效，已停止重试".to_owned()),
        "r2_connection_failed" => error_status.map(|status| format!("连接 R2 失败：HTTP {status}")),
        "r2_upload_failed" => error_status.map(|status| format!("上传备份失败：HTTP {status}")),
        "r2_delete_failed" => error_status.map(|status| format!("删除备份失败：HTTP {status}")),
        "r2_read_failed" => error_status.map(|status| format!("读取备份失败：HTTP {status}")),
        "r2_list_failed" => error_status.map(|status| format!("获取备份列表失败：HTTP {status}")),
        _ => None,
    };
    if message.is_some() {
        return message.map(Value::String).unwrap_or(Value::Null);
    }
    if raw_error
        || !error_code.is_empty()
        || raw.contains_key("last_error_status")
        || raw
            .get("_last_error_public")
            .and_then(Value::as_bool)
            .is_some_and(|value| value)
    {
        Value::String(FALLBACK.to_owned())
    } else {
        Value::Null
    }
}

fn backup_state(state: &AppState) -> Result<Value, ApiError> {
    let raw = backup_state_map(state)?;
    let last_status = match raw.get("last_status").and_then(Value::as_str) {
        Some(value) if matches!(value, "idle" | "running" | "success" | "error") => value,
        _ => "idle",
    };
    Ok(json!({
        "running": state.backup_running.load(Ordering::Acquire) || last_status == "running",
        "last_started_at": public_backup_state_text(raw.get("last_started_at"), 128),
        "last_finished_at": public_backup_state_text(raw.get("last_finished_at"), 128),
        "last_status": last_status,
        "last_error": public_backup_error(&raw),
        "last_object_key": public_backup_state_text(raw.get("last_object_key"), 2048),
    }))
}

fn backup_key_file(state: &AppState, key: &str) -> Result<PathBuf, ApiError> {
    let key = key.trim();
    let Some(name) = key.strip_prefix("backups/") else {
        return Err(ApiError::backup_r2_message(
            "backup_key_invalid",
            "备份对象 key 无效",
        ));
    };
    if name.is_empty()
        || name.contains('/')
        || name.contains('\\')
        || name.contains("..")
        || !name.starts_with("backup-")
        || !(name.ends_with(".tar.gz") || name.ends_with(".tar.gz.enc"))
    {
        return Err(ApiError::backup_r2_message(
            "backup_key_invalid",
            "备份对象 key 无效",
        ));
    }
    Ok(backup_dir(state).join(name))
}

fn backup_key_prefix(state: &AppState) -> String {
    let prefix = backup_raw_text(&backup_raw_settings(state), "prefix");
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        "backups".to_owned()
    } else {
        prefix.to_owned()
    }
}

fn validate_remote_backup_key(state: &AppState, key: &str) -> Result<String, ApiError> {
    let key = key.trim();
    let prefix = backup_key_prefix(state);
    let Some(name) = key.strip_prefix(&format!("{prefix}/")) else {
        return Err(ApiError::backup_r2_message(
            "backup_key_invalid",
            "备份对象 key 无效",
        ));
    };
    if name.is_empty()
        || name.contains('/')
        || name.contains('\\')
        || name.contains("..")
        || !name.is_ascii()
        || !name.starts_with("backup-")
        || !(name.ends_with(".tar.gz") || name.ends_with(".tar.gz.enc"))
    {
        return Err(ApiError::backup_r2_message(
            "backup_key_invalid",
            "备份对象 key 无效",
        ));
    }
    Ok(key.to_owned())
}

fn add_tar_bytes(
    builder: &mut Builder<GzEncoder<Vec<u8>>>,
    name: &str,
    payload: &[u8],
) -> Result<(), ApiError> {
    let mut header = Header::new_gnu();
    header.set_size(payload.len() as u64);
    header.set_mode(0o600);
    header.set_mtime(unix_seconds(SystemTime::now()) as u64);
    header.set_cksum();
    builder
        .append_data(&mut header, name, payload)
        .map_err(|_| ApiError::unavailable())
}

fn add_tar_file(
    builder: &mut Builder<GzEncoder<Vec<u8>>>,
    root: &Path,
    path: &Path,
    name: &str,
) -> Result<(), ApiError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| ApiError::unavailable())?;
    let _ = relative;
    let payload = read_bounded(path, MAX_BACKUP_MEMBER_BYTES)?;
    add_tar_bytes(builder, name, &payload)
}

async fn build_backup(state: &AppState, _key: &str, trigger: &str) -> Result<Vec<u8>, ApiError> {
    let settings = backup_raw_settings(state);
    let include = object_or_empty(
        settings
            .get("include")
            .cloned()
            .unwrap_or_else(|| json!({})),
    );
    let mut metadata = json!({
        "version": 2,
        "created_at": iso_timestamp(SystemTime::now()),
        "trigger": trigger,
        "app_version": state.config.version,
        "storage_backend": state
            .storage_backend
            .as_deref()
            .map(super::storage::StorageBackend::info)
            .unwrap_or_else(|| json!({"type": "json", "description": "本地 JSON 存储"})),
    });
    let account_snapshot = if bool_or(include.get("accounts_snapshot"), true) {
        let snapshot = if let Some(backend) = state.storage_backend.as_deref() {
            backend
                .load_accounts()
                .await
                .map_err(|_| ApiError::unavailable())?
        } else {
            let records = state.account_store.raw_records();
            let health = state.account_store.health_stats();
            super::storage::StorageSnapshot {
                revision: [0; 32],
                records,
                cumulative_total: Some(health.cumulative_total),
            }
        };
        if let Some(cumulative_total) = snapshot.cumulative_total {
            metadata["snapshot_manifest"] = json!({
                "version": 1,
                "accounts": {"cumulative_total": cumulative_total},
            });
        }
        Some(snapshot.records)
    } else {
        None
    };
    let auth_snapshot = if bool_or(include.get("auth_keys_snapshot"), true) {
        let snapshot = if let Some(backend) = state.storage_backend.as_deref() {
            backend
                .load_auth_keys()
                .await
                .map_err(|_| ApiError::unavailable())?
        } else {
            super::storage::StorageSnapshot {
                revision: [0; 32],
                records: state.auth_store.raw_records(),
                cumulative_total: None,
            }
        };
        Some(snapshot.records)
    } else {
        None
    };
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut builder = Builder::new(encoder);
    add_tar_bytes(
        &mut builder,
        "backup-metadata.json",
        &serde_json::to_vec(&metadata).map_err(|_| ApiError::unavailable())?,
    )?;
    if bool_or(include.get("config"), true) {
        let config = redact_config(read_config(state));
        add_tar_bytes(
            &mut builder,
            "config.json",
            &serde_json::to_vec(&config).map_err(|_| ApiError::unavailable())?,
        )?;
    }
    let data_root = state.data_dir.as_ref();
    let optional_files = [
        ("logs", "logs.jsonl", "data/logs.jsonl"),
        ("image_tasks", "image_tasks.json", "data/image_tasks.json"),
        ("image_tasks", "image_index.json", "data/image_index.json"),
        ("cpa", "cpa_config.json", "data/cpa_config.json"),
        ("sub2api", "sub2api_config.json", "data/sub2api_config.json"),
        ("ccload", "ccload_config.json", "data/ccload_config.json"),
        ("images", "image_tags.json", "data/image_tags.json"),
    ];
    for (flag, filename, archive_name) in optional_files {
        if bool_or(include.get(flag), flag != "images" && flag != "ccload") {
            let path = data_root.join(filename);
            if path.is_file() {
                add_tar_file(&mut builder, data_root, &path, archive_name)?;
            }
        }
    }
    if bool_or(include.get("images"), false) {
        for (relative, path) in image_files(state) {
            add_tar_file(
                &mut builder,
                &image_root(state),
                &path,
                &format!("data/images/{relative}"),
            )?;
        }
    }
    if let Some(records) = account_snapshot {
        add_tar_bytes(
            &mut builder,
            "snapshots/accounts.json",
            &serde_json::to_vec(&records).map_err(|_| ApiError::unavailable())?,
        )?;
    }
    if let Some(records) = auth_snapshot {
        add_tar_bytes(
            &mut builder,
            "snapshots/auth_keys.json",
            &serde_json::to_vec(&records).map_err(|_| ApiError::unavailable())?,
        )?;
    }
    let encoder = builder.into_inner().map_err(|_| ApiError::unavailable())?;
    encoder.finish().map_err(|_| ApiError::unavailable())
}

fn backup_items(state: &AppState) -> Result<Vec<Value>, ApiError> {
    let root = backup_dir(state);
    if !root.is_dir() {
        return Ok(Vec::new());
    }
    let mut items = Vec::new();
    for entry in fs::read_dir(root)
        .map_err(|_| ApiError::unavailable())?
        .flatten()
    {
        let metadata = entry.metadata().map_err(|_| ApiError::unavailable())?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !(name.ends_with(".tar.gz") || name.ends_with(".tar.gz.enc")) {
            continue;
        }
        let encrypted = name.ends_with(".enc");
        items.push(json!({
            "key": format!("backups/{name}"),
            "name": name,
            "size": metadata.len(),
            "updated_at": metadata.modified().ok().map(iso_timestamp),
            "encrypted": encrypted,
        }));
    }
    items.sort_by(|left, right| right["name"].as_str().cmp(&left["name"].as_str()));
    Ok(items)
}

async fn remote_backup_items(state: &AppState, client: &R2Client) -> Result<Vec<Value>, ApiError> {
    let mut items = Vec::new();
    for object in client.list_objects().await? {
        let Ok(key) = validate_remote_backup_key(state, &object.key) else {
            continue;
        };
        let name = key.rsplit('/').next().unwrap_or_default().to_owned();
        items.push(json!({
            "key": key,
            "name": name,
            "size": object.size,
            "updated_at": public_backup_timestamp(&object.updated_at),
            "encrypted": name.ends_with(".enc"),
        }));
    }
    Ok(items)
}

fn public_backup_timestamp(value: &str) -> Value {
    if value.len() <= 64
        && time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
            .is_ok()
    {
        Value::String(value.to_owned())
    } else {
        Value::Null
    }
}

async fn rotate_remote_backups(
    state: &AppState,
    client: &R2Client,
    current_key: &str,
    keep: usize,
) -> Result<(), ApiError> {
    if keep == 0 {
        return Ok(());
    }
    let items = client.list_objects().await?;
    let protected = {
        let current = backup_state_map(state)?;
        [
            Some(current_key.to_owned()),
            current
                .get("pending_object_key")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            current
                .get("last_object_key")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        ]
        .into_iter()
        .flatten()
        .collect::<HashSet<_>>()
    };
    let eligible = items
        .into_iter()
        .filter(|item| validate_remote_backup_key(state, &item.key).is_ok())
        .filter(|item| !protected.contains(&item.key))
        .collect::<Vec<_>>();
    let delete_count = eligible
        .len()
        .saturating_sub(keep.saturating_sub(protected.len()));
    for item in eligible
        .into_iter()
        .skip(keep.saturating_sub(protected.len()))
        .take(delete_count)
    {
        client.delete_object(&item.key).await?;
    }
    Ok(())
}

pub(super) async fn test_backup(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    if backup_is_remote(&state) {
        let client = R2Client::from_state(&state)?;
        return Ok(Json(json!({"result": client.test_connection().await?})));
    }
    Ok(Json(
        json!({"result": {"ok": true, "status": 200, "backend": "local", "error": null}}),
    ))
}

pub(super) async fn list_backups(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let items = if backup_is_remote(&state) {
        if !backup_is_configured(&state) {
            Vec::new()
        } else {
            remote_backup_items(&state, &R2Client::from_state(&state)?).await?
        }
    } else {
        backup_items(&state)?
    };
    Ok(Json(json!({
        "items": items,
        "state": backup_state(&state)?,
        "settings": backup_settings(&state),
    })))
}

struct BackupRunningGuard(Arc<AtomicBool>);

impl Drop for BackupRunningGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

async fn run_backup_impl(state: AppState, trigger: &str) -> Result<Value, ApiError> {
    state
        .backup_running
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| ApiError::backup_busy())?;
    let _running_guard = BackupRunningGuard(state.backup_running.clone());
    let remote = backup_is_remote(&state);
    let state_path = backup_state_path(&state);
    let owner_gate = super::backup_owner_gate(&state_path);
    let _owner_guard = owner_gate
        .try_lock_owned()
        .map_err(|_| ApiError::backup_busy())?;
    let Some(_state_lock) = super::try_acquire_path_write_lock(&state_path).await? else {
        return Err(ApiError::backup_busy());
    };

    let current = backup_state_map(&state)?;
    if !remote {
        fs::create_dir_all(backup_dir(&state)).map_err(|_| ApiError::unavailable())?;
    }
    let target_fingerprint = backup_target_fingerprint(&state);
    let encryption_enabled = backup_encryption_enabled(&state);
    let pending_raw = current
        .get("pending_object_key")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let object_name = backup_object_name(encryption_enabled);
    let key = format!(
        "{}/{}",
        if remote {
            backup_key_prefix(&state)
        } else {
            "backups".to_owned()
        },
        object_name
    );
    let (key, started) = if let Some(pending_key) = pending_raw.as_deref() {
        let valid_key = if remote {
            validate_remote_backup_key(&state, pending_key).is_ok()
        } else {
            backup_key_file(&state, pending_key).is_ok()
        };
        let pending_is_encrypted = pending_key.ends_with(".tar.gz.enc");
        let consistent = valid_key
            && pending_is_encrypted == encryption_enabled
            && current
                .get("pending_target_fingerprint")
                .and_then(Value::as_str)
                .is_some_and(|value| value == target_fingerprint);
        if !consistent {
            return Err(ApiError::backup_state_invalid());
        }
        (
            pending_key.to_owned(),
            current
                .get("last_started_at")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| iso_timestamp(SystemTime::now())),
        )
    } else {
        (key, iso_timestamp(SystemTime::now()))
    };
    let running_state = backup_running_state(&current, &started, &key, &target_fingerprint);
    write_json_unlocked(&state_path, &running_state)?;
    #[cfg(test)]
    backup_test_after_running(&state_path, &key).await;
    let result = build_backup(&state, &key, trigger).await;
    match result {
        Ok(payload_raw) => {
            let payload = match if encryption_enabled {
                let passphrase = backup_raw_text(&backup_raw_settings(&state), "passphrase");
                if passphrase.is_empty() {
                    Err(ApiError::backup_r2_message(
                        "backup_encrypt_passphrase_missing",
                        "已启用备份加密，但未设置加密口令",
                    ))
                } else {
                    openssl_backup_crypt(payload_raw, passphrase, false)
                        .await
                        .map_err(|_| {
                            ApiError::backup_r2_message(
                                "backup_encrypt_failed",
                                "加密备份失败：openssl 执行失败",
                            )
                        })
                }
            } else {
                Ok(payload_raw)
            } {
                Ok(payload) => payload,
                Err(error) => {
                    let failure = backup_error_state_from_api(
                        &current,
                        &started,
                        &key,
                        &target_fingerprint,
                        &error,
                    );
                    let _ = write_json_unlocked(&state_path, &failure);
                    return Err(error);
                }
            };
            let commit = if remote {
                let metadata = vec![
                    ("created-at".to_owned(), iso_timestamp(SystemTime::now())),
                    (
                        "encrypted".to_owned(),
                        if encryption_enabled { "true" } else { "false" }.to_owned(),
                    ),
                    ("trigger".to_owned(), trigger.to_owned()),
                ];
                match R2Client::from_state(&state) {
                    Ok(client) => client.upload_bytes(&key, &payload, &metadata).await,
                    Err(error) => Err(error),
                }
            } else {
                let path = backup_key_file(&state, &key)?;
                write_atomic(&path, &payload)
            };
            if let Err(error) = commit {
                let failure = backup_error_state_from_api(
                    &current,
                    &started,
                    &key,
                    &target_fingerprint,
                    &error,
                );
                let _ = write_json_unlocked(&state_path, &failure);
                return Err(error);
            }
            if remote {
                let keep = backup_raw_settings(&state)
                    .get("rotation_keep")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                    .unwrap_or(10);
                let rotation = match R2Client::from_state(&state) {
                    Ok(client) => rotate_remote_backups(&state, &client, &key, keep).await,
                    Err(error) => Err(error),
                };
                if let Err(error) = rotation {
                    let failure = backup_error_state_from_api(
                        &current,
                        &started,
                        &key,
                        &target_fingerprint,
                        &error,
                    );
                    let _ = write_json_unlocked(&state_path, &failure);
                    return Err(error);
                }
            }
            #[cfg(test)]
            backup_test_after_archive_commit(&state_path).await;
            let mut success = running_state
                .as_object()
                .cloned()
                .expect("running backup state object");
            success.remove("running");
            success.insert("last_status".to_owned(), json!("success"));
            success.insert(
                "last_finished_at".to_owned(),
                json!(iso_timestamp(SystemTime::now())),
            );
            success.insert("last_error".to_owned(), Value::Null);
            success.remove("last_error_code");
            success.remove("last_error_status");
            success.insert("last_object_key".to_owned(), json!(key));
            success.insert("pending_object_key".to_owned(), Value::Null);
            success.insert("pending_target_fingerprint".to_owned(), Value::Null);
            if let Err(error) = write_json_unlocked(&state_path, &Value::Object(success)) {
                let failure = backup_error_state(
                    &current,
                    &started,
                    &key,
                    &target_fingerprint,
                    "backup_failed",
                );
                let _ = write_json_unlocked(&state_path, &failure);
                return Err(error);
            }
            Ok(
                json!({"result": {"key": key, "size": payload.len(), "encrypted": encryption_enabled}}),
            )
        }
        Err(error) => {
            let failure = backup_error_state(
                &current,
                &started,
                &key,
                &target_fingerprint,
                "backup_failed",
            );
            let _ = write_json_unlocked(&state_path, &failure);
            Err(error)
        }
    }
}

pub(super) async fn run_backup(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    Ok(Json(run_backup_impl(state, "manual").await?))
}

pub(super) async fn run_scheduled_backup_if_due(state: &AppState) {
    let settings = backup_raw_settings(state);
    let current = match backup_state_map(state) {
        Ok(current) => current,
        Err(_) => return,
    };
    if !backup_schedule_due(&Value::Object(settings), &current, SystemTime::now()) {
        return;
    }
    let _ = run_backup_impl(state.clone(), "schedule").await;
}

fn content_type(name: &str) -> &'static str {
    if name.ends_with(".json") {
        "application/json"
    } else if name.ends_with(".jsonl") {
        "application/x-ndjson"
    } else if name.ends_with(".gz") {
        "application/gzip"
    } else {
        "application/octet-stream"
    }
}

fn public_archive_member_name(name: &str) -> Option<&str> {
    if name.is_empty() || name.len() > 256 || !name.is_ascii() {
        return None;
    }
    if name
        .chars()
        .any(|character| !character.is_ascii_alphanumeric() && !"._/-".contains(character))
    {
        return None;
    }
    let parts = name.split('/').collect::<Vec<_>>();
    if parts
        .iter()
        .any(|part| part.is_empty() || matches!(*part, "." | ".."))
    {
        return None;
    }
    if name == "config.json"
        || matches!(name, "snapshots/accounts.json" | "snapshots/auth_keys.json")
        || (parts.len() >= 2 && parts.first() == Some(&"data"))
    {
        return Some(name);
    }
    None
}

fn read_backup_detail(state: &AppState, key: &str) -> Result<Value, ApiError> {
    let path = backup_key_file(state, key)?;
    let payload = read_bounded(&path, MAX_BACKUP_BYTES)?;
    read_backup_detail_payload(key, payload)
}

fn read_backup_detail_payload(key: &str, payload: Vec<u8>) -> Result<Value, ApiError> {
    let decoder = GzDecoder::new(Cursor::new(payload));
    let mut archive = Archive::new(decoder);
    let mut files = Vec::new();
    let mut snapshots = BTreeMap::<String, usize>::new();
    let mut metadata = Map::new();
    let entries = archive.entries().map_err(|_| ApiError::invalid_request())?;
    for (member_index, entry) in entries.enumerate() {
        if member_index >= MAX_BACKUP_DETAIL_MEMBERS {
            return Err(ApiError::invalid_request());
        }
        let entry = entry.map_err(|_| ApiError::invalid_request())?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        if entry.header().path_bytes().contains(&b'\\') {
            continue;
        }
        let name = match std::str::from_utf8(entry.path_bytes().as_ref()) {
            Ok(name) => name.to_owned(),
            Err(_) => continue,
        };
        let is_metadata = name == "backup-metadata.json";
        let public_name = if is_metadata {
            Some(name.as_str())
        } else {
            public_archive_member_name(&name)
        };
        if public_name.is_none() {
            continue;
        }
        let size = entry
            .header()
            .size()
            .map_err(|_| ApiError::invalid_request())?;
        if size > MAX_BACKUP_MEMBER_BYTES {
            return Err(ApiError::validation());
        }
        let mut bytes = Vec::new();
        entry
            .take(MAX_BACKUP_MEMBER_BYTES.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|_| ApiError::invalid_request())?;
        if bytes.len() as u64 > MAX_BACKUP_MEMBER_BYTES {
            return Err(ApiError::validation());
        }
        if name == "backup-metadata.json" {
            metadata = serde_json::from_slice::<Value>(&bytes)
                .map_err(|_| ApiError::invalid_request())?
                .as_object()
                .cloned()
                .ok_or_else(ApiError::invalid_request)?;
            continue;
        }
        let public_name = public_name.expect("validated public archive member");
        if let Some(snapshot_name) = public_name
            .strip_prefix("snapshots/")
            .and_then(|value| value.strip_suffix(".json"))
        {
            if !matches!(snapshot_name, "accounts" | "auth_keys") {
                continue;
            }
            let snapshot =
                serde_json::from_slice::<Value>(&bytes).map_err(|_| ApiError::invalid_request())?;
            let count = snapshot
                .as_array()
                .map(Vec::len)
                .ok_or_else(ApiError::invalid_request)?;
            snapshots.insert(snapshot_name.to_owned(), count);
            continue;
        }
        let digest = Sha256::digest(&bytes);
        let hash = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        files.push(json!({
            "name": public_name,
            "exists": true,
            "content_type": content_type(public_name),
            "size": bytes.len(),
            "sha256": hash,
        }));
    }
    let created_at = metadata
        .get("created_at")
        .and_then(Value::as_str)
        .filter(|value| {
            value.len() <= 64
                && !value.is_empty()
                && time::OffsetDateTime::parse(
                    value,
                    &time::format_description::well_known::Rfc3339,
                )
                .is_ok()
        })
        .map(|value| Value::String(value.to_owned()))
        .unwrap_or(Value::Null);
    let trigger = metadata
        .get("trigger")
        .and_then(Value::as_str)
        .filter(|value| matches!(*value, "manual" | "schedule"))
        .map(|value| Value::String(value.to_owned()))
        .unwrap_or(Value::Null);
    let app_version = metadata
        .get("app_version")
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 64
                && value.is_ascii()
                && value.chars().all(|character| {
                    character.is_ascii_alphanumeric() || ".-_+".contains(character)
                })
        })
        .map(|value| Value::String(value.to_owned()))
        .unwrap_or(Value::Null);
    let storage_backend = metadata
        .get("storage_backend")
        .and_then(Value::as_object)
        .and_then(|value| {
            value
                .get("type")
                .and_then(Value::as_str)
                .or_else(|| value.get("backend").and_then(Value::as_str))
        })
        .filter(|value| matches!(*value, "json" | "database" | "git"))
        .map(|value| json!({"type": value}))
        .unwrap_or_else(|| json!({}));
    let cumulative_total = match metadata.get("snapshot_manifest") {
        None => None,
        Some(Value::Object(manifest)) if manifest.get("version") == Some(&json!(1)) => {
            let accounts = manifest
                .get("accounts")
                .and_then(Value::as_object)
                .ok_or_else(ApiError::invalid_request)?;
            Some(
                accounts
                    .get("cumulative_total")
                    .and_then(Value::as_u64)
                    .ok_or_else(ApiError::invalid_request)?,
            )
        }
        Some(_) => return Err(ApiError::invalid_request()),
    };
    let snapshots = snapshots
        .into_iter()
        .map(|(name, count)| {
            if name == "accounts"
                && let Some(cumulative_total) = cumulative_total
            {
                return json!({
                    "name": name,
                    "count": count,
                    "cumulative_total": cumulative_total,
                });
            }
            json!({"name": name, "count": count})
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
    Ok(json!({
        "key": key,
        "name": key.rsplit('/').next().unwrap_or("backup.tar.gz"),
        "encrypted": key.ends_with(".enc"),
        "created_at": created_at,
        "trigger": trigger,
        "app_version": app_version,
        "storage_backend": storage_backend,
        "files": files,
        "snapshots": snapshots,
    }))
}

pub(super) async fn delete_backup(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let value = super::account_json_body(body).await?;
    let object = value.as_object().ok_or_else(ApiError::validation)?;
    let key = match object.get("key") {
        None => {
            return Err(ApiError::backup_r2_message(
                "backup_key_required",
                "备份对象 key 不能为空",
            ));
        }
        Some(Value::String(value)) => value.as_str(),
        Some(_) => return Err(ApiError::validation()),
    };
    if key.trim().is_empty() {
        return Err(ApiError::backup_r2_message(
            "backup_key_required",
            "备份对象 key 不能为空",
        ));
    }
    let state_path = backup_state_path(&state);
    let owner_gate = super::backup_owner_gate(&state_path);
    let _owner_guard = owner_gate
        .try_lock_owned()
        .map_err(|_| ApiError::backup_delete_busy())?;
    let current = {
        let Some(_state_lock) = super::try_acquire_path_write_lock(&state_path).await? else {
            return Err(ApiError::backup_delete_busy());
        };
        backup_state_map(&state)?
    };
    if current
        .get("last_status")
        .and_then(Value::as_str)
        .is_some_and(|value| value == "running")
        && current
            .get("pending_object_key")
            .and_then(Value::as_str)
            .is_some_and(|pending| pending == key)
    {
        return Err(ApiError::backup_delete_busy());
    }
    if backup_is_remote(&state) {
        let candidate = validate_remote_backup_key(&state, key)?;
        R2Client::from_state(&state)?
            .delete_object(&candidate)
            .await?;
    } else {
        let path = backup_key_file(&state, key)?;
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(ApiError::not_found());
            }
            Err(_) => return Err(ApiError::unavailable()),
        }
    }
    Ok(Json(json!({"ok": true})))
}

pub(super) async fn backup_detail(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<BackupKeyQuery>,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let key = query.key.as_deref().unwrap_or_default();
    let detail = if backup_is_remote(&state) {
        let key = validate_remote_backup_key(&state, key)?;
        let payload = R2Client::from_state(&state)?.download_bytes(&key).await?;
        let payload = decrypt_backup_if_needed(
            &state,
            &key,
            payload,
            "backup_detail_passphrase_missing",
            "当前未配置加密口令，无法查看已加密备份",
        )
        .await?;
        read_backup_detail_payload(&key, payload)?
    } else {
        if key.ends_with(".enc") {
            let path = backup_key_file(&state, key)?;
            let payload = read_bounded(&path, MAX_BACKUP_BYTES)?;
            let payload = decrypt_backup_if_needed(
                &state,
                key,
                payload,
                "backup_detail_passphrase_missing",
                "当前未配置加密口令，无法查看已加密备份",
            )
            .await?;
            read_backup_detail_payload(key, payload)?
        } else {
            read_backup_detail(&state, key)?
        }
    };
    Ok(Json(json!({"item": detail})))
}

pub(super) async fn download_backup(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<BackupKeyQuery>,
) -> Result<Response, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let key = query.key.as_deref().unwrap_or_default();
    let (payload, filename) = if backup_is_remote(&state) {
        let key = validate_remote_backup_key(&state, key)?;
        let payload = R2Client::from_state(&state)?.download_bytes(&key).await?;
        let payload = decrypt_backup_if_needed(
            &state,
            &key,
            payload,
            "backup_download_passphrase_missing",
            "当前未配置加密口令，无法下载并解密已加密备份",
        )
        .await?;
        let filename = key
            .rsplit('/')
            .next()
            .unwrap_or("backup.tar.gz")
            .trim_end_matches(".enc")
            .to_owned();
        (payload, filename)
    } else {
        let path = backup_key_file(&state, key)?;
        let payload = read_bounded(&path, MAX_BACKUP_BYTES)?;
        let payload = decrypt_backup_if_needed(
            &state,
            key,
            payload,
            "backup_download_passphrase_missing",
            "当前未配置加密口令，无法下载并解密已加密备份",
        )
        .await?;
        let filename = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("backup.tar.gz")
            .trim_end_matches(".enc")
            .to_owned();
        (payload, filename)
    };
    let disposition = format!("attachment; filename*=UTF-8''{filename}");
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/gzip"),
            (header::CONTENT_DISPOSITION, disposition.as_str()),
            (header::CONTENT_LENGTH, &payload.len().to_string()),
        ],
        Body::from(payload),
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::{
        ApiError, MAX_R2_DOWNLOAD_BYTES, MAX_R2_LIST_RESPONSE_BYTES, Map, R2Client, Value,
        apply_ccload_image_capability, backup_schedule_due, bounded_public_text,
        ccload_model_entries, ccload_model_ids, ccload_model_payload, ccload_recent_refresh_time,
        cpa_remote_proxy_profile, log_id, log_uuid_hex, merge_ccload_account_catalog,
        normalized_ccload_credential, parse_r2_list_xml, public_backup_error, public_registry_item,
        python_integer_or_zero, python_remote_string_if_present, sub2api_page_items,
    };

    use crate::model_pool::ModelProvenance;
    use axum::response::IntoResponse;
    use flate2::read::DeflateDecoder;
    use serde_json::json;
    use std::{
        io::Read,
        time::{Duration, SystemTime},
    };
    #[test]
    fn remote_group_counts_match_python_int_coercion() {
        assert_eq!(python_integer_or_zero(Some(&json!(" 7 "))), Ok(7));
        assert_eq!(python_integer_or_zero(Some(&json!(1.9))), Ok(1));
        assert_eq!(python_integer_or_zero(Some(&json!(false))), Ok(0));
        assert!(python_integer_or_zero(Some(&json!({"bad": true}))).is_err());
        assert_eq!(
            python_remote_string_if_present(Some(&json!(0)), 128),
            Some("0".to_owned())
        );
        assert_eq!(
            python_remote_string_if_present(Some(&json!(false)), 128),
            Some("False".to_owned())
        );
        assert_eq!(
            python_remote_string_if_present(Some(&Value::Null), 128),
            None
        );
    }
    #[test]
    fn clearance_test_profile_falls_back_to_legacy_proxy() {
        let profile = crate::proxy_service::profile_from_runtime(
            &json!({"enabled": false, "egress_mode": "direct", "proxy_url": ""}),
            None,
            None,
            Some("http://legacy-proxy:8080"),
            false,
            true,
        );
        assert_eq!(profile.proxy_source, "global");
        assert_eq!(profile.proxy_url, "http://legacy-proxy:8080");
    }
    #[test]
    fn public_text_projection_matches_python_str_coercion() {
        assert_eq!(bounded_public_text(Some(&json!(7)), 16), "7");
        assert_eq!(bounded_public_text(Some(&json!(false)), 16), "");
        assert_eq!(
            bounded_public_text(Some(&json!(["a", true])), 32),
            "['a', True]"
        );
    }
    #[test]
    fn public_registry_projection_matches_python_clean_values() {
        let projected = public_registry_item(
            "sub2api",
            &json!({
                "id": 7,
                "name": true,
                "base_url": "https://user:pass@example.test/root/",
                "email": ["a", true],
                "api_key": 9,
                "group_id": false,
            }),
        );
        assert_eq!(projected["id"], "7");
        assert_eq!(projected["name"], "True");
        assert_eq!(projected["base_url"], "https://example.test/root/");
        assert_eq!(projected["email"], "['a', True]");
        assert_eq!(projected["has_api_key"], true);
        assert_eq!(projected["group_id"], "");
    }

    #[test]
    fn legacy_log_id_matches_python_sha1_fixture() {
        assert_eq!(log_id(&Map::new(), "{}", 0), "7847797acb01758ab281379b");
    }

    #[test]
    fn log_uuid_hex_matches_uuid4_hex_shape() {
        let id = log_uuid_hex();
        assert_eq!(id.len(), 32);
        assert!(
            id.bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        );
        assert_eq!(&id[12..13], "4");
        assert!(matches!(id.as_bytes()[16], b'8' | b'9' | b'a' | b'b'));
    }

    #[test]
    fn image_zip_uses_deflate_and_marks_utf8_filenames() {
        let filename = "图片.png";
        let payload = b"compressible image payload".repeat(64);
        let archive = super::zip_archive(vec![(filename.to_owned(), payload.clone())])
            .expect("image archive");
        assert_eq!(u16::from_le_bytes([archive[6], archive[7]]), 1 << 11);
        assert_eq!(u16::from_le_bytes([archive[8], archive[9]]), 8);
        let compressed_size =
            u32::from_le_bytes(archive[18..22].try_into().expect("compressed size")) as usize;
        let uncompressed_size =
            u32::from_le_bytes(archive[22..26].try_into().expect("uncompressed size")) as usize;
        assert_eq!(uncompressed_size, payload.len());
        let name_length = u16::from_le_bytes([archive[26], archive[27]]) as usize;
        let extra_length = u16::from_le_bytes([archive[28], archive[29]]) as usize;
        assert_eq!(
            std::str::from_utf8(&archive[30..30 + name_length]).expect("UTF-8 filename"),
            filename
        );
        let data_start = 30 + name_length + extra_length;
        let mut decoder = DeflateDecoder::new(&archive[data_start..data_start + compressed_size]);
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .expect("inflate image member");
        assert_eq!(decoded, payload);
        assert!(compressed_size < uncompressed_size);
    }

    #[test]
    fn backup_schedule_due_matches_python_scheduler_rules() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let enabled = json!({"enabled": true, "interval_minutes": 60});
        assert!(!backup_schedule_due(
            &json!({"enabled": false}),
            &Map::new(),
            now
        ));
        let running = serde_json::from_value(json!({"running": true})).expect("running state");
        assert!(!backup_schedule_due(&enabled, &running, now));
        assert!(backup_schedule_due(&enabled, &Map::new(), now));
        let recent = serde_json::from_value(json!({"last_finished_at":"1970-01-12T13:46:40Z"}))
            .expect("recent state");
        assert!(!backup_schedule_due(&enabled, &recent, now));
        let old = serde_json::from_value(json!({"last_finished_at":"1970-01-12T11:46:40Z"}))
            .expect("old state");
        assert!(backup_schedule_due(&enabled, &old, now));
    }

    #[test]
    fn backup_restart_recovery_clears_stale_running_and_preserves_pending_work() {
        let running = json!({
            "running": true,
            "last_status": "running",
            "pending_object_key": "backups/backup-pending.tar.gz",
            "pending_target_fingerprint": "target",
            "last_finished_at": "2026-09-01T00:00:00Z"
        });
        let recovered = super::backup_state_after_restart(running.as_object().expect("state"))
            .expect("stale running backup should recover");
        assert!(recovered.get("running").is_none());
        assert_eq!(recovered["last_status"], "idle");
        assert_eq!(
            recovered["pending_object_key"],
            "backups/backup-pending.tar.gz"
        );
        assert_eq!(recovered["pending_target_fingerprint"], "target");
        assert_eq!(recovered["last_finished_at"], "2026-09-01T00:00:00Z");
        assert!(
            super::backup_state_after_restart(
                json!({"last_status":"success"}).as_object().expect("state")
            )
            .is_none()
        );
    }

    #[test]
    fn python_import_restart_recovery_preserves_job_progress_and_errors() {
        let mut job = json!({
            "status":"running",
            "total":12,
            "completed":7,
            "added":3,
            "skipped":2,
            "failed":1,
            "errors":[{"name":"a.json","error":"network"}],
            "updated_at":"2026-09-20T12:00:00Z"
        })
        .as_object()
        .cloned()
        .expect("import job");
        super::mark_interrupted_import_job(&mut job, false);
        assert_eq!(job["status"], "failed");
        assert_eq!(job["completed"], 7);
        assert_eq!(job["failed"], 1);
        assert_eq!(job["errors"], json!([{"name":"a.json","error":"network"}]));
        assert_eq!(job["updated_at"], "2026-09-20T12:00:00Z");
    }

    #[test]
    fn cpa_remote_requests_use_legacy_global_proxy_not_runtime_egress() {
        let root = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-cpa-proxy-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("CPA proxy test directory");
        let accounts_path = root.join("accounts.json");
        std::fs::write(&accounts_path, "[]").expect("empty accounts");
        std::fs::write(
            root.join("config.json"),
            serde_json::to_vec(&json!({
                "proxy": "http://legacy-proxy:8080",
                "proxy_runtime": {
                    "enabled": true,
                    "egress_mode": "single_proxy",
                    "proxy_url": "http://runtime-proxy:8081",
                    "skip_ssl_verify": true
                }
            }))
            .expect("CPA proxy config"),
        )
        .expect("write CPA proxy config");
        let state = crate::AppState::new(crate::AppConfig {
            version: "test".to_owned(),
            auth_key: None,
            models: vec!["auto".to_owned()],
            upstream_base_url: Some("https://chatgpt.com".to_owned()),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(accounts_path),
            upstream_protocol: crate::UpstreamProtocol::ChatGpt,
        })
        .expect("CPA proxy state");

        let profile = cpa_remote_proxy_profile(&state);
        assert_eq!(profile.proxy_source, "global");
        assert_eq!(profile.proxy_url, "http://legacy-proxy:8080");
        assert!(profile.skip_ssl_verify);

        drop(state);
        std::fs::remove_dir_all(root).expect("CPA proxy test cleanup");
    }

    #[test]
    fn sub2api_page_parser_matches_python_paged_shapes_and_totals() {
        let wrapped = json!({
            "code": 0,
            "message": "ok",
            "data": {
                "list": [{"id": "first"}, {"id": "second"}],
                "total": 401
            }
        });
        let (items, total) = sub2api_page_items(&wrapped).expect("wrapped page");
        assert_eq!(items.len(), 2);
        assert_eq!(total, 401);

        let zero_total = json!({"items": [{"id": "only"}], "total": 0});
        let (items, total) = sub2api_page_items(&zero_total).expect("zero total fallback");
        assert_eq!(items.len(), 1);
        assert_eq!(total, 1);

        let unwrapped = json!([{"id": "only"}]);
        let (items, total) = sub2api_page_items(&unwrapped).expect("unwrapped page");
        assert_eq!(items.len(), 1);
        assert_eq!(total, 1);

        let unknown = json!({"unexpected": []});
        let (items, total) = sub2api_page_items(&unknown).expect("unknown shape is empty");
        assert!(items.is_empty());
        assert_eq!(total, 0);

        let invalid_total = json!({"data": [], "total": "not-a-number"});
        assert!(sub2api_page_items(&invalid_total).is_err());
    }

    #[test]
    fn r2_list_parser_matches_python_optional_fields_and_cleaning() {
        let xml = concat!(
            "<ListBucketResult>",
            "<Contents><Key>  backups/backup-one.tar.gz  </Key>",
            "<LastModified> 2026-08-24T00:00:00Z </LastModified></Contents>",
            "<Contents><Key>   </Key><Size>not-a-size</Size></Contents>",
            "<Contents><Size>9</Size></Contents>",
            "<Contents><Key>backups/backup-two.tar.gz</Key><Size>  </Size></Contents>",
            "<IsTruncated>false</IsTruncated>",
            "</ListBucketResult>",
        );
        let (items, truncated, continuation) = parse_r2_list_xml(xml).expect("valid R2 XML");
        assert!(!truncated);
        assert!(continuation.is_none());
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].key, "backups/backup-one.tar.gz");
        assert_eq!(items[0].size, 0);
        assert_eq!(items[0].updated_at, "2026-08-24T00:00:00Z");
        assert_eq!(items[1].key, "backups/backup-two.tar.gz");
        assert_eq!(items[1].size, 0);
    }

    #[test]
    fn r2_list_budget_matches_python_contract() {
        assert_eq!(MAX_R2_LIST_RESPONSE_BYTES, 4 * 1024 * 1024);
        assert_eq!(MAX_R2_DOWNLOAD_BYTES, 512 * 1024 * 1024);
    }

    #[tokio::test]
    async fn r2_management_errors_use_python_safe_detail_contract() {
        let error = match R2Client::from_settings(&Map::new()) {
            Ok(_) => panic!("missing R2 settings must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.code(), "r2_config_incomplete");
        let response = error.into_response();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .expect("R2 error body");
        let value: Value = serde_json::from_slice(&body).expect("R2 error JSON");
        assert_eq!(
            value["detail"]["error"],
            "R2 配置不完整：缺少 Account ID、Access Key ID、Secret Access Key、Bucket"
        );

        let response =
            ApiError::backup_r2_status("r2_connection_failed", "连接 R2 失败", 503).into_response();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .expect("R2 status error body");
        let value: Value = serde_json::from_slice(&body).expect("R2 status error JSON");
        assert_eq!(value["detail"]["error"], "连接 R2 失败：HTTP 503");
    }

    #[test]
    fn backup_state_projects_all_python_r2_status_errors() {
        for (code, expected) in [
            ("r2_connection_failed", "连接 R2 失败：HTTP 503"),
            ("r2_upload_failed", "上传备份失败：HTTP 503"),
            ("r2_delete_failed", "删除备份失败：HTTP 503"),
            ("r2_read_failed", "读取备份失败：HTTP 503"),
            ("r2_list_failed", "获取备份列表失败：HTTP 503"),
        ] {
            let mut raw = Map::new();
            raw.insert("last_error_code".to_owned(), Value::String(code.to_owned()));
            raw.insert("last_error_status".to_owned(), Value::from(503));
            assert_eq!(
                public_backup_error(&raw),
                Value::String(expected.to_owned()),
                "Python public backup state mapping for {code}"
            );
        }

        let mut malformed = Map::new();
        malformed.insert(
            "last_error_code".to_owned(),
            Value::String("r2_list_failed".to_owned()),
        );
        malformed.insert("last_error_status".to_owned(), Value::from(700));
        assert_eq!(
            public_backup_error(&malformed),
            Value::String("备份执行失败，请稍后重试".to_owned())
        );
    }

    #[test]
    fn ccload_import_accepts_access_token_only_and_drops_secondary_tokens() {
        let credential = normalized_ccload_credential(Some(&serde_json::json!({
            "access_token": "access-only",
            "type": "Codex",
            "plan_type": "pro",
            "id_token": "discarded-id",
            "refresh_token": "discarded-refresh"
        })))
        .expect("access-token-only ccLoad credential");
        assert_eq!(credential, "access-only");
    }

    #[test]
    fn ccload_import_uses_recent_refresh_time_as_created_at() {
        let credential = serde_json::json!({
            "refresh_time": "2026-08-20T12:34:56Z"
        });
        assert_eq!(
            ccload_recent_refresh_time(None, Some(&credential)),
            Some("2026-08-20 12:34:56".to_owned())
        );

        let channel = serde_json::json!({"updated_at": 1_755_693_296_i64});
        assert_eq!(
            ccload_recent_refresh_time(Some(&channel), None),
            Some("2025-08-20 12:34:56".to_owned())
        );
    }

    #[test]
    fn ccload_model_catalog_uses_explicit_provenance() {
        let value = serde_json::json!([
            {"model":"gpt-5-codex","source":"codex"},
            {"model":"gpt-5-codex","endpoint":"/backend-api/models?iim=false&is_gizmo=false&supports_model_picker_upgrade_presets=true"},
            {"model":"auto","endpoint":"/backend-api/tpp/models/?supports_model_picker_upgrade_presets=true"},
            {"model":"codex-endpoint-only","endpoint":"/backend-api/codex/models"},
            {"model":"configured-codex-name"},
            {"model":"unknown-source","source":"unknown"}
        ]);
        let entries = ccload_model_entries(Some(&value));
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].id, "gpt-5-codex");
        assert_eq!(entries[0].provenance, ModelProvenance::Web);
        assert_eq!(entries[1].provenance, ModelProvenance::Web);
        assert_eq!(entries[2].provenance, ModelProvenance::Codex);
        assert_eq!(entries[3].provenance, ModelProvenance::Unknown);
        assert_eq!(entries[4].provenance, ModelProvenance::Unknown);
        assert_eq!(
            ccload_model_ids(Some(&value)),
            vec!["gpt-5-codex".to_owned(), "auto".to_owned()]
        );
        let (models, sources) = ccload_model_payload(entries);
        assert_eq!(models, serde_json::json!(["gpt-5-codex", "auto"]));
        assert_eq!(
            sources,
            serde_json::json!({"gpt-5-codex": "web", "auto": "web"})
        );
    }

    #[test]
    fn ccload_image_projection_requires_a_refreshed_positive_quota() {
        let mut without_capability = serde_json::json!({
            "models": ["web-model", "gpt-image-2"],
            "model_sources": {"web-model":"web", "gpt-image-2":"web"}
        });
        apply_ccload_image_capability(&mut without_capability, &[]);
        assert_eq!(
            without_capability["models"],
            serde_json::json!(["web-model"])
        );
        assert!(
            without_capability["model_sources"]
                .get("gpt-image-2")
                .is_none()
        );

        let mut with_capability = serde_json::json!({
            "models": ["web-model", "GPT-IMAGE-2", "gpt-image-2.5"],
            "model_sources": {"web-model":"web", "GPT-IMAGE-2":"web", "gpt-image-2.5":"web"}
        });
        apply_ccload_image_capability(
            &mut with_capability,
            &["gpt-image-2".to_owned(), "gpt-image-2.5".to_owned()],
        );
        assert_eq!(
            with_capability["models"],
            serde_json::json!(["web-model", "gpt-image-2", "gpt-image-2.5"])
        );
        assert_eq!(with_capability["model_sources"]["gpt-image-2"], "image");
        assert_eq!(with_capability["model_sources"]["gpt-image-2.5"], "image");
    }

    #[test]
    fn ccload_channel_catalog_uses_only_the_current_token_catalog() {
        let (models, sources) = merge_ccload_account_catalog(
            Some(&serde_json::json!(["gpt-5-5"])),
            Some(&serde_json::json!({"gpt-5-5":"web"})),
            &serde_json::json!({
                "models": ["gpt-image-2"],
                "model_sources": {"gpt-image-2":"image"}
            }),
        );
        assert_eq!(models, serde_json::json!(["gpt-image-2"]));
        assert_eq!(sources, serde_json::json!({"gpt-image-2":"image"}));
    }
    #[test]
    fn management_json_writers_match_python_pretty_newline_contract() {
        let root = std::env::temp_dir().join(format!(
            "chatgpt2api-management-json-{}-{}",
            std::process::id(),
            super::unix_seconds(SystemTime::now())
        ));
        std::fs::create_dir_all(&root).expect("management JSON test directory");
        let value = serde_json::json!({"items": {"图片": "image.png"}});
        let locked = root.join("locked.json");
        super::write_json(&locked, &value).expect("locked JSON write");
        assert!(
            std::fs::read(&locked)
                .expect("locked JSON bytes")
                .ends_with(b"\n")
        );
        let unlocked = root.join("unlocked.json");
        super::write_json_unlocked(&unlocked, &value).expect("unlocked JSON write");
        assert!(
            std::fs::read(&unlocked)
                .expect("unlocked JSON bytes")
                .ends_with(b"\n")
        );
        std::fs::remove_dir_all(root).expect("management JSON test cleanup");
    }

    #[test]
    fn image_index_mtime_fallback_uses_local_wall_clock() {
        let offset = time::UtcOffset::current_local_offset().unwrap_or(time::UtcOffset::UTC);
        let local = time::OffsetDateTime::from_unix_timestamp(0)
            .expect("Unix epoch")
            .to_offset(offset);
        let expected = format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            local.year(),
            local.month() as u8,
            local.day(),
            local.hour(),
            local.minute(),
            local.second(),
        );
        assert_eq!(super::local_timestamp(SystemTime::UNIX_EPOCH), expected);
    }
    #[test]
    fn malformed_image_index_recovers_like_python() {
        let root = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-image-index-recovery-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("image index test directory");
        let accounts_path = root.join("accounts.json");
        std::fs::write(&accounts_path, "[]").expect("empty accounts");
        let state = crate::AppState::new(crate::AppConfig {
            version: "test".to_owned(),
            auth_key: None,
            models: Vec::new(),
            upstream_base_url: None,
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(accounts_path),
            upstream_protocol: crate::UpstreamProtocol::ChatGpt,
        })
        .expect("image index state");
        std::fs::write(super::image_index_path(&state), b"{broken").expect("corrupt index");
        assert!(
            super::read_image_index_unlocked(&state)
                .expect("malformed index recovery")
                .is_empty()
        );
        std::fs::write(
            super::image_index_path(&state),
            serde_json::to_vec(&serde_json::json!({
                "items": {
                    "invalid.txt": {},
                    "../escape.png": {},
                    "valid.png": {}
                }
            }))
            .expect("index JSON"),
        )
        .expect("mixed image index");
        let filtered = super::read_image_index_unlocked(&state).expect("filtered index");
        assert!(filtered.get("invalid.txt").is_none());
        assert!(filtered.get("../escape.png").is_none());
        assert!(filtered.get("valid.png").is_some());
        drop(state);
        std::fs::remove_dir_all(root).expect("image index test cleanup");
    }
    #[test]
    fn image_delete_boolean_matches_pydantic_coercion() {
        for (raw, expected) in [
            (serde_json::json!(true), true),
            (serde_json::json!(" YES "), true),
            (serde_json::json!(1.0), true),
            (serde_json::json!("off"), false),
        ] {
            assert_eq!(super::pydantic_management_bool(&raw), Some(expected));
        }
        assert_eq!(
            super::pydantic_management_bool(&serde_json::json!("maybe")),
            None
        );
    }

    #[test]
    fn cleanup_query_coercion_matches_python_query_models() {
        assert_eq!(
            super::cleanup_query_target(Some(" 1_000 ")).expect("integer query"),
            1000
        );
        assert_eq!(
            super::cleanup_query_target(None).expect("default target"),
            500
        );
        assert!(super::cleanup_query_target(Some("1.5")).is_err());
        assert!(super::cleanup_query_bool(Some(" YES ")).expect("bool query"));
        assert!(!super::cleanup_query_bool(Some("off")).expect("bool query"));
        assert!(super::cleanup_query_bool(Some("maybe")).is_err());
    }
}
