use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{self, File},
    io::{Cursor, Read},
    path::{Component, Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json,
    body::Body,
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use futures_util::{StreamExt, stream::FuturesUnordered};
use image::ImageReader;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tar::{Archive, Builder, Header};

use super::{
    ApiError, AppState, admin_authenticated, authenticated, data_file, image_content_type,
    image_root, read_image_tags, redact_config, safe_relative_path,
};

const MAX_LOG_BYTES: u64 = 16 * 1024 * 1024;
const MAX_LOG_ITEMS: usize = 200;
const MAX_IMAGE_ITEMS: usize = 5_000;
const MAX_IMAGE_ARCHIVE_ITEMS: usize = 5_000;
const MAX_IMAGE_ARCHIVE_BYTES: usize = 512 * 1024 * 1024;
const MAX_BACKUP_BYTES: u64 = 512 * 1024 * 1024;
const MAX_BACKUP_MEMBER_BYTES: u64 = 64 * 1024 * 1024;
const CCLOAD_CHANNEL_BROWSE_DEADLINE: Duration = Duration::from_secs(90);
const CCLOAD_IMPORT_DEADLINE: Duration = Duration::from_secs(30 * 60);
const CCLOAD_MAX_CHANNELS: usize = 5_000;
const CCLOAD_MAX_CHANNEL_PAGES: usize = 25;

#[derive(Debug, Deserialize, Default)]
pub(super) struct LogQuery {
    #[serde(rename = "type")]
    pub r#type: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub(super) struct ImageCleanupQuery {
    pub target_free_mb: Option<u64>,
    pub dry_run: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
pub(super) struct BackupKeyQuery {
    pub key: Option<String>,
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos())
        .unwrap_or_default()
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

fn iso_timestamp(value: SystemTime) -> String {
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
    super::atomic_replace_checked_with_limit(path, bytes, MAX_BACKUP_BYTES, false)
}

fn write_json(path: &Path, value: &Value) -> Result<(), ApiError> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|_| ApiError::unavailable())?;
    write_atomic(path, &bytes)
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
        .take(MAX_IMAGE_ITEMS)
        .collect()
}

fn image_item(tags: &Map<String, Value>, relative: &str, path: &Path) -> Option<Value> {
    let metadata = fs::metadata(path).ok()?;
    let created_at = metadata
        .modified()
        .ok()
        .map(unix_seconds)
        .unwrap_or_default();
    let date = date_from_unix(created_at);
    let tags = tags.get(relative).cloned().unwrap_or_else(|| json!([]));
    Some(json!({
        "rel": relative,
        "path": relative,
        "name": path.file_name().and_then(|value| value.to_str()).unwrap_or("image"),
        "date": date,
        "size": metadata.len(),
        "url": format!("/images/{relative}"),
        "thumbnail_url": format!("/image-thumbnails/{relative}"),
        "created_at": created_at.to_string(),
        "tags": tags,
    }))
}

pub(super) async fn list_images(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let tags = read_image_tags(&state)?;
    let mut items = Vec::new();
    for (relative, path) in image_files(&state) {
        if let Some(item) = image_item(&tags, &relative, &path) {
            items.push(item);
        }
    }
    let mut grouped: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for item in &items {
        let date = item
            .get("date")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        grouped.entry(date).or_default().push(item.clone());
    }
    let groups = grouped
        .into_iter()
        .map(|(date, items)| json!({"date": date, "items": items}))
        .collect::<Vec<_>>();
    Ok(Json(json!({"items": items, "groups": groups})))
}

fn image_path_from_value(value: &Value) -> Result<PathBuf, ApiError> {
    value
        .as_str()
        .and_then(safe_relative_path)
        .ok_or_else(ApiError::invalid_request)
}

pub(super) async fn delete_images(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let value = super::account_json_body(body).await?;
    let object = value.as_object().ok_or_else(ApiError::invalid_request)?;
    let all_matching = object
        .get("all_matching")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let start_date = object
        .get("start_date")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();
    let end_date = object
        .get("end_date")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();
    let mut targets = Vec::new();
    if all_matching {
        let listed = list_images(State(state.clone()), headers.clone()).await?.0;
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
    } else if let Some(paths) = object.get("paths").and_then(Value::as_array) {
        for path in paths {
            targets.push(
                image_path_from_value(path)?
                    .to_string_lossy()
                    .replace('\\', "/"),
            );
        }
    }
    let root = image_root(&state);
    let mut removed = 0usize;
    for relative in targets {
        let Some(safe) = safe_relative_path(&relative) else {
            continue;
        };
        if fs::remove_file(root.join(safe)).is_ok() {
            removed += 1;
        }
    }
    Ok(Json(json!({"removed": removed})))
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

fn zip_archive(files: Vec<(String, Vec<u8>)>) -> Result<Vec<u8>, ApiError> {
    if files.is_empty() {
        return Err(ApiError::not_found());
    }
    let mut output = Vec::new();
    let mut central = Vec::new();
    let mut file_count = 0usize;
    for (name, payload) in files {
        if output.len().saturating_add(payload.len()) > MAX_IMAGE_ARCHIVE_BYTES {
            return Err(ApiError::validation());
        }
        let name_bytes = name.as_bytes();
        let offset = u32::try_from(output.len()).map_err(|_| ApiError::validation())?;
        let size = u32::try_from(payload.len()).map_err(|_| ApiError::validation())?;
        let checksum = crc32(&payload);
        zip_u32(&mut output, 0x0403_4b50);
        zip_u16(&mut output, 20);
        zip_u16(&mut output, 0);
        zip_u16(&mut output, 0);
        zip_u16(&mut output, 0);
        zip_u16(&mut output, 0);
        zip_u32(&mut output, checksum);
        zip_u32(&mut output, size);
        zip_u32(&mut output, size);
        zip_u16(
            &mut output,
            u16::try_from(name_bytes.len()).map_err(|_| ApiError::validation())?,
        );
        zip_u16(&mut output, 0);
        output.extend_from_slice(name_bytes);
        output.extend_from_slice(&payload);
        file_count = file_count.saturating_add(1);

        zip_u32(&mut central, 0x0201_4b50);
        zip_u16(&mut central, 20);
        zip_u16(&mut central, 20);
        zip_u16(&mut central, 0);
        zip_u16(&mut central, 0);
        zip_u16(&mut central, 0);
        zip_u16(&mut central, 0);
        zip_u32(&mut central, checksum);
        zip_u32(&mut central, size);
        zip_u32(&mut central, size);
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
    let count =
        u16::try_from(file_count.min(usize::from(u16::MAX))).map_err(|_| ApiError::validation())?;
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
        .ok_or_else(ApiError::invalid_request)?;
    if paths.is_empty() || paths.len() > MAX_IMAGE_ARCHIVE_ITEMS {
        return Err(ApiError::validation());
    }
    let root = image_root(&state);
    let mut files = Vec::new();
    for item in paths {
        let relative = image_path_from_value(item)?;
        let name = relative.to_string_lossy().replace('\\', "/");
        let path = safe_regular_file(&root, &relative)?;
        let payload = read_bounded(&path, MAX_IMAGE_ARCHIVE_BYTES as u64)?;
        files.push((name, payload));
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
    let path = safe_regular_file(&image_root(&state), &relative)?;
    let payload = read_bounded(&path, MAX_IMAGE_ARCHIVE_BYTES as u64)?;
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("image.bin")
        .replace('"', "");
    let disposition = format!("attachment; filename=\"{filename}\"");
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, image_content_type(&path)),
            (header::CONTENT_DISPOSITION, disposition.as_str()),
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
    for (_relative, path) in image_files(state) {
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
    let image = ImageReader::open(path)
        .map_err(|_| ApiError::unavailable())?
        .decode()
        .map_err(|_| ApiError::unavailable())?;
    let mut output = Cursor::new(Vec::new());
    image
        .write_to(&mut output, image::ImageFormat::Png)
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

pub(super) async fn cleanup_images(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ImageCleanupQuery>,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let target_free_mb = query.target_free_mb.unwrap_or(500);
    let dry_run = query.dry_run.unwrap_or(false);
    let root = image_root(&state);
    let current_free_mb = fs2::available_space(&root)
        .map(|value| value / 1024 / 1024)
        .unwrap_or_default();
    let mut candidates = image_files(&state)
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
    for (_modified, _relative, path, size) in candidates {
        if current_free_mb.saturating_add(freed_bytes / 1024 / 1024) >= target_free_mb {
            break;
        }
        if !dry_run && fs::remove_file(path).is_ok() {
            removed += 1;
        }
        freed_bytes = freed_bytes.saturating_add(size);
    }
    let done = current_free_mb.saturating_add(freed_bytes / 1024 / 1024) >= target_free_mb;
    Ok(Json(json!({
        "removed": removed,
        "freed_mb": freed_bytes / 1024 / 1024,
        "target_free_mb": target_free_mb,
        "current_free_mb": current_free_mb,
        "done": done,
        "dry_run": dry_run,
    })))
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
        "reason",
        "key_id",
        "key_name",
        "role",
        "endpoint",
        "model",
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
            _ => {}
        }
    }
    Value::Object(projected)
}

fn log_id(raw: &Map<String, Value>, line: &str, ordinal: usize) -> String {
    if let Some(id) = raw
        .get("id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty() && value.len() <= 256)
    {
        return id.to_owned();
    }
    let mut hasher = Sha256::new();
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
    let ids = value
        .get("ids")
        .and_then(Value::as_array)
        .ok_or_else(ApiError::invalid_request)?;
    if ids.len() > MAX_LOG_ITEMS * 10 {
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
        let Some(projected) = project_log(line, ordinal) else {
            kept.extend_from_slice(line.as_bytes());
            kept.push(b'\n');
            continue;
        };
        let id = projected
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if wanted.contains(id) {
            removed += 1;
        } else {
            let encoded = serde_json::to_vec(&projected).map_err(|_| ApiError::unavailable())?;
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
    value.and_then(Value::as_bool).unwrap_or(fallback)
}

fn runtime_value(state: &AppState) -> Value {
    let config = read_config(state);
    let raw = config
        .get("proxy_runtime")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let object = object_or_empty(raw);
    let clearance = object
        .get("clearance")
        .cloned()
        .map(object_or_empty)
        .unwrap_or_default();
    let mode = object
        .get("egress_mode")
        .and_then(Value::as_str)
        .filter(|value| matches!(*value, "direct" | "single_proxy"))
        .unwrap_or("direct");
    let clearance_mode = clearance
        .get("mode")
        .and_then(Value::as_str)
        .filter(|value| matches!(*value, "none" | "manual" | "flaresolverr"))
        .unwrap_or("none");
    json!({
        "enabled": bool_or(object.get("enabled"), false),
        "egress_mode": mode,
        "proxy_url": public_url(object.get("proxy_url")),
        "resource_proxy_url": public_url(object.get("resource_proxy_url")),
        "skip_ssl_verify": bool_or(object.get("skip_ssl_verify"), false),
        "reset_session_status_codes": object.get("reset_session_status_codes").cloned().unwrap_or_else(|| json!([403])),
        "clearance": {
            "enabled": bool_or(clearance.get("enabled"), false),
            "mode": clearance_mode,
            "cf_cookies": "",
            "cf_clearance": "",
            "has_cf_cookies": clearance.get("cf_cookies").and_then(Value::as_str).is_some_and(|value| !value.is_empty()),
            "has_cf_clearance": clearance.get("cf_clearance").and_then(Value::as_str).is_some_and(|value| !value.is_empty()),
            "user_agent": clearance.get("user_agent").and_then(Value::as_str).unwrap_or(""),
            "browser": clearance.get("browser").and_then(Value::as_str).unwrap_or("chrome"),
            "flaresolverr_url": public_url(clearance.get("flaresolverr_url")),
            "timeout_sec": clearance.get("timeout_sec").and_then(Value::as_u64).unwrap_or(60),
            "refresh_interval": clearance.get("refresh_interval").and_then(Value::as_u64).unwrap_or(3600),
            "warm_up_on_start": bool_or(clearance.get("warm_up_on_start"), false),
        }
    })
}

fn runtime_status(state: &AppState) -> Value {
    let runtime = runtime_value(state);
    let enabled = runtime
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let proxy_url = runtime
        .get("proxy_url")
        .and_then(Value::as_str)
        .unwrap_or_default();
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
        "egress_mode": runtime.get("egress_mode").cloned().unwrap_or_else(|| json!("direct")),
        "proxy_source": if !proxy_url.is_empty() { "runtime" } else { "direct" },
        "has_proxy": !proxy_url.is_empty(),
        "clearance_enabled": clearance_enabled,
        "clearance_mode": clearance.get("mode").cloned().unwrap_or_else(|| json!("none")),
        "has_clearance_bundle": false,
        "cached_clearance_hosts": [],
    })
}

pub(super) fn health_proxy_runtime(state: &AppState) -> Value {
    let status = runtime_status(state);
    json!({
        "enabled": status.get("enabled").and_then(Value::as_bool).unwrap_or(false),
        "clearance_enabled": status
            .get("clearance_enabled")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
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
    Ok(Json(
        json!({"runtime": runtime_value(&state), "status": runtime_status(&state)}),
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
    config.insert("proxy_runtime".to_owned(), Value::Object(runtime));
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
    let input = value
        .get("url")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();
    let runtime = object_or_empty(runtime_value(&state));
    let candidate = if input.is_empty() {
        runtime
            .get("proxy_url")
            .and_then(Value::as_str)
            .unwrap_or_default()
    } else {
        input
    };
    let source = if input.is_empty() { "runtime" } else { "input" };
    let mut result = proxy_result_base(source, candidate);
    if candidate.is_empty() {
        result.extend([
            ("ok".to_owned(), json!(false)),
            ("status".to_owned(), json!(0)),
            ("latency_ms".to_owned(), json!(0)),
            ("error".to_owned(), json!("no active proxy configured")),
        ]);
        return Ok(Json(Value::Object(result)));
    }
    let started = std::time::Instant::now();
    let proxy = reqwest::Proxy::all(candidate).map_err(|_| ApiError::invalid_request())?;
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .proxy(proxy)
        .build()
        .map_err(|_| ApiError::unavailable())?;
    let response = client.get("https://chatgpt.com/api/auth/csrf").send().await;
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
    Ok(Json(Value::Object(result)))
}

pub(super) async fn test_clearance(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let _ = super::account_json_body(body).await?;
    let runtime = runtime_status(&state);
    let enabled = runtime
        .get("clearance_enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let result = if !enabled {
        json!({
            "ok": false, "status": "disabled", "latency_ms": 0,
            "has_cookies": false, "user_agent": "", "error": "clearance is disabled",
            "runtime": runtime,
        })
    } else {
        json!({
            "ok": false, "status": "failed", "latency_ms": 0,
            "has_cookies": false, "user_agent": "", "error": "clearance refresh returned no bundle",
            "runtime": runtime,
        })
    };
    Ok(Json(json!({"result": result})))
}

fn image_storage_settings(state: &AppState) -> Map<String, Value> {
    object_or_empty(
        read_config(state)
            .get("image_storage")
            .cloned()
            .unwrap_or_else(|| json!({})),
    )
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
        "created_at",
        "updated_at",
        "total",
        "completed",
        "added",
        "skipped",
        "refreshed",
        "failed",
        "errors",
    ] {
        if let Some(value) = object.get(key) {
            output.insert(key.to_owned(), value.clone());
        }
    }
    Value::Object(output)
}

fn public_registry_item(kind: &str, value: &Value) -> Value {
    let object = value.as_object().cloned().unwrap_or_default();
    let id = object.get("id").cloned().unwrap_or_else(|| json!(""));
    let name = object.get("name").cloned().unwrap_or_else(|| json!(""));
    let base_url = object
        .get("base_url")
        .and_then(Value::as_str)
        .map(|value| public_url(Some(&Value::String(value.to_owned()))))
        .map(Value::String)
        .unwrap_or_else(|| json!(""));
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
            "email": object.get("email").and_then(Value::as_str).unwrap_or(""),
            "has_api_key": object.get("api_key").and_then(Value::as_str).is_some_and(|value| !value.is_empty()),
            "group_id": object.get("group_id").and_then(Value::as_str).unwrap_or(""),
            "import_job": job,
        }),
        "ccload" => json!({
            "id": id,
            "name": name,
            "base_url": base_url,
            "has_password": object.get("password").and_then(Value::as_str).is_some_and(|value| !value.is_empty()),
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
    let timestamp = iso_timestamp(SystemTime::now());
    let created_at = created_at
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(timestamp.as_str());
    json!({
        "job_id": job_id,
        "status": status,
        "created_at": created_at,
        "updated_at": iso_timestamp(SystemTime::now()),
        "total": progress.total,
        "completed": progress.completed,
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
    authenticated(&headers, &state).await?;
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .map(|(_, token)| token.trim())
        .unwrap_or_default();
    let (role, subject_id, name) = if state.config.auth_key.as_deref().is_some_and(|expected| {
        super::constant_time_equal(token.as_bytes(), expected.trim().as_bytes())
    }) {
        ("admin".to_owned(), "admin".to_owned(), "admin".to_owned())
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
    required_name(&object)?;
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
    if updates.contains_key("name") {
        object.insert("name".to_owned(), Value::String(required_name(&updates)?));
    }
    if updates.contains_key("base_url") {
        object.insert(
            "base_url".to_owned(),
            Value::String(normalized_remote_url(updates.get("base_url"))?),
        );
    }
    if let Some(secret) = updates.get("secret_key") {
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
        state
            .client
            .get(format!("{base}/v0/management/auth-files"))
            .bearer_auth(secret)
            .header("Accept", "application/json"),
    )
    .await?;
    let files = remote_array(&value, &["files"]).ok_or_else(ApiError::upstream)?
        .into_iter().take(5_000).filter_map(|item| { let object=item.as_object()?; let name=object.get("name").and_then(Value::as_str)?.trim(); if name.is_empty(){return None;} Some(json!({"name":name,"email":object.get("email").and_then(Value::as_str).or_else(||object.get("account").and_then(Value::as_str)).unwrap_or("")})) }).collect::<Vec<_>>();
    Ok(Json(json!({"pool_id": pool_id, "files": files})))
}

async fn execute_cpa_import(
    state: AppState,
    pool_id: String,
    names: Vec<String>,
    expected_job_id: String,
) {
    let Some(pool) = registry_item(&state, "cpa_pools", &pool_id) else {
        return;
    };
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
    let mut errors = Vec::new();
    let mut successful = 0usize;
    let mut failed = 0usize;
    let mut imported = Vec::new();
    for name in &names {
        let value = remote_json(
            &state,
            state
                .client
                .get(format!("{base}/v0/management/auth-files/download"))
                .query(&[("name", name)])
                .bearer_auth(&secret)
                .header("Accept", "application/json"),
        )
        .await;
        match value.and_then(|value| {
            value
                .get("access_token")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .ok_or_else(ApiError::upstream)
        }) {
            Ok(token) => {
                imported.push(json!({"access_token": token, "source_type": "codex"}));
                successful += 1;
            }
            Err(_) => {
                failed += 1;
                errors.push(json!({"name": name, "error": "远程文件导入失败"}));
            }
        }
    }
    let (added, skipped, failed) = match state.account_store.merge_import_records(imported).await {
        Ok((added, skipped)) => (added, skipped, failed),
        Err(_) => {
            errors.push(json!({"name": "accounts", "error": "账号快照写入失败"}));
            (0, 0, failed.saturating_add(successful))
        }
    };
    let job = import_job(
        &expected_job_id,
        names.len(),
        added,
        skipped,
        0,
        failed,
        errors,
    );
    let _ = set_registry_job(&state, "cpa_pools", &pool_id, job, Some(&expected_job_id));
}

pub(super) async fn start_cpa_import(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(pool_id): AxumPath<String>,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    registry_item(&state, "cpa_pools", &pool_id).ok_or_else(ApiError::not_found)?;
    let names = super::account_json_body(body)
        .await?
        .get("names")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(ApiError::invalid_request)?;
    if names.len() > 5_000 {
        return Err(ApiError::validation());
    }
    let names = names
        .iter()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .take(5_000)
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if names.is_empty() {
        return Err(ApiError::invalid_request());
    }
    let job_id = format!("job-{}-{}", std::process::id(), now_nanos());
    let job = json!({
        "job_id": job_id.clone(),
        "status": "pending",
        "created_at": iso_timestamp(SystemTime::now()),
        "updated_at": iso_timestamp(SystemTime::now()),
        "total": names.len(),
        "completed": 0,
        "added": 0,
        "skipped": 0,
        "refreshed": 0,
        "failed": 0,
        "errors": [],
    });
    let saved = begin_registry_job(&state, "cpa_pools", &pool_id, job)?;
    tokio::spawn(execute_cpa_import(state, pool_id, names, job_id));
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
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty() && text.chars().count() <= limit)
        .unwrap_or_default()
        .to_owned()
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

fn normalized_ccload_credential(value: Option<&Value>) -> Option<HashMap<String, String>> {
    let object = value?.as_object()?;
    let mut credential = HashMap::new();
    for name in [
        "id_token",
        "access_token",
        "refresh_token",
        "account_id",
        "email",
        "type",
        "expired",
        "plan_type",
    ] {
        let text = match object.get(name) {
            None => String::new(),
            Some(Value::String(text)) => text.trim().to_owned(),
            Some(_) => return None,
        };
        credential.insert(name.to_owned(), text);
    }
    let account_type = credential
        .get_mut("type")
        .expect("credential type field")
        .to_ascii_lowercase();
    *credential.get_mut("type").expect("credential type field") = if account_type.is_empty() {
        "codex".to_owned()
    } else {
        account_type
    };
    if credential.get("type").map(String::as_str) != Some("codex")
        || credential
            .get("access_token")
            .is_none_or(|value| value.is_empty())
        || credential
            .get("refresh_token")
            .is_none_or(|value| value.is_empty())
        || credential
            .get("expired")
            .is_none_or(|value| !valid_ccload_expired_text(value))
    {
        return None;
    }
    Some(credential)
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
    let login = remote_json(
        state,
        state
            .client
            .post(format!("{base}/api/v1/auth/login"))
            .json(&json!({"email": email, "password": password})),
    )
    .await?;
    let token = login
        .get("access_token")
        .and_then(Value::as_str)
        .or_else(|| {
            login
                .get("data")
                .and_then(|data| data.get("access_token"))
                .and_then(Value::as_str)
        })
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(ApiError::upstream)?;
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
    let name = required_name(&object)?;
    let base_url = normalized_remote_url(object.get("base_url"))?;
    object.insert("name".to_owned(), Value::String(name));
    object.insert("base_url".to_owned(), Value::String(base_url));
    if !sub2api_credentials_present(&object) {
        return Err(ApiError::invalid_request());
    }
    for key in ["email", "password", "api_key", "group_id"] {
        if let Some(value) = object.get(key).and_then(Value::as_str)
            && value.len() > 16 * 1024
        {
            return Err(ApiError::validation());
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
    let mut candidate = registry_value(&state, "sub2api", &server_id)?;
    for key in [
        "name", "base_url", "email", "password", "api_key", "group_id",
    ] {
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
    let request = state
        .client
        .get(format!("{base}/api/v1/admin/groups"))
        .query(&[("page", "1"), ("page_size", "5000")]);
    let value = remote_json(&state, sub2api_request(&state, &server, request).await?).await?;
    let groups = remote_array(&value, &["items", "groups", "data"]).ok_or_else(ApiError::upstream)?
        .into_iter()
        .take(5_000)
        .filter_map(|item| {
            let object = item.as_object()?;
            let id = bounded_public_text(object.get("id"), 128);
            if id.is_empty() {
                return None;
            }
            Some(json!({
                "id": id,
                "name": bounded_public_text(object.get("name"), 256),
                "description": bounded_public_text(object.get("description"), 256),
                "platform": bounded_public_text(object.get("platform"), 64),
                "status": bounded_public_text(object.get("status"), 64),
                "account_count": object.get("account_count").and_then(Value::as_u64).unwrap_or(0),
                "active_account_count": object.get("active_account_count").and_then(Value::as_u64).unwrap_or(0),
            }))
        })
        .collect::<Vec<_>>();
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
    let mut request = state
        .client
        .get(format!("{base}/api/v1/admin/accounts"))
        .query(&[
            ("platform", "openai"),
            ("type", "oauth"),
            ("page", "1"),
            ("page_size", "5000"),
        ]);
    if let Some(group) = server
        .get("group_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        request = request.query(&[("group", group)]);
    }
    let value = remote_json(&state, sub2api_request(&state, &server, request).await?).await?;
    let accounts = remote_array(&value, &["items", "accounts", "data"]).ok_or_else(ApiError::upstream)?
        .into_iter()
        .take(5_000)
        .filter_map(|item| {
            let object = item.as_object()?;
            let id = bounded_public_text(object.get("id"), 128);
            if id.is_empty() {
                return None;
            }
            let credentials = object.get("credentials").and_then(Value::as_object);
            Some(json!({
                "id": id,
                "name": bounded_public_text(object.get("name"), 256),
                "email": bounded_public_text(credentials.and_then(|value| value.get("email")), 256),
                "plan_type": bounded_public_text(credentials.and_then(|value| value.get("plan_type")), 64),
                "status": bounded_public_text(object.get("status"), 64),
                "expires_at": bounded_public_text(credentials.and_then(|value| value.get("expires_at")), 64),
                "has_refresh_token": credentials.and_then(|value| value.get("refresh_token")).and_then(Value::as_str).is_some_and(|value| !value.trim().is_empty()),
            }))
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({"server_id": server_id, "accounts": accounts})))
}

async fn execute_sub2api_import(
    state: AppState,
    server_id: String,
    ids: Vec<String>,
    expected_job_id: String,
) {
    let Ok(server) = registry_value(&state, "sub2api", &server_id) else {
        return;
    };
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
        Ok(request) => remote_json(&state, request).await,
        Err(error) => Err(error),
    };
    let mut errors = Vec::new();
    let mut successful = 0usize;
    let mut failed = 0usize;
    let mut imported = Vec::new();
    match result {
        Ok(value) => {
            let Some(accounts) = remote_array(&value, &["accounts", "data"]) else {
                errors.push(json!({"name": "Sub2API", "error": "远程账号响应无效"}));
                let job = import_job(&expected_job_id, ids.len(), 0, 0, 0, ids.len(), errors);
                let _ =
                    set_registry_job(&state, "sub2api", &server_id, job, Some(&expected_job_id));
                return;
            };
            for account in accounts {
                let object = account.as_object().cloned().unwrap_or_default();
                let credentials = object
                    .get("credentials")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                let token = credentials
                    .get("access_token")
                    .and_then(Value::as_str)
                    .or_else(|| credentials.get("accessToken").and_then(Value::as_str))
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned);
                match token {
                    Some(token) => {
                        imported.push(json!({"access_token": token, "source_type": "codex", "plan_type": bounded_public_text(credentials.get("plan_type"), 64)}));
                        successful += 1;
                    }
                    None => {
                        failed += 1;
                        errors.push(json!({"name": bounded_public_text(object.get("id"), 128), "error": "missing access_token"}));
                    }
                }
            }
        }
        Err(_) => {
            failed = ids.len();
            errors.push(json!({"name": "Sub2API", "error": "远程账号导入失败"}));
        }
    }
    failed = failed.saturating_add(ids.len().saturating_sub(successful.saturating_add(failed)));
    let (added, skipped, failed) = match state.account_store.merge_import_records(imported).await {
        Ok((added, skipped)) => (added, skipped, failed),
        Err(_) => {
            errors.push(json!({"name": "accounts", "error": "账号快照写入失败"}));
            (0, 0, failed.saturating_add(successful))
        }
    };
    let job = import_job(
        &expected_job_id,
        ids.len(),
        added,
        skipped,
        0,
        failed,
        errors,
    );
    let _ = set_registry_job(&state, "sub2api", &server_id, job, Some(&expected_job_id));
}

pub(super) async fn start_sub2api_import(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(server_id): AxumPath<String>,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    registry_value(&state, "sub2api", &server_id)?;
    let ids = super::account_json_body(body)
        .await?
        .get("account_ids")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(ApiError::invalid_request)?
        .into_iter()
        .filter_map(|value| {
            value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
        .collect::<Vec<_>>();
    if ids.is_empty() || ids.len() > 5_000 {
        return Err(ApiError::invalid_request());
    }
    let job_id = format!("job-{}-{}", std::process::id(), now_nanos());
    let job = json!({
        "job_id": job_id.clone(),
        "status": "pending",
        "created_at": iso_timestamp(SystemTime::now()),
        "updated_at": iso_timestamp(SystemTime::now()),
        "total": ids.len(),
        "completed": 0,
        "added": 0,
        "skipped": 0,
        "refreshed": 0,
        "failed": 0,
        "errors": [],
    });
    let saved = begin_registry_job(&state, "sub2api", &server_id, job)?;
    tokio::spawn(execute_sub2api_import(state, server_id, ids, job_id));
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
                "models_loaded": !enabled,
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
    let catalogs = tokio::time::timeout(
        CCLOAD_CHANNEL_MODEL_DEADLINE,
        load_ccload_channel_models(&state, &server_id, &server, ids),
    )
    .await
    .map_err(|_| ApiError::upstream())??;
    Ok(Json(json!({"server_id": server_id, "channels": catalogs})))
}

async fn load_ccload_channel_models(
    state: &AppState,
    _server_id: &str,
    server: &Map<String, Value>,
    ids: Vec<String>,
) -> Result<Vec<Value>, ApiError> {
    let (base, token) = ccload_login(state, server).await?;
    let mut catalogs = Vec::with_capacity(ids.len());
    let mut model_tokens = Vec::new();

    for id in ids {
        let catalog_index = catalogs.len();
        let mut catalog = json!({"id": id, "plan_type": "", "models": [], "models_loaded": false});
        let editor = remote_json(
            state,
            state
                .client
                .get(format!("{base}/admin/channels/{id}/editor"))
                .bearer_auth(&token),
        )
        .await;
        if let Ok(editor) = editor {
            let channel = editor.get("data").and_then(|value| value.get("channel"));
            let channel_matches = channel
                .and_then(|value| clean_ccload_channel_id(value.get("id")))
                .is_some_and(|value| value == id)
                && channel
                    .and_then(|value| value.get("auth_type"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    == Some("codex_oauth");
            if channel_matches
                && let Some(credential) = normalized_ccload_credential(
                    editor
                        .get("data")
                        .and_then(|value| value.get("oauth_credential")),
                )
            {
                catalog["plan_type"] = Value::String(
                    credential
                        .get("plan_type")
                        .filter(|value| value.chars().count() <= 256)
                        .cloned()
                        .unwrap_or_default(),
                );
                model_tokens.push((
                    catalog_index,
                    credential
                        .get("access_token")
                        .cloned()
                        .expect("validated ccLoad access token"),
                ));
            }
        }
        catalogs.push(catalog);
    }

    let Some(model_base) = state
        .config
        .upstream_base_url
        .clone()
        .or_else(|| Some("https://chatgpt.com".to_owned()))
    else {
        return Ok(catalogs);
    };
    let mut requests = FuturesUnordered::new();
    for (catalog_index, access) in model_tokens {
        let request_state = state.clone();
        let model_base = model_base.clone();
        requests.push(async move {
            let models = remote_json(
                &request_state,
                request_state
                    .client
                    .get(format!(
                        "{}/backend-api/models?history_and_training_disabled=false",
                        model_base.trim_end_matches('/')
                    ))
                    .bearer_auth(access)
                    .header("X-OpenAI-Target-Route", "/backend-api/models"),
            )
            .await;
            (catalog_index, models)
        });
    }
    while let Some((catalog_index, result)) = requests.next().await {
        let Ok(models) = result else {
            continue;
        };
        let Some(items) = models.get("data").and_then(Value::as_array) else {
            continue;
        };
        let mut seen = HashSet::new();
        let model_ids = items
            .iter()
            .filter_map(|item| item.get("id").and_then(Value::as_str))
            .map(str::trim)
            .filter(|value| !value.is_empty() && seen.insert((*value).to_owned()))
            .take(5_000)
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        catalogs[catalog_index]["models"] = json!(model_ids);
        catalogs[catalog_index]["models_loaded"] = Value::Bool(true);
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
        "created_at": iso_timestamp(SystemTime::now()),
        "updated_at": iso_timestamp(SystemTime::now()),
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
                            added: usize,
                            skipped: usize,
                            refreshed: usize,
                            failed: usize,
                            status: &str,
                            errors: &[Value]| {
        let job = progress_job_with_created(
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
        );
        let _ = set_registry_job(&state, "ccload", &server_id, job, Some(&expected_job_id));
    };
    publish_progress(0, 0, 0, 0, 0, "running", &[]);
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
                    && let Some(credential) = normalized_ccload_credential(credential)
                {
                    let mut candidate = json!({
                        "access_token": credential
                            .get("access_token")
                            .expect("validated ccLoad access token"),
                        "refresh_token": credential
                            .get("refresh_token")
                            .expect("validated ccLoad refresh token"),
                        "source_type": "codex",
                        "type": "codex",
                        "plan_type": credential
                            .get("plan_type")
                            .filter(|value| value.chars().count() <= 256)
                            .cloned()
                            .unwrap_or_default(),
                    });
                    for key in ["id_token", "account_id", "email", "expired"] {
                        if let Some(value) = credential.get(key).filter(|value| !value.is_empty()) {
                            candidate[key] = Value::String(value.clone());
                        }
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
        publish_progress(index.saturating_add(1), 0, 0, 0, failed, "running", &errors);
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
        added,
        skipped,
        0,
        fetch_failed,
        "running",
        &errors,
    );

    let mut refresh_jobs = FuturesUnordered::new();
    for candidate in candidates {
        let refresh_state = state.clone();
        let old_token = candidate
            .get("access_token")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_default();
        refresh_jobs.push(async move {
            let remaining = deadline
                .checked_duration_since(std::time::Instant::now())
                .unwrap_or_default();
            if old_token.is_empty() || remaining.is_zero() {
                return (old_token, Err("timeout"));
            }
            let result = tokio::time::timeout(
                remaining,
                super::refresh_oauth_account(&refresh_state, &candidate),
            )
            .await;
            (
                old_token,
                match result {
                    Ok(result) => result,
                    Err(_) => Err("timeout"),
                },
            )
        });
    }

    let mut refreshed = 0usize;
    let mut refresh_failed = 0usize;
    while let Some((old_token, result)) = refresh_jobs.next().await {
        match result {
            Ok(updated) => match state
                .account_store
                .update_refreshed_account(&old_token, updated)
                .await
            {
                Ok(true) => refreshed += 1,
                Ok(false) | Err(_) => {
                    refresh_failed += 1;
                    push_import_error(
                        &mut errors,
                        json!({"name": "ccLoad", "error": "刷新账号写入失败"}),
                    );
                }
            },
            Err(code) => {
                refresh_failed += 1;
                let _ = state
                    .account_store
                    .mark_refresh_failed(&old_token, code)
                    .await;
                push_import_error(
                    &mut errors,
                    if code == "timeout" {
                        json!({"name": "ccLoad", "error": "刷新阶段超时"})
                    } else {
                        json!({"name": "ccLoad", "error": format!("刷新失败: {code}")})
                    },
                );
            }
        }
        publish_progress(
            ids.len(),
            added,
            skipped,
            refreshed,
            fetch_failed.saturating_add(refresh_failed),
            "running",
            &errors,
        );
    }
    let failed = fetch_failed.saturating_add(refresh_failed).min(ids.len());
    let status = if failed > 0 { "failed" } else { "completed" };
    let job = progress_job_with_created(
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

pub(super) async fn test_image_storage(
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
    if !enabled || mode == "local" {
        return Ok(Json(
            json!({"result": {"ok": true, "status": 200, "error": null, "backend": "local"}}),
        ));
    }
    let url = settings
        .get("webdav_url")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();
    if url.is_empty() {
        return Ok(Json(
            json!({"result": {"ok": false, "status": 0, "error": "WebDAV 未配置"}}),
        ));
    }
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|_| ApiError::unavailable())?;
    let request = client.head(url);
    let request = if let (Some(user), Some(password)) = (
        settings.get("webdav_username").and_then(Value::as_str),
        settings.get("webdav_password").and_then(Value::as_str),
    ) {
        request.basic_auth(user, Some(password))
    } else {
        request
    };
    let response = request.send().await.map_err(|_| ApiError::unavailable())?;
    let status = response.status().as_u16();
    Ok(Json(json!({"result": {
        "ok": status < 400,
        "status": status,
        "error": if status < 400 { Value::Null } else { json!("WebDAV 测试失败") },
    }})))
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
    if enabled && matches!(mode, "webdav" | "both") {
        return Err(ApiError::unavailable());
    }
    let skipped = image_files(&state).len();
    Ok(Json(
        json!({"result": {"uploaded": 0, "skipped": skipped, "failed": 0}}),
    ))
}

fn backup_dir(state: &AppState) -> PathBuf {
    data_file(state, "backups")
}

fn backup_state_path(state: &AppState) -> PathBuf {
    data_file(state, "backup_state.json")
}

fn backup_settings(state: &AppState) -> Value {
    let raw = object_or_empty(
        read_config(state)
            .get("backup")
            .cloned()
            .unwrap_or_else(|| json!({})),
    );
    let include_raw = object_or_empty(raw.get("include").cloned().unwrap_or_else(|| json!({})));
    json!({
        "enabled": bool_or(raw.get("enabled"), false),
        "provider": raw.get("provider").and_then(Value::as_str).unwrap_or("local"),
        "account_id": raw.get("account_id").and_then(Value::as_str).unwrap_or(""),
        "access_key_id": raw.get("access_key_id").and_then(Value::as_str).unwrap_or(""),
        "secret_access_key": secret_mask(raw.get("secret_access_key")),
        "bucket": raw.get("bucket").and_then(Value::as_str).unwrap_or(""),
        "prefix": raw.get("prefix").and_then(Value::as_str).filter(|value| !value.is_empty()).unwrap_or("backups"),
        "interval_minutes": raw.get("interval_minutes").and_then(Value::as_u64).unwrap_or(360),
        "rotation_keep": raw.get("rotation_keep").and_then(Value::as_u64).unwrap_or(10),
        "encrypt": bool_or(raw.get("encrypt"), false),
        "passphrase": secret_mask(raw.get("passphrase")),
        "include": {
            "config": bool_or(include_raw.get("config"), true),
            "cpa": bool_or(include_raw.get("cpa"), true),
            "sub2api": bool_or(include_raw.get("sub2api"), true),
            "ccload": bool_or(include_raw.get("ccload"), true),
            "logs": bool_or(include_raw.get("logs"), true),
            "image_tasks": bool_or(include_raw.get("image_tasks"), true),
            "accounts_snapshot": bool_or(include_raw.get("accounts_snapshot"), true),
            "auth_keys_snapshot": bool_or(include_raw.get("auth_keys_snapshot"), true),
            "images": bool_or(include_raw.get("images"), false),
        }
    })
}

fn backup_is_remote(state: &AppState) -> bool {
    backup_settings(state)
        .get("provider")
        .and_then(Value::as_str)
        .is_some_and(|value| value.eq_ignore_ascii_case("cloudflare_r2"))
}

fn backup_state(state: &AppState) -> Value {
    fs::read(backup_state_path(state))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .filter(Value::is_object)
        .unwrap_or_else(|| {
            json!({
                "running": false,
                "last_started_at": null,
                "last_finished_at": null,
                "last_status": "idle",
                "last_error": null,
                "last_object_key": null,
            })
        })
}

fn backup_key_file(state: &AppState, key: &str) -> Result<PathBuf, ApiError> {
    let key = key.trim().replace('\\', "/");
    let Some(name) = key.strip_prefix("backups/") else {
        return Err(ApiError::invalid_request());
    };
    if name.is_empty()
        || name.contains('/')
        || name.contains("..")
        || !name.starts_with("backup-")
        || !name.ends_with(".tar.gz")
    {
        return Err(ApiError::invalid_request());
    }
    Ok(backup_dir(state).join(name))
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

fn build_backup(state: &AppState, key: &str) -> Result<Vec<u8>, ApiError> {
    let settings = object_or_empty(backup_settings(state));
    let include = object_or_empty(
        settings
            .get("include")
            .cloned()
            .unwrap_or_else(|| json!({})),
    );
    let metadata = json!({
        "version": 1,
        "created_at": iso_timestamp(SystemTime::now()),
        "trigger": "manual",
        "app_version": state.config.version,
        "storage_backend": {"type": "local"},
        "sensitive_snapshots_excluded": true,
        "object_key": key,
    });
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
        ("cpa", "cpa_config.json", "data/cpa_config.json"),
        ("sub2api", "sub2api_config.json", "data/sub2api_config.json"),
        ("ccload", "ccload_config.json", "data/ccload_config.json"),
        ("images", "image_tags.json", "data/image_tags.json"),
    ];
    for (flag, filename, archive_name) in optional_files {
        if bool_or(include.get(flag), flag != "images") {
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
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("gz") {
            continue;
        }
        let metadata = entry.metadata().map_err(|_| ApiError::unavailable())?;
        let name = entry.file_name().to_string_lossy().into_owned();
        items.push(json!({
            "key": format!("backups/{name}"),
            "name": name,
            "size": metadata.len(),
            "updated_at": metadata.modified().ok().map(iso_timestamp),
            "encrypted": false,
        }));
    }
    items.sort_by(|left, right| right["name"].as_str().cmp(&left["name"].as_str()));
    Ok(items)
}

pub(super) async fn test_backup(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    if backup_is_remote(&state) {
        return Err(ApiError::unavailable());
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
    if backup_is_remote(&state) {
        return Err(ApiError::unavailable());
    }
    Ok(Json(json!({
        "items": backup_items(&state)?,
        "state": backup_state(&state),
        "settings": backup_settings(&state),
    })))
}

pub(super) async fn run_backup(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    if backup_is_remote(&state) {
        return Err(ApiError::unavailable());
    }
    let key = format!(
        "backups/backup-{}-{}.tar.gz",
        unix_seconds(SystemTime::now()),
        now_nanos()
    );
    let started = iso_timestamp(SystemTime::now());
    write_json(
        &backup_state_path(&state),
        &json!({"running": true, "last_started_at": started, "last_status": "running"}),
    )?;
    let result = build_backup(&state, &key);
    match result {
        Ok(payload) => {
            let path = backup_key_file(&state, &key)?;
            write_atomic(&path, &payload)?;
            write_json(
                &backup_state_path(&state),
                &json!({
                    "running": false,
                    "last_started_at": started,
                    "last_finished_at": iso_timestamp(SystemTime::now()),
                    "last_status": "success",
                    "last_error": null,
                    "last_object_key": key,
                }),
            )?;
            Ok(Json(
                json!({"result": {"key": key, "size": payload.len(), "encrypted": false}}),
            ))
        }
        Err(error) => {
            let _ = write_json(
                &backup_state_path(&state),
                &json!({"running": false, "last_status": "error", "last_error": "备份执行失败，请稍后重试"}),
            );
            Err(error)
        }
    }
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

fn read_backup_detail(state: &AppState, key: &str) -> Result<Value, ApiError> {
    let path = backup_key_file(state, key)?;
    let payload = read_bounded(&path, MAX_BACKUP_BYTES)?;
    let decoder = GzDecoder::new(Cursor::new(payload));
    let mut archive = Archive::new(decoder);
    let mut files = Vec::new();
    let mut snapshots = BTreeMap::<String, usize>::new();
    let entries = archive.entries().map_err(|_| ApiError::invalid_request())?;
    for entry in entries {
        let entry = entry.map_err(|_| ApiError::invalid_request())?;
        let name = entry
            .path()
            .map_err(|_| ApiError::invalid_request())?
            .to_string_lossy()
            .replace('\\', "/");
        if name.contains("..") || name.starts_with('/') {
            return Err(ApiError::invalid_request());
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
        let digest = Sha256::digest(&bytes);
        let hash = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        if let Some(snapshot) = name.strip_prefix("snapshots/") {
            *snapshots.entry(snapshot.to_owned()).or_default() += 1;
        }
        files.push(json!({
            "name": name,
            "exists": true,
            "content_type": content_type(&name),
            "size": bytes.len(),
            "sha256": hash,
        }));
    }
    Ok(json!({
        "key": key,
        "name": key.rsplit('/').next().unwrap_or("backup.tar.gz"),
        "encrypted": false,
        "created_at": null,
        "trigger": "manual",
        "app_version": null,
        "storage_backend": {"type": "local"},
        "files": files,
        "snapshots": snapshots.into_iter().map(|(name, count)| json!({"name": name, "count": count})).collect::<Vec<_>>(),
    }))
}

pub(super) async fn delete_backup(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    let value = super::account_json_body(body).await?;
    let key = value
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::invalid_request)?;
    let path = backup_key_file(&state, key)?;
    if fs::remove_file(path).is_err() {
        return Err(ApiError::not_found());
    }
    Ok(Json(json!({"ok": true})))
}

pub(super) async fn backup_detail(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<BackupKeyQuery>,
) -> Result<Json<Value>, ApiError> {
    admin_authenticated(&headers, &state).await?;
    if backup_is_remote(&state) {
        return Err(ApiError::unavailable());
    }
    let key = query.key.as_deref().ok_or_else(ApiError::invalid_request)?;
    Ok(Json(json!({"item": read_backup_detail(&state, key)?})))
}

pub(super) async fn download_backup(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<BackupKeyQuery>,
) -> Result<Response, ApiError> {
    admin_authenticated(&headers, &state).await?;
    if backup_is_remote(&state) {
        return Err(ApiError::unavailable());
    }
    let key = query.key.as_deref().ok_or_else(ApiError::invalid_request)?;
    let path = backup_key_file(&state, key)?;
    let payload = read_bounded(&path, MAX_BACKUP_BYTES)?;
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("backup.tar.gz");
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
