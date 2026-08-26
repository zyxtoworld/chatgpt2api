use std::{
    collections::{HashMap, HashSet},
    fs,
    future::Future,
    io,
    path::Path,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use std::path::PathBuf;
#[cfg(test)]
use std::sync::Mutex as StdMutex;

use axum::{
    Json,
    body::{Body, to_bytes},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use file_identity::{DirectoryHandle, open_directory, open_or_create_directory};
use futures_util::StreamExt;
use reqwest::{RequestBuilder, Url};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};

use super::{
    AccountLease, AccountModelGroup, AccountStore, ApiError, AppState,
    MAX_EDITABLE_TASK_FILE_BYTES, MAX_EDITABLE_TASKS, MAX_REQUEST_BODY_BYTES, NativeRequestContext,
    account_pool::current_timestamp, acquire_path_write_lock, acquire_path_write_lock_sync,
    atomic_replace_checked_with_limit, authenticated_subject, bounded_response_body,
    editable_capability_digest, editable_file_tasks_path, editable_relative_path,
    native_bootstrap_with_timeout_context, native_browser_headers_with_referer,
    native_chat_requirements_with_resources_for_route_context, native_created, native_image_input,
    native_message_id, read_editable_task_records, read_editable_task_records_at,
    search_conversation_id_from_response, valid_editable_task_id,
};

const MAX_EDITABLE_REFERENCE_IMAGES: usize = 16;
const EDITABLE_TASK_CONCURRENCY: usize = 8;
const EDITABLE_MODEL: &str = "gpt-5-5-thinking";
const EDITABLE_THINKING_EFFORT: &str = "extended";
const EDITABLE_CLIENT_VERSION: &str = "prod-bede35f9dcd856d080e012478f0c1031faa2588e";
const EDITABLE_CLIENT_BUILD_NUMBER: &str = "6631702";
const EDITABLE_TIMEOUT: Duration = Duration::from_secs(1_200);
const EDITABLE_POLL_INTERVAL: Duration = Duration::from_secs(5);
const MAX_EDITABLE_IMAGE_BYTES: usize = 10 * 1024 * 1024;
const MAX_EDITABLE_ARTIFACT_BYTES: usize = 100 * 1024 * 1024;
const MAX_EDITABLE_ARTIFACT_NODES: usize = 100_000;
const MAX_EDITABLE_ARTIFACT_DEPTH: usize = 128;
const GENERIC_TASK_ERROR: &str = "editable file task failed";
const PSD_IMAGE_REQUIRED_ERROR: &str = "PSD 任务需要至少一张图片";
const RESTART_INTERRUPTED_ERROR: &str = "服务已重启，未完成的任务已中断";
const EDITABLE_PPT_PROMPT: &str = "我需要你根据用户的需求，来制作一个可以编辑的PPT，你可以使用Agent来做，你不要再继续询问用户问题，内容风格、版式、配色、内容结构和页面信息你可以自行补充并直接执行。整体的流程如下：\n1. 用生图的方式，帮我生成一个精美的产品介绍ppt，5-6个页面\n2. 帮我把以上涉及到的所有图像和形状素材拆分成单独png，每个素材单独一张图片，不要有遗漏，让我可以直接在ppt里拼接素材还原，不要文字\n3. 利用以上所有图片和形状素材，帮我还原你第一次生成的展示ppt，我需要是可编辑的ppt格式，主要部分需要你单独还原插入，文字需要可以编辑\n最后只需要给我生成一个PPT文件，以及生成中遇到的各种素材压缩包zip文件就行。";
const EDITABLE_PSD_PROMPT: &str = "帮我生成这个图像，把这张海报分成若干图像，包括背景图，每个元素不要改位置，这样子我可以直接在 平时里无需拖动，底色为白色，不要伪透明底。再帮我将以上拆分的图像拼合成一个psd文件，去除白色底，不要改变每个图层的相应位置，保留每个元素所在图层的相应位置，保留每个元素的图层，最后只需要给我输出psd文件，以及每个图层的zip文件";
static EDITABLE_TASK_SEMAPHORE: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(EDITABLE_TASK_CONCURRENCY)));
#[cfg(test)]
static FORCE_NEXT_EDITABLE_ADMISSION_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
struct RecoveryWriteFailureDiagnostic {
    target_path: PathBuf,
    token: String,
    arm_thread: String,
}

#[cfg(test)]
static RECOVERY_WRITE_FAILURE_HOOKS: LazyLock<
    StdMutex<HashMap<std::path::PathBuf, RecoveryWriteFailureDiagnostic>>,
> = LazyLock::new(|| StdMutex::new(HashMap::new()));

#[cfg(test)]
pub(super) struct RecoveryWriteFailureGuard {
    target_path: PathBuf,
    token: String,
}

#[cfg(test)]
impl Drop for RecoveryWriteFailureGuard {
    fn drop(&mut self) {
        let mut hooks = RECOVERY_WRITE_FAILURE_HOOKS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if hooks
            .get(&self.target_path)
            .is_some_and(|value| value.token == self.token)
        {
            hooks.remove(&self.target_path);
        }
    }
}

pub(super) struct EditableWorkers {
    accepting: AtomicBool,
    active: AtomicUsize,
    handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    idle: Notify,
    #[cfg(test)]
    completion_barrier: StdMutex<Option<(Arc<Notify>, Arc<Notify>)>>,
}

impl EditableWorkers {
    pub(super) fn new() -> Self {
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        Self {
            accepting: AtomicBool::new(true),
            active: AtomicUsize::new(0),
            handles: Mutex::new(Vec::new()),
            shutdown_tx,
            idle: Notify::new(),
            #[cfg(test)]
            completion_barrier: StdMutex::new(None),
        }
    }

    pub(super) async fn spawn<F>(self: &Arc<Self>, future: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut handles = self.handles.lock().await;
        handles.retain(|handle| !handle.is_finished());
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        if !self.accepting.load(Ordering::Acquire) {
            return false;
        }
        self.active.fetch_add(1, Ordering::AcqRel);
        let workers = self.clone();
        let handle = tokio::spawn(async move {
            let _completion = EditableWorkerCompletion { workers };
            if *shutdown_rx.borrow() {
                return;
            }
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {}
                () = future => {}
            }
        });
        handles.push(handle);
        true
    }

    pub(super) fn begin_shutdown(&self) {
        self.accepting.store(false, Ordering::Release);
        self.shutdown_tx.send_replace(true);
    }

    pub(super) async fn finish_shutdown(&self) {
        let handles = {
            let mut handles = self.handles.lock().await;
            std::mem::take(&mut *handles)
        };
        for handle in handles {
            let _ = handle.await;
        }
        self.wait_for_idle().await;
    }

    #[cfg(test)]
    pub(super) async fn shutdown(&self) {
        self.begin_shutdown();
        self.finish_shutdown().await;
    }

    pub(super) async fn wait_for_idle(&self) {
        loop {
            let idle = self.idle.notified();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }

    #[cfg(test)]
    pub(super) fn active_for_test(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn hold_next_completion_for_test(&self) -> (Arc<Notify>, Arc<Notify>) {
        let reached = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut barrier = self
            .completion_barrier
            .lock()
            .expect("editable worker completion barrier");
        assert!(barrier.is_none(), "editable completion barrier already set");
        *barrier = Some((reached.clone(), release.clone()));
        (reached, release)
    }

    #[cfg(test)]
    async fn wait_at_completion_barrier_for_test(&self) {
        let barrier = self
            .completion_barrier
            .lock()
            .expect("editable worker completion barrier")
            .take();
        if let Some((reached, release)) = barrier {
            reached.notify_one();
            release.notified().await;
        }
    }
}

struct EditableWorkerCompletion {
    workers: Arc<EditableWorkers>,
}

impl Drop for EditableWorkerCompletion {
    fn drop(&mut self) {
        if self.workers.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.workers.idle.notify_waiters();
        }
    }
}

struct EditableRequest {
    task_id: String,
    prompt: String,
    images: Vec<String>,
}

struct EditableUpload {
    file_id: String,
    library_file_id: String,
    file_name: String,
    file_size: usize,
    mime_type: String,
    width: u32,
    height: u32,
}

#[derive(Clone)]
struct EditableArtifact {
    id: String,
    name: String,
    mime_type: String,
    sandbox_path: String,
    message_id: String,
    created_at: f64,
}

struct EditableExport {
    conversation_id: String,
    primary_name: String,
    zip_name: String,
}

struct EditableUpstream<'a> {
    state: &'a AppState,
    lease: &'a AccountLease,
    context: &'a NativeRequestContext,
    base_url: &'a str,
    deadline: Instant,
}

enum TaskFailure {
    PsdImageRequired,
    Generic,
}

impl TaskFailure {
    fn public_message(&self) -> &'static str {
        match self {
            Self::PsdImageRequired => PSD_IMAGE_REQUIRED_ERROR,
            Self::Generic => GENERIC_TASK_ERROR,
        }
    }
}

fn current_task_time() -> (String, f64) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let seconds = now.as_secs_f64();
    (current_timestamp(), seconds)
}

fn optional_timestamp(object: &Map<String, Value>, field: &str) -> Result<Option<f64>, ApiError> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_f64()
            .filter(|value| value.is_finite() && *value >= 0.0)
            .map(Some)
            .ok_or_else(ApiError::unavailable),
    }
}

fn persisted_error(value: Option<&Value>) -> Result<Option<String>, ApiError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(Value::String(value))
            if matches!(
                value.as_str(),
                GENERIC_TASK_ERROR | PSD_IMAGE_REQUIRED_ERROR | RESTART_INTERRUPTED_ERROR
            ) =>
        {
            Ok(Some(value.clone()))
        }
        Some(_) => Err(ApiError::unavailable()),
    }
}

pub(super) fn normalize_persisted_task(record: &Value) -> Result<Value, ApiError> {
    let object = record.as_object().ok_or_else(ApiError::unavailable)?;
    let id = object
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| valid_editable_task_id(value))
        .ok_or_else(ApiError::unavailable)?;
    let owner_id = object
        .get("owner_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.chars().count() <= 256)
        .ok_or_else(ApiError::unavailable)?;
    let status = match object.get("status") {
        None | Some(Value::Null) => "error",
        Some(Value::String(value)) if value.is_empty() => "error",
        Some(Value::String(value))
            if matches!(value.as_str(), "queued" | "running" | "success" | "error") =>
        {
            value
        }
        Some(_) => return Err(ApiError::unavailable()),
    };
    let kind = match object.get("kind") {
        None | Some(Value::Null) => "ppt",
        Some(Value::String(value)) if value.is_empty() => "ppt",
        Some(Value::String(value)) if matches!(value.as_str(), "ppt" | "psd") => value,
        Some(_) => return Err(ApiError::unavailable()),
    };
    let model = match object.get("model") {
        None | Some(Value::Null) => EDITABLE_MODEL,
        Some(Value::String(value)) if !value.trim().is_empty() && value.chars().count() <= 256 => {
            value
        }
        Some(_) => return Err(ApiError::unavailable()),
    };
    let created_at = object
        .get("created_at")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty() && value.chars().count() <= 128)
        .ok_or_else(ApiError::unavailable)?;
    let updated_at = object
        .get("updated_at")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty() && value.chars().count() <= 128)
        .ok_or_else(ApiError::unavailable)?;
    let created_ts = optional_timestamp(object, "created_ts")?.unwrap_or_default();
    let updated_ts = optional_timestamp(object, "updated_ts")?.unwrap_or_default();

    let mut normalized = Map::new();
    normalized.insert("id".to_owned(), Value::String(id.to_owned()));
    normalized.insert("owner_id".to_owned(), Value::String(owner_id.to_owned()));
    normalized.insert("status".to_owned(), Value::String(status.to_owned()));
    normalized.insert("kind".to_owned(), Value::String(kind.to_owned()));
    normalized.insert("model".to_owned(), Value::String(model.to_owned()));
    normalized.insert(
        "created_at".to_owned(),
        Value::String(created_at.to_owned()),
    );
    normalized.insert(
        "updated_at".to_owned(),
        Value::String(updated_at.to_owned()),
    );
    normalized.insert("created_ts".to_owned(), json!(created_ts));
    normalized.insert("updated_ts".to_owned(), json!(updated_ts));
    for field in ["started_ts", "ended_ts"] {
        if let Some(value) = optional_timestamp(object, field)?.filter(|value| *value > 0.0) {
            normalized.insert(field.to_owned(), json!(value));
        }
    }

    if let Some(raw_result) = object.get("result") {
        let result = raw_result.as_object().ok_or_else(ApiError::unavailable)?;
        if result.keys().any(|field| {
            !matches!(
                field.as_str(),
                "conversation_id" | "primary_url" | "zip_url"
            )
        }) {
            return Err(ApiError::unavailable());
        }
        let mut projected = Map::new();
        for field in ["conversation_id", "primary_url", "zip_url"] {
            match result.get(field) {
                None | Some(Value::Null) => {}
                Some(Value::String(value))
                    if value.chars().count() <= 4096 && !value.chars().any(char::is_control) =>
                {
                    projected.insert(field.to_owned(), Value::String(value.clone()));
                }
                Some(_) => return Err(ApiError::unavailable()),
            }
        }
        if !projected.is_empty() {
            normalized.insert("result".to_owned(), Value::Object(projected));
        }
    }
    if let Some(error) = persisted_error(object.get("error"))? {
        normalized.insert("error".to_owned(), Value::String(error));
    }
    if let Some(raw_hashes) = object.get("download_capability_hashes") {
        let hashes = raw_hashes.as_object().ok_or_else(ApiError::unavailable)?;
        let mut projected = Map::new();
        for (relative_path, digest) in hashes {
            if editable_relative_path(relative_path).as_deref() != Some(relative_path.as_str())
                || !digest.as_str().is_some_and(|value| {
                    value.len() == 64
                        && value
                            .bytes()
                            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
                })
            {
                return Err(ApiError::unavailable());
            }
            projected.insert(relative_path.clone(), digest.clone());
        }
        if !projected.is_empty() {
            normalized.insert(
                "download_capability_hashes".to_owned(),
                Value::Object(projected),
            );
        }
    }
    Ok(Value::Object(normalized))
}

fn encoded_tasks(tasks: &[Value]) -> Result<Vec<u8>, ApiError> {
    if tasks.len() > MAX_EDITABLE_TASKS {
        return Err(ApiError::unavailable());
    }
    let mut bytes =
        serde_json::to_vec_pretty(&json!({"tasks": tasks})).map_err(|_| ApiError::unavailable())?;
    bytes.push(b'\n');
    if bytes.len() as u64 > MAX_EDITABLE_TASK_FILE_BYTES {
        return Err(ApiError::unavailable());
    }
    Ok(bytes)
}

pub(super) fn recover_unfinished(data_dir: &Path) -> Result<(), ApiError> {
    let path = data_dir.join("editable_file_tasks.json");
    match fs::symlink_metadata(&path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(ApiError::unavailable()),
    }
    let _file_lock = acquire_path_write_lock_sync(&path)?;
    let records = read_editable_task_records_at(&path)?;
    let mut tasks = records
        .into_iter()
        .map(|value| {
            value
                .get("record")
                .cloned()
                .ok_or_else(ApiError::unavailable)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (updated_at, updated_ts) = current_task_time();
    let mut changed = false;
    for task in tasks.iter_mut().filter_map(Value::as_object_mut) {
        if task
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| matches!(status, "queued" | "running"))
        {
            task.insert("status".to_owned(), Value::String("error".to_owned()));
            task.insert(
                "error".to_owned(),
                Value::String(RESTART_INTERRUPTED_ERROR.to_owned()),
            );
            task.insert("ended_ts".to_owned(), json!(updated_ts));
            task.insert("updated_at".to_owned(), Value::String(updated_at.clone()));
            task.insert("updated_ts".to_owned(), json!(updated_ts));
            changed = true;
        }
    }
    if changed {
        let bytes = encoded_tasks(&tasks)?;
        #[cfg(test)]
        if let Some(diagnostic) = RECOVERY_WRITE_FAILURE_HOOKS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&path)
        {
            eprintln!(
                "editable_recovery_hook consume token={} target_path={} actual_path={} arm_thread={} consume_thread={:?}",
                diagnostic.token,
                diagnostic.target_path.display(),
                path.display(),
                diagnostic.arm_thread,
                std::thread::current().id(),
            );
            return Err(ApiError::unavailable());
        }
        atomic_replace_checked_with_limit(&path, &bytes, MAX_EDITABLE_TASK_FILE_BYTES, false)?;
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn fail_next_recovery_write_for_test(
    target_path: &Path,
    token: &str,
) -> RecoveryWriteFailureGuard {
    assert!(
        target_path.is_absolute(),
        "recovery test path must be absolute"
    );
    let target_path = target_path.to_owned();
    let token = token.to_owned();
    let previous = RECOVERY_WRITE_FAILURE_HOOKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(
            target_path.clone(),
            RecoveryWriteFailureDiagnostic {
                target_path: target_path.clone(),
                token: token.clone(),
                arm_thread: format!("{:?}", std::thread::current().id()),
            },
        );
    assert!(
        previous.is_none(),
        "duplicate recovery failure hook for the same task path"
    );
    eprintln!(
        "editable_recovery_hook arm token={} target_path={} arm_thread={:?}",
        token,
        target_path.display(),
        std::thread::current().id(),
    );
    RecoveryWriteFailureGuard { target_path, token }
}

async fn parse_request(body: Body) -> Result<EditableRequest, ApiError> {
    let bytes = to_bytes(body, MAX_REQUEST_BODY_BYTES)
        .await
        .map_err(|_| ApiError::validation())?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| ApiError::validation())?;
    let object = value.as_object().ok_or_else(ApiError::validation)?;

    let prompt = match object.get("prompt") {
        None => String::new(),
        Some(Value::String(value)) => value.clone(),
        Some(_) => return Err(ApiError::validation()),
    };
    let images = match object.get("base64_images") {
        None => Vec::new(),
        Some(Value::Array(values)) if values.len() <= MAX_EDITABLE_REFERENCE_IMAGES => values
            .iter()
            .map(|value| value.as_str().map(ToOwned::to_owned))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(ApiError::validation)?,
        Some(_) => return Err(ApiError::validation()),
    };
    let task_id = match object.get("client_task_id") {
        None | Some(Value::Null) => native_message_id(),
        Some(Value::String(value)) => {
            let value = value.trim();
            if value.is_empty() || value.chars().count() > 256 || value.contains(',') {
                return Err(ApiError::validation());
            }
            value.to_owned()
        }
        Some(_) => return Err(ApiError::validation()),
    };
    if !valid_editable_task_id(&task_id) {
        return Err(ApiError::invalid_request());
    }
    Ok(EditableRequest {
        task_id,
        prompt,
        images,
    })
}

fn public_task(task: &Map<String, Value>) -> Value {
    let start = task
        .get("started_ts")
        .or_else(|| task.get("created_ts"))
        .and_then(Value::as_f64)
        .unwrap_or_default();
    let end = task
        .get("ended_ts")
        .and_then(Value::as_f64)
        .unwrap_or_else(|| current_task_time().1);
    let mut public = Map::new();
    for field in ["id", "status", "kind", "created_at", "updated_at"] {
        if let Some(value) = task.get(field) {
            public.insert(field.to_owned(), value.clone());
        }
    }
    if let Some(id) = task.get("id") {
        public.insert("taskId".to_owned(), id.clone());
    }
    public.insert(
        "elapsed_seconds".to_owned(),
        json!(if start > 0.0 {
            (end - start).max(0.0).floor() as i64
        } else {
            0
        }),
    );
    for field in ["result", "error"] {
        if let Some(value) = task.get(field)
            && !value.is_null()
            && value.as_str().is_none_or(|value| !value.is_empty())
        {
            public.insert(field.to_owned(), value.clone());
        }
    }
    Value::Object(public)
}

async fn mutate_tasks<R>(
    state: &AppState,
    mutate: impl FnOnce(&mut Vec<Value>) -> Result<R, ApiError>,
) -> Result<R, ApiError> {
    let path = editable_file_tasks_path(state);
    let _file_lock = acquire_path_write_lock(path.as_path()).await?;
    let mut tasks = read_editable_task_records(state)?
        .into_iter()
        .map(|value| {
            value
                .get("record")
                .cloned()
                .ok_or_else(ApiError::unavailable)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let result = mutate(&mut tasks)?;
    let bytes = encoded_tasks(&tasks)?;
    atomic_replace_checked_with_limit(&path, &bytes, MAX_EDITABLE_TASK_FILE_BYTES, false)?;
    Ok(result)
}

async fn existing_task(
    state: &AppState,
    owner: &str,
    task_id: &str,
) -> Result<Option<Value>, ApiError> {
    let path = editable_file_tasks_path(state);
    let _file_lock = acquire_path_write_lock(path.as_path()).await?;
    let tasks = read_editable_task_records(state)?
        .into_iter()
        .map(|value| {
            value
                .get("record")
                .cloned()
                .ok_or_else(ApiError::unavailable)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(tasks.into_iter().find_map(|task| {
        let object = task.as_object()?;
        (object.get("owner_id").and_then(Value::as_str) == Some(owner)
            && object.get("id").and_then(Value::as_str) == Some(task_id))
        .then(|| public_task(object))
    }))
}

fn acquire_editable_task_permit() -> Result<OwnedSemaphorePermit, ApiError> {
    #[cfg(test)]
    if FORCE_NEXT_EDITABLE_ADMISSION_FAILURE.swap(false, Ordering::AcqRel) {
        return Err(ApiError::background_queue_full());
    }
    EDITABLE_TASK_SEMAPHORE
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError::background_queue_full())
}

async fn update_task(
    state: &AppState,
    owner: &str,
    task_id: &str,
    status: &'static str,
    error: Option<&'static str>,
) -> Result<(), ApiError> {
    let (updated_at, updated_ts) = current_task_time();
    mutate_tasks(state, |tasks| {
        let task = tasks
            .iter_mut()
            .filter_map(Value::as_object_mut)
            .find(|task| {
                task.get("owner_id").and_then(Value::as_str) == Some(owner)
                    && task.get("id").and_then(Value::as_str) == Some(task_id)
            })
            .ok_or_else(ApiError::unavailable)?;
        task.insert("status".to_owned(), Value::String(status.to_owned()));
        task.insert("updated_at".to_owned(), Value::String(updated_at));
        task.insert("updated_ts".to_owned(), json!(updated_ts));
        if status == "running" {
            task.insert("started_ts".to_owned(), json!(updated_ts));
            task.remove("ended_ts");
            task.remove("error");
        } else {
            task.insert("ended_ts".to_owned(), json!(updated_ts));
            if let Some(error) = error {
                task.insert("error".to_owned(), Value::String(error.to_owned()));
            }
        }
        Ok(())
    })
    .await
}

fn editable_prompt(kind: &str, prompt: &str) -> String {
    let fixed = if kind == "psd" {
        EDITABLE_PSD_PROMPT
    } else {
        EDITABLE_PPT_PROMPT
    };
    let prompt = prompt.trim();
    if prompt.is_empty() {
        fixed.to_owned()
    } else {
        format!("{fixed}\n\n以下是用户补充需求，请直接结合执行：\n{prompt}")
    }
}

fn valid_upstream_text(value: Option<&Value>, max_chars: usize) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| {
            !value.is_empty()
                && value.chars().count() <= max_chars
                && !value.chars().any(char::is_control)
        })
        .map(ToOwned::to_owned)
}

async fn send_before_deadline(
    request: RequestBuilder,
    deadline: Instant,
) -> Result<reqwest::Response, TaskFailure> {
    if deadline.saturating_duration_since(Instant::now()).is_zero() {
        return Err(TaskFailure::Generic);
    }
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), request.send())
        .await
        .map_err(|_| TaskFailure::Generic)?
        .map_err(|_| TaskFailure::Generic)
}

async fn json_before_deadline(
    response: reqwest::Response,
    deadline: Instant,
) -> Result<Value, TaskFailure> {
    if !response.status().is_success() {
        return Err(TaskFailure::Generic);
    }
    let bytes = tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        bounded_response_body(response),
    )
    .await
    .map_err(|_| TaskFailure::Generic)?
    .map_err(|_| TaskFailure::Generic)?;
    serde_json::from_slice(&bytes).map_err(|_| TaskFailure::Generic)
}

fn authenticated_request(
    request: RequestBuilder,
    lease: &AccountLease,
    context: &NativeRequestContext,
    referer: &str,
    path: &str,
    route: &str,
) -> RequestBuilder {
    let request = native_browser_headers_with_referer(request, context, referer)
        .header(header::AUTHORIZATION, format!("Bearer {}", lease.token()))
        .header("X-OpenAI-Target-Path", path)
        .header("X-OpenAI-Target-Route", route);
    match lease.chatgpt_account_id() {
        Some(account_id) => request.header("ChatGPT-Account-ID", account_id),
        None => request,
    }
}

fn editable_image_format(bytes: &[u8]) -> Result<(&'static str, &'static str), TaskFailure> {
    match image::guess_format(bytes).map_err(|_| TaskFailure::Generic)? {
        image::ImageFormat::Png => Ok(("image/png", "png")),
        image::ImageFormat::Jpeg => Ok(("image/jpeg", "jpg")),
        image::ImageFormat::WebP => Ok(("image/webp", "webp")),
        _ => Err(TaskFailure::Generic),
    }
}

async fn upload_editable_image(
    state: &AppState,
    lease: &AccountLease,
    context: &NativeRequestContext,
    base_url: &str,
    image: &str,
    index: usize,
    deadline: Instant,
) -> Result<EditableUpload, TaskFailure> {
    let (bytes, _declared_mime, width, height) =
        native_image_input(image).map_err(|_| TaskFailure::Generic)?;
    if bytes.len() > MAX_EDITABLE_IMAGE_BYTES {
        return Err(TaskFailure::Generic);
    }
    let (mime_type, extension) = editable_image_format(&bytes)?;
    let file_name = format!("image_{index}.{extension}");
    let path = "/backend-api/files";
    let referer = format!("{base_url}/");
    let request = authenticated_request(
        state.client.post(format!("{base_url}{path}")),
        lease,
        context,
        &referer,
        path,
        path,
    )
    .header(header::ACCEPT, "*/*")
    .json(&json!({
        "file_name": file_name,
        "file_size": bytes.len(),
        "use_case": "multimodal",
        "timezone_offset_min": -480,
        "reset_rate_limits": false,
        "store_in_library": true,
        "library_persistence_mode": "opportunistic",
    }));
    let upload =
        json_before_deadline(send_before_deadline(request, deadline).await?, deadline).await?;
    let upload_url = valid_upstream_text(upload.get("upload_url"), 4096)
        .and_then(|value| validated_download_url(&value))
        .ok_or(TaskFailure::Generic)?;
    let file_id = valid_upstream_text(upload.get("file_id"), 256)
        .filter(|value| valid_artifact_id(value))
        .ok_or(TaskFailure::Generic)?;
    let library_file_id = valid_upstream_text(upload.get("library_file_id"), 256)
        .filter(|value| valid_artifact_id(value))
        .unwrap_or_default();
    let upload_response = send_before_deadline(
        state
            .client
            .put(upload_url)
            .header(header::CONTENT_TYPE, mime_type)
            .header("x-ms-blob-type", "BlockBlob")
            .header("x-ms-version", "2020-04-08")
            .header("Origin", base_url)
            .header(header::REFERER, &referer)
            .header(header::ACCEPT, "application/json, text/plain, */*")
            .body(bytes.clone()),
        deadline,
    )
    .await?;
    if !upload_response.status().is_success() {
        return Err(TaskFailure::Generic);
    }
    drop(upload_response);
    let uploaded_path = format!("/backend-api/files/{file_id}/uploaded");
    let uploaded = authenticated_request(
        state.client.post(format!("{base_url}{uploaded_path}")),
        lease,
        context,
        &referer,
        &uploaded_path,
        "/backend-api/files/{file_id}/uploaded",
    )
    .header(header::ACCEPT, "*/*")
    .json(&json!({}));
    let _ = json_before_deadline(send_before_deadline(uploaded, deadline).await?, deadline).await?;
    Ok(EditableUpload {
        file_id,
        library_file_id,
        file_name,
        file_size: bytes.len(),
        mime_type: mime_type.to_owned(),
        width,
        height,
    })
}

async fn prepare_editable_conversation(
    state: &AppState,
    lease: &AccountLease,
    context: &NativeRequestContext,
    base_url: &str,
    prompt: &str,
    uploads: &[EditableUpload],
    deadline: Instant,
) -> Result<String, TaskFailure> {
    let path = "/backend-api/f/conversation/prepare";
    let referer = format!("{base_url}/");
    let mut payload = json!({
        "action": "next",
        "fork_from_shared_post": false,
        "parent_message_id": "client-created-root",
        "model": EDITABLE_MODEL,
        "client_prepare_state": "success",
        "timezone_offset_min": -480,
        "timezone": "Asia/Shanghai",
        "conversation_mode": {"kind":"primary_assistant"},
        "system_hints": [],
        "partial_query": {
            "id": native_message_id(),
            "author": {"role":"user"},
            "content": {"content_type":"text","parts":[prompt]},
        },
        "supports_buffering": true,
        "supported_encodings": ["v1"],
        "client_contextual_info": {"app_name":"chatgpt.com"},
        "thinking_effort": EDITABLE_THINKING_EFFORT,
    });
    if !uploads.is_empty() {
        payload["attachment_mime_types"] = Value::Array(
            uploads
                .iter()
                .map(|upload| Value::String(upload.mime_type.clone()))
                .collect(),
        );
    }
    let request = authenticated_request(
        state.client.post(format!("{base_url}{path}")),
        lease,
        context,
        &referer,
        path,
        path,
    )
    .header(header::ACCEPT, "*/*")
    .header("X-Conduit-Token", "no-token")
    .json(&payload);
    let response =
        json_before_deadline(send_before_deadline(request, deadline).await?, deadline).await?;
    valid_upstream_text(response.get("conduit_token"), 4096).ok_or(TaskFailure::Generic)
}

async fn run_editable_conversation(
    upstream: &EditableUpstream<'_>,
    prompt: &str,
    uploads: &[EditableUpload],
    conduit_token: &str,
) -> Result<String, TaskFailure> {
    let state = upstream.state;
    let lease = upstream.lease;
    let context = upstream.context;
    let base_url = upstream.base_url;
    let deadline = upstream.deadline;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(TaskFailure::Generic);
    }
    let resources = native_bootstrap_with_timeout_context(
        &state.client,
        base_url,
        lease.token(),
        remaining,
        context,
    )
    .await
    .map_err(|_| TaskFailure::Generic)?;
    let requirements = native_chat_requirements_with_resources_for_route_context(
        &state.client,
        base_url,
        lease.token(),
        &resources,
        deadline.saturating_duration_since(Instant::now()),
        true,
        context,
    )
    .await
    .map_err(|_| TaskFailure::Generic)?;

    let mut message = json!({
        "id": native_message_id(),
        "author": {"role":"user"},
        "create_time": native_created(),
    });
    if uploads.is_empty() {
        message["content"] = json!({"content_type":"text","parts":[prompt]});
    } else {
        let mut parts = uploads
            .iter()
            .map(|upload| {
                json!({
                    "content_type": "image_asset_pointer",
                    "asset_pointer": format!("sediment://{}", upload.file_id),
                    "size_bytes": upload.file_size,
                    "width": upload.width,
                    "height": upload.height,
                })
            })
            .collect::<Vec<_>>();
        parts.push(Value::String(prompt.to_owned()));
        message["content"] = json!({"content_type":"multimodal_text","parts":parts});
        message["metadata"] = json!({
            "attachments": uploads.iter().map(|upload| json!({
                "id": upload.file_id,
                "size": upload.file_size,
                "name": upload.file_name,
                "mime_type": upload.mime_type,
                "width": upload.width,
                "height": upload.height,
                "source": "library",
                "library_file_id": upload.library_file_id,
                "is_big_paste": false,
            })).collect::<Vec<_>>(),
            "developer_mode_connector_ids": [],
            "selected_sources": [],
            "selected_github_repos": [],
            "selected_all_github_repos": false,
            "serialization_metadata": {"custom_symbol_offsets": []},
        });
    }
    let path = "/backend-api/f/conversation";
    let referer = format!("{base_url}/");
    let payload = json!({
        "action": "next",
        "messages": [message],
        "parent_message_id": "client-created-root",
        "model": EDITABLE_MODEL,
        "client_prepare_state": "sent",
        "timezone_offset_min": -480,
        "timezone": "Asia/Shanghai",
        "conversation_mode": {"kind":"primary_assistant"},
        "enable_message_followups": true,
        "system_hints": [],
        "supports_buffering": true,
        "supported_encodings": ["v1"],
        "client_contextual_info": {
            "is_dark_mode": false,
            "time_since_loaded": 401,
            "page_height": 1138,
            "page_width": 803,
            "pixel_ratio": 2,
            "screen_height": 1440,
            "screen_width": 2560,
            "app_name": "chatgpt.com",
        },
        "paragen_cot_summary_display_override": "allow",
        "force_parallel_switch": "auto",
        "thinking_effort": EDITABLE_THINKING_EFFORT,
    });
    let mut request = authenticated_request(
        state.client.post(format!("{base_url}{path}")),
        lease,
        context,
        &referer,
        path,
        path,
    )
    .header(header::ACCEPT, "text/event-stream")
    .header("X-Conduit-Token", conduit_token)
    .header(
        "OpenAI-Sentinel-Chat-Requirements-Token",
        requirements.token,
    )
    .json(&payload);
    if let Some(value) = requirements.so_token {
        request = request.header("OpenAI-Sentinel-SO-Token", value);
    }
    if let Some(value) = requirements.proof_token {
        request = request.header("OpenAI-Sentinel-Proof-Token", value);
    }
    if let Some(value) = requirements.turnstile_token {
        request = request.header("OpenAI-Sentinel-Turnstile-Token", value);
    }
    let response = send_before_deadline(request, deadline).await?;
    if !response.status().is_success() {
        return Err(TaskFailure::Generic);
    }
    search_conversation_id_from_response(response, deadline)
        .await
        .map_err(|_| TaskFailure::Generic)
}

fn valid_artifact_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn artifact_pointer_id(value: &str) -> Option<String> {
    ["file-service://", "sediment://"]
        .into_iter()
        .find_map(|prefix| value.strip_prefix(prefix))
        .map(str::trim)
        .filter(|value| valid_artifact_id(value))
        .map(ToOwned::to_owned)
}

fn sandbox_artifact_paths(value: &str) -> Vec<String> {
    const PREFIX: &str = "/mnt/data/";
    const SUFFIXES: [&str; 4] = [".pptx", ".ppt", ".psd", ".zip"];

    let mut paths = Vec::new();
    let mut search_from = 0usize;
    while paths.len() < MAX_EDITABLE_ARTIFACT_NODES && search_from < value.len() {
        let Some(relative_start) = value[search_from..].find(PREFIX) else {
            break;
        };
        let start = search_from + relative_start;
        let rest = &value[start..];
        let segment_end = rest
            .char_indices()
            .find_map(|(index, character)| {
                (character.is_whitespace() || matches!(character, '"' | '\'' | ')' | ']'))
                    .then_some(index)
            })
            .unwrap_or(rest.len());
        let segment = &rest[..segment_end];
        let lowered = segment.to_ascii_lowercase();
        let path_end = SUFFIXES
            .iter()
            .filter_map(|suffix| lowered.rfind(suffix).map(|index| index + suffix.len()))
            .max()
            .unwrap_or_default();
        if path_end > PREFIX.len() {
            let path = &segment[..path_end];
            if path.chars().count() <= 4096
                && !path.chars().any(char::is_control)
                && !paths.iter().any(|current| current == path)
            {
                paths.push(path.to_owned());
            }
        }
        search_from = start.saturating_add(segment_end.max(PREFIX.len()));
    }
    paths
}

fn clean_artifact_name(value: &str) -> String {
    let value = value.rsplit(['/', '\\']).next().unwrap_or_default().trim();
    if value.is_empty()
        || value.chars().count() > 255
        || value.contains(':')
        || value.chars().any(char::is_control)
    {
        String::new()
    } else {
        value.to_owned()
    }
}

fn clean_mime_type(value: &str) -> String {
    let value = value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if value.len() <= 256 && value.contains('/') && !value.chars().any(char::is_control) {
        value
    } else {
        String::new()
    }
}

fn artifact_from_object(
    object: &Map<String, Value>,
    message_id: &str,
    created_at: f64,
) -> Option<EditableArtifact> {
    let name = ["name", "file_name", "filename", "title"]
        .into_iter()
        .find_map(|field| object.get(field).and_then(Value::as_str))
        .map(clean_artifact_name)
        .unwrap_or_default();
    let mime_type = ["mime_type", "mimeType"]
        .into_iter()
        .find_map(|field| object.get(field).and_then(Value::as_str))
        .map(clean_mime_type)
        .unwrap_or_default();
    let pointer_id = object
        .get("asset_pointer")
        .and_then(Value::as_str)
        .and_then(artifact_pointer_id);
    let file_id = object
        .get("file_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| valid_artifact_id(value))
        .map(ToOwned::to_owned);
    let attachment_id = object
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| valid_artifact_id(value))
        .filter(|_| !name.is_empty() || !mime_type.is_empty())
        .map(ToOwned::to_owned);
    let id = pointer_id.or(file_id).or(attachment_id)?;
    Some(EditableArtifact {
        id,
        name,
        mime_type,
        sandbox_path: String::new(),
        message_id: message_id.to_owned(),
        created_at,
    })
}

fn collect_message_artifacts(
    message: &Value,
    message_id: &str,
    created_at: f64,
    artifacts: &mut HashMap<String, EditableArtifact>,
) -> Result<(), TaskFailure> {
    let mut stack = vec![(message, 0usize)];
    let mut nodes = 0usize;
    while let Some((value, depth)) = stack.pop() {
        nodes = nodes.saturating_add(1);
        if nodes > MAX_EDITABLE_ARTIFACT_NODES || depth > MAX_EDITABLE_ARTIFACT_DEPTH {
            return Err(TaskFailure::Generic);
        }
        match value {
            Value::Object(object) => {
                if let Some(candidate) = artifact_from_object(object, message_id, created_at) {
                    match artifacts.get_mut(&candidate.id) {
                        Some(current) if current.created_at <= candidate.created_at => {
                            if !candidate.name.is_empty() {
                                current.name = candidate.name;
                            }
                            if !candidate.mime_type.is_empty() {
                                current.mime_type = candidate.mime_type;
                            }
                            if !candidate.sandbox_path.is_empty() {
                                current.sandbox_path = candidate.sandbox_path;
                            }
                            current.message_id = candidate.message_id;
                            current.created_at = candidate.created_at;
                        }
                        None => {
                            artifacts.insert(candidate.id.clone(), candidate);
                        }
                        Some(_) => {}
                    }
                }
                stack.extend(object.values().map(|value| (value, depth + 1)));
            }
            Value::Array(values) => {
                stack.extend(values.iter().map(|value| (value, depth + 1)));
            }
            Value::String(value) => {
                for sandbox_path in sandbox_artifact_paths(value) {
                    let name = clean_artifact_name(&sandbox_path);
                    if name.is_empty() {
                        continue;
                    }
                    let candidate = EditableArtifact {
                        id: String::new(),
                        name,
                        mime_type: String::new(),
                        sandbox_path: sandbox_path.clone(),
                        message_id: message_id.to_owned(),
                        created_at,
                    };
                    let key = format!("sandbox:{sandbox_path}");
                    match artifacts.get_mut(&key) {
                        Some(current) if current.created_at <= created_at => {
                            *current = candidate;
                        }
                        None => {
                            artifacts.insert(key, candidate);
                        }
                        Some(_) => {}
                    }
                    if artifacts.len() > MAX_EDITABLE_ARTIFACT_NODES {
                        return Err(TaskFailure::Generic);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn extract_editable_artifacts(conversation: &Value) -> Result<Vec<EditableArtifact>, TaskFailure> {
    let mapping = conversation
        .get("mapping")
        .and_then(Value::as_object)
        .ok_or(TaskFailure::Generic)?;
    if mapping.len() > MAX_EDITABLE_ARTIFACT_NODES {
        return Err(TaskFailure::Generic);
    }
    let mut artifacts = HashMap::new();
    for node in mapping.values() {
        let Some(message) = node.get("message") else {
            continue;
        };
        let role = message
            .get("author")
            .and_then(|author| author.get("role"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if !matches!(role.as_str(), "assistant" | "tool") {
            continue;
        }
        let message_id = message
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| valid_artifact_id(value))
            .unwrap_or_default();
        let created_at = message
            .get("create_time")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite())
            .unwrap_or_default();
        collect_message_artifacts(message, message_id, created_at, &mut artifacts)?;
    }
    let mut artifacts = artifacts.into_values().collect::<Vec<_>>();
    artifacts.sort_by(|left, right| left.created_at.total_cmp(&right.created_at));
    Ok(artifacts)
}

fn is_primary_artifact(artifact: &EditableArtifact, kind: &str) -> bool {
    let name = artifact.name.to_ascii_lowercase();
    let sandbox_path = artifact.sandbox_path.to_ascii_lowercase();
    let mime = artifact.mime_type.as_str();
    if kind == "psd" {
        name.ends_with(".psd") || sandbox_path.ends_with(".psd") || mime.contains("photoshop")
    } else {
        name.ends_with(".ppt")
            || name.ends_with(".pptx")
            || sandbox_path.ends_with(".ppt")
            || sandbox_path.ends_with(".pptx")
            || mime.contains("presentationml.presentation")
            || mime.contains("ms-powerpoint")
    }
}

fn is_zip_artifact(artifact: &EditableArtifact) -> bool {
    artifact.name.to_ascii_lowercase().ends_with(".zip")
        || artifact.sandbox_path.to_ascii_lowercase().ends_with(".zip")
        || matches!(
            artifact.mime_type.as_str(),
            "application/zip" | "application/x-zip-compressed"
        )
        || artifact.mime_type.ends_with("/zip")
}

async fn sleep_editable_poll(deadline: Instant) -> bool {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return false;
    }
    tokio::time::sleep(EDITABLE_POLL_INTERVAL.min(remaining)).await;
    Instant::now() < deadline
}

async fn wait_for_editable_artifacts(
    state: &AppState,
    lease: &AccountLease,
    context: &NativeRequestContext,
    base_url: &str,
    conversation_id: &str,
    kind: &str,
    deadline: Instant,
) -> Result<(EditableArtifact, EditableArtifact), TaskFailure> {
    loop {
        if deadline.saturating_duration_since(Instant::now()).is_zero() {
            return Err(TaskFailure::Generic);
        }
        let path = format!("/backend-api/conversation/{conversation_id}");
        let referer = format!("{base_url}/c/{conversation_id}");
        let request = authenticated_request(
            state.client.get(format!("{base_url}{path}")),
            lease,
            context,
            &referer,
            &path,
            "/backend-api/conversation/{conversation_id}",
        )
        .header(header::ACCEPT, "*/*");
        let response = send_before_deadline(request, deadline).await?;
        let status = response.status();
        if !status.is_success() {
            drop(response);
            if matches!(
                status,
                StatusCode::NOT_FOUND
                    | StatusCode::CONFLICT
                    | StatusCode::LOCKED
                    | StatusCode::TOO_MANY_REQUESTS
                    | StatusCode::INTERNAL_SERVER_ERROR
                    | StatusCode::BAD_GATEWAY
                    | StatusCode::SERVICE_UNAVAILABLE
                    | StatusCode::GATEWAY_TIMEOUT
            ) && sleep_editable_poll(deadline).await
            {
                continue;
            }
            return Err(TaskFailure::Generic);
        }
        let conversation = json_before_deadline(response, deadline).await?;
        let artifacts = extract_editable_artifacts(&conversation)?;
        let primary = artifacts
            .iter()
            .rev()
            .find(|artifact| is_primary_artifact(artifact, kind))
            .cloned();
        let zip = artifacts
            .iter()
            .rev()
            .find(|artifact| is_zip_artifact(artifact))
            .cloned();
        if let (Some(primary), Some(zip)) = (primary, zip)
            && primary.id != zip.id
        {
            return Ok((primary, zip));
        }
        if !sleep_editable_poll(deadline).await {
            return Err(TaskFailure::Generic);
        }
    }
}

fn validated_download_url(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > 4096 || value.chars().any(char::is_control) {
        return None;
    }
    let parsed = Url::parse(value).ok()?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return None;
    }
    Some(value.to_owned())
}

async fn probe_download_url(
    upstream: &EditableUpstream<'_>,
    conversation_id: &str,
    path: &str,
    route: &str,
    query: Option<&[(&str, &str)]>,
) -> Result<Option<String>, TaskFailure> {
    let state = upstream.state;
    let lease = upstream.lease;
    let context = upstream.context;
    let base_url = upstream.base_url;
    let deadline = upstream.deadline;
    let referer = format!("{base_url}/c/{conversation_id}");
    let mut request = authenticated_request(
        state.client.get(format!("{base_url}{path}")),
        lease,
        context,
        &referer,
        path,
        route,
    )
    .header(header::ACCEPT, "*/*");
    if let Some(query) = query {
        request = request.query(query);
    }
    let response = send_before_deadline(request, deadline).await?;
    if !response.status().is_success() {
        return Ok(None);
    }
    let value = json_before_deadline(response, deadline).await?;
    Ok(["download_url", "url"]
        .into_iter()
        .find_map(|field| value.get(field).and_then(Value::as_str))
        .and_then(validated_download_url))
}

async fn resolve_download_url(
    upstream: &EditableUpstream<'_>,
    conversation_id: &str,
    artifact: &EditableArtifact,
) -> Result<String, TaskFailure> {
    if !artifact.sandbox_path.is_empty() && !artifact.message_id.is_empty() {
        let path = format!("/backend-api/conversation/{conversation_id}/interpreter/download");
        let query = [
            ("message_id", artifact.message_id.as_str()),
            ("sandbox_path", artifact.sandbox_path.as_str()),
        ];
        if let Some(url) = probe_download_url(
            upstream,
            conversation_id,
            &path,
            "/backend-api/conversation/{conversation_id}/interpreter/download",
            Some(&query),
        )
        .await?
        {
            return Ok(url);
        }
    }
    if artifact.id.is_empty() {
        return Err(TaskFailure::Generic);
    }
    let attachment_path = format!(
        "/backend-api/conversation/{conversation_id}/attachment/{}/download",
        artifact.id
    );
    if let Some(url) = probe_download_url(
        upstream,
        conversation_id,
        &attachment_path,
        "/backend-api/conversation/{conversation_id}/attachment/{attachment_id}/download",
        None,
    )
    .await?
    {
        return Ok(url);
    }
    let files_path = format!("/backend-api/files/download/{}", artifact.id);
    let query = [("post_id", ""), ("inline", "false")];
    if let Some(url) = probe_download_url(
        upstream,
        conversation_id,
        &files_path,
        "/backend-api/files/download/{file_id}",
        Some(&query),
    )
    .await?
    {
        return Ok(url);
    }
    let legacy_path = format!("/backend-api/files/{}/download", artifact.id);
    probe_download_url(
        upstream,
        conversation_id,
        &legacy_path,
        "/backend-api/files/download/{file_id}",
        None,
    )
    .await?
    .ok_or(TaskFailure::Generic)
}

async fn bounded_artifact_body(
    response: reqwest::Response,
    deadline: Instant,
) -> Result<Vec<u8>, TaskFailure> {
    if let Some(value) = response.headers().get(header::CONTENT_LENGTH) {
        let value = value.to_str().map_err(|_| TaskFailure::Generic)?;
        if value.is_empty()
            || value.len() > 20
            || !value.bytes().all(|byte| byte.is_ascii_digit())
            || value
                .parse::<usize>()
                .ok()
                .is_none_or(|value| value > MAX_EDITABLE_ARTIFACT_BYTES)
        {
            return Err(TaskFailure::Generic);
        }
    }
    let mut body = Vec::new();
    let mut chunks = response.bytes_stream();
    loop {
        let next = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), chunks.next())
            .await
            .map_err(|_| TaskFailure::Generic)?;
        let Some(chunk) = next else {
            break;
        };
        let chunk = chunk.map_err(|_| TaskFailure::Generic)?;
        if body
            .len()
            .checked_add(chunk.len())
            .is_none_or(|size| size > MAX_EDITABLE_ARTIFACT_BYTES)
        {
            return Err(TaskFailure::Generic);
        }
        body.extend_from_slice(&chunk);
    }
    if body.is_empty() {
        return Err(TaskFailure::Generic);
    }
    Ok(body)
}

fn output_directory_is_current(output_directory: &DirectoryHandle, output_path: &Path) -> bool {
    output_directory.same_identity().unwrap_or(false)
        && open_directory(output_path)
            .ok()
            .is_some_and(|current| current.identity() == output_directory.identity())
}

fn artifact_output_name(artifact: &EditableArtifact, kind: &str, primary: bool) -> String {
    let extension = if primary {
        if kind == "psd" { ".psd" } else { ".pptx" }
    } else {
        ".zip"
    };
    let mut name = clean_artifact_name(&artifact.name);
    if name.is_empty() {
        return format!("artifact{extension}");
    }
    if Path::new(&name).extension().is_none() {
        name.push_str(extension);
    }
    name
}

async fn download_artifact(
    upstream: &EditableUpstream<'_>,
    conversation_id: &str,
    artifact: &EditableArtifact,
    output_path: &Path,
    output_directory: &DirectoryHandle,
    output_name: String,
) -> Result<String, TaskFailure> {
    let url = resolve_download_url(upstream, conversation_id, artifact).await?;
    let response = send_before_deadline(upstream.state.client.get(url), upstream.deadline).await?;
    if !response.status().is_success() {
        return Err(TaskFailure::Generic);
    }
    let bytes = bounded_artifact_body(response, upstream.deadline).await?;
    if !output_directory_is_current(output_directory, output_path) {
        return Err(TaskFailure::Generic);
    }
    let target = output_path.join(&output_name);
    let write_target = target.clone();
    tokio::task::spawn_blocking(move || {
        atomic_replace_checked_with_limit(
            &write_target,
            &bytes,
            MAX_EDITABLE_ARTIFACT_BYTES as u64,
            false,
        )
    })
    .await
    .map_err(|_| TaskFailure::Generic)?
    .map_err(|_| TaskFailure::Generic)?;
    if !output_directory_is_current(output_directory, output_path) {
        return Err(TaskFailure::Generic);
    }
    Ok(output_name)
}

async fn download_editable_outputs(
    upstream: &EditableUpstream<'_>,
    owner: &str,
    task_id: &str,
    kind: &str,
    conversation_id: &str,
    primary: &EditableArtifact,
    zip: &EditableArtifact,
) -> Result<(String, String), TaskFailure> {
    let owner_scope = Sha256::digest(owner.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let output_path = upstream
        .state
        .data_dir
        .join("files")
        .join(owner_scope)
        .join(kind)
        .join(task_id);
    let output_directory =
        open_or_create_directory(&output_path).map_err(|_| TaskFailure::Generic)?;
    let primary_name = artifact_output_name(primary, kind, true);
    let zip_name = artifact_output_name(zip, kind, false);
    let primary_lower = primary_name.to_ascii_lowercase();
    let valid_primary = if kind == "psd" {
        primary_lower.ends_with(".psd")
    } else {
        primary_lower.ends_with(".ppt") || primary_lower.ends_with(".pptx")
    };
    if !valid_primary
        || !zip_name.to_ascii_lowercase().ends_with(".zip")
        || primary_name == zip_name
    {
        return Err(TaskFailure::Generic);
    }
    let primary_name = download_artifact(
        upstream,
        conversation_id,
        primary,
        &output_path,
        &output_directory,
        primary_name,
    )
    .await?;
    let zip_name = download_artifact(
        upstream,
        conversation_id,
        zip,
        &output_path,
        &output_directory,
        zip_name,
    )
    .await?;
    Ok((primary_name, zip_name))
}

fn new_download_capability() -> Result<String, TaskFailure> {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).map_err(|_| TaskFailure::Generic)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn editable_url_segment(value: &str) -> String {
    percent_encoding::percent_encode(value.as_bytes(), percent_encoding::NON_ALPHANUMERIC)
        .to_string()
}

async fn complete_task(
    state: &AppState,
    owner: &str,
    task_id: &str,
    kind: &str,
    export: EditableExport,
) -> Result<(), ApiError> {
    let primary_name = editable_relative_path(&export.primary_name)
        .filter(|value| value == &export.primary_name)
        .ok_or_else(ApiError::unavailable)?;
    let zip_name = editable_relative_path(&export.zip_name)
        .filter(|value| value == &export.zip_name)
        .ok_or_else(ApiError::unavailable)?;
    if primary_name == zip_name {
        return Err(ApiError::unavailable());
    }
    let capability = new_download_capability().map_err(|_| ApiError::unavailable())?;
    let owner_scope = Sha256::digest(owner.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let task_segment = editable_url_segment(task_id);
    let primary_url = format!(
        "/files/{capability}/{owner_scope}/{kind}/{task_segment}/{}",
        editable_url_segment(&primary_name)
    );
    let zip_url = format!(
        "/files/{capability}/{owner_scope}/{kind}/{task_segment}/{}",
        editable_url_segment(&zip_name)
    );
    let primary_digest =
        editable_capability_digest(&capability, owner, kind, task_id, &primary_name);
    let zip_digest = editable_capability_digest(&capability, owner, kind, task_id, &zip_name);
    let mut capability_hashes = Map::new();
    capability_hashes.insert(primary_name, Value::String(primary_digest));
    capability_hashes.insert(zip_name, Value::String(zip_digest));
    let (updated_at, updated_ts) = current_task_time();

    mutate_tasks(state, |tasks| {
        let task = tasks
            .iter_mut()
            .filter_map(Value::as_object_mut)
            .find(|task| {
                task.get("owner_id").and_then(Value::as_str) == Some(owner)
                    && task.get("id").and_then(Value::as_str) == Some(task_id)
            })
            .ok_or_else(ApiError::unavailable)?;
        if task.get("status").and_then(Value::as_str) != Some("running") {
            return Err(ApiError::unavailable());
        }
        task.insert("status".to_owned(), Value::String("success".to_owned()));
        task.insert("updated_at".to_owned(), Value::String(updated_at));
        task.insert("updated_ts".to_owned(), json!(updated_ts));
        task.insert("ended_ts".to_owned(), json!(updated_ts));
        task.insert(
            "result".to_owned(),
            json!({
                "conversation_id": export.conversation_id,
                "primary_url": primary_url,
                "zip_url": zip_url,
            }),
        );
        task.insert(
            "download_capability_hashes".to_owned(),
            Value::Object(capability_hashes),
        );
        task.remove("error");
        Ok(())
    })
    .await
}

async fn execute_task(
    state: &AppState,
    kind: &'static str,
    prompt: &str,
    images: &[String],
    owner: &str,
    task_id: &str,
    lease: &AccountLease,
) -> Result<EditableExport, TaskFailure> {
    let base_url = state
        .config
        .upstream_base_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(TaskFailure::Generic)?
        .trim_end_matches('/');
    if base_url.is_empty() {
        return Err(TaskFailure::Generic);
    }
    let deadline = Instant::now() + EDITABLE_TIMEOUT;
    let context =
        NativeRequestContext::for_client(EDITABLE_CLIENT_VERSION, EDITABLE_CLIENT_BUILD_NUMBER);
    let upstream = EditableUpstream {
        state,
        lease,
        context: &context,
        base_url,
        deadline,
    };
    let prompt = editable_prompt(kind, prompt);
    let mut uploads = Vec::with_capacity(images.len());
    for (index, image) in images.iter().enumerate() {
        uploads.push(
            upload_editable_image(state, lease, &context, base_url, image, index + 1, deadline)
                .await?,
        );
    }
    let conduit_token = prepare_editable_conversation(
        state, lease, &context, base_url, &prompt, &uploads, deadline,
    )
    .await?;
    let conversation_id =
        run_editable_conversation(&upstream, &prompt, &uploads, &conduit_token).await?;
    let (primary, zip) = wait_for_editable_artifacts(
        state,
        lease,
        &context,
        base_url,
        &conversation_id,
        kind,
        deadline,
    )
    .await?;
    let (primary_name, zip_name) = download_editable_outputs(
        &upstream,
        owner,
        task_id,
        kind,
        &conversation_id,
        &primary,
        &zip,
    )
    .await?;
    Ok(EditableExport {
        conversation_id,
        primary_name,
        zip_name,
    })
}

async fn run_task(
    state: &AppState,
    owner: String,
    task_id: String,
    kind: &'static str,
    prompt: String,
    images: Vec<String>,
    _permit: OwnedSemaphorePermit,
) {
    if update_task(state, &owner, &task_id, "running", None)
        .await
        .is_err()
    {
        return;
    }
    if kind == "psd" && images.is_empty() {
        let _ = update_task(
            state,
            &owner,
            &task_id,
            "error",
            Some(TaskFailure::PsdImageRequired.public_message()),
        )
        .await;
        return;
    }
    let allowed_groups = HashSet::<AccountModelGroup>::from([
        "plus".to_owned(),
        "team".to_owned(),
        "pro".to_owned(),
        "enterprise".to_owned(),
    ]);
    let Some(lease) = state
        .account_store
        .acquire_excluding_with_type_and_capability_filter(
            EDITABLE_MODEL,
            &HashSet::new(),
            Some(&allowed_groups),
            Some("web"),
        )
        .await
    else {
        let _ = update_task(
            state,
            &owner,
            &task_id,
            "error",
            Some(TaskFailure::Generic.public_message()),
        )
        .await;
        return;
    };

    match execute_task(state, kind, &prompt, &images, &owner, &task_id, &lease).await {
        Ok(export) => {
            if !state.account_store.mark_text_used(lease.token()) {
                AccountStore::note_usage_mark_failure();
            }
            if complete_task(state, &owner, &task_id, kind, export)
                .await
                .is_err()
            {
                let _ = update_task(
                    state,
                    &owner,
                    &task_id,
                    "error",
                    Some(TaskFailure::Generic.public_message()),
                )
                .await;
            }
        }
        Err(error) => {
            let _ = update_task(
                state,
                &owner,
                &task_id,
                "error",
                Some(error.public_message()),
            )
            .await;
        }
    }
    drop(lease);
}

pub(super) async fn submit(
    state: AppState,
    headers: HeaderMap,
    body: Body,
    kind: &'static str,
) -> Result<Response, ApiError> {
    let owner = authenticated_subject(&headers, &state).await?;
    let request = parse_request(body).await?;
    if let Some(existing) = existing_task(&state, &owner, &request.task_id).await? {
        return Ok(Json(existing).into_response());
    }
    let permit = acquire_editable_task_permit()?;
    let (created_at, created_ts) = current_task_time();
    let mut created = false;
    let public = mutate_tasks(&state, |tasks| {
        if let Some(task) = tasks.iter().filter_map(Value::as_object).find(|task| {
            task.get("owner_id").and_then(Value::as_str) == Some(owner.as_str())
                && task.get("id").and_then(Value::as_str) == Some(request.task_id.as_str())
        }) {
            return Ok(public_task(task));
        }
        created = true;
        let task = json!({
            "id": request.task_id,
            "owner_id": owner,
            "status": "queued",
            "kind": kind,
            "model": EDITABLE_MODEL,
            "created_at": created_at,
            "updated_at": created_at,
            "created_ts": created_ts,
            "updated_ts": created_ts,
        });
        let public = public_task(task.as_object().ok_or_else(ApiError::unavailable)?);
        tasks.push(task);
        Ok(public)
    })
    .await?;
    if created {
        let worker_state = state.clone();
        let worker_tracker = state.editable_workers.clone();
        #[cfg(test)]
        let completion_tracker = worker_tracker.clone();
        let worker_owner = owner;
        let worker_task_id = request.task_id;
        let rejected_owner = worker_owner.clone();
        let rejected_task_id = worker_task_id.clone();
        let worker_prompt = request.prompt;
        let worker_images = request.images;
        let started = worker_tracker
            .spawn(async move {
                run_task(
                    &worker_state,
                    worker_owner,
                    worker_task_id,
                    kind,
                    worker_prompt,
                    worker_images,
                    permit,
                )
                .await;
                #[cfg(test)]
                completion_tracker
                    .wait_at_completion_barrier_for_test()
                    .await;
                drop(worker_state);
            })
            .await;
        if !started {
            let _ = update_task(
                &state,
                &rejected_owner,
                &rejected_task_id,
                "error",
                Some(TaskFailure::Generic.public_message()),
            )
            .await;
            return Err(ApiError::unavailable());
        }
    } else {
        drop(permit);
    }
    Ok(Json(public).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn editable_worker_owner_shutdown_aborts_joins_and_fences_admission() {
        struct PendingWorkerGuard(Arc<AtomicBool>);

        impl Drop for PendingWorkerGuard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let workers = Arc::new(EditableWorkers::new());
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let worker_started = started.clone();
        let worker_dropped = dropped.clone();
        assert!(
            workers
                .spawn(async move {
                    let _guard = PendingWorkerGuard(worker_dropped);
                    worker_started.notify_one();
                    std::future::pending::<()>().await;
                })
                .await
        );
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("editable worker starts");
        assert_eq!(workers.active_for_test(), 1);

        tokio::time::timeout(Duration::from_secs(1), workers.shutdown())
            .await
            .expect("editable worker owner shutdown");
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(workers.active_for_test(), 0);
        assert!(!workers.spawn(async {}).await);
    }

    #[test]
    fn editable_sandbox_only_artifacts_are_bound_to_the_assistant_message() {
        let conversation = json!({
            "mapping": {
                "assistant": {
                    "message": {
                        "id":"message-sandbox",
                        "author":{"role":"assistant"},
                        "create_time":42.0,
                        "content": {
                            "content_type":"multimodal_text",
                            "parts":[
                                "sandbox:/mnt/data/quarterly-deck.pptx",
                                {"text":"layers: /mnt/data/deck-assets.zip"}
                            ]
                        }
                    }
                }
            }
        });

        let artifacts = match extract_editable_artifacts(&conversation) {
            Ok(artifacts) => artifacts,
            Err(_) => panic!("sandbox-only artifact extraction failed"),
        };

        assert_eq!(artifacts.len(), 2);
        let primary = artifacts
            .iter()
            .find(|artifact| is_primary_artifact(artifact, "ppt"))
            .expect("sandbox PPT artifact");
        let zip = artifacts
            .iter()
            .find(|artifact| is_zip_artifact(artifact))
            .expect("sandbox ZIP artifact");
        assert_eq!(primary.message_id, "message-sandbox");
        assert_eq!(primary.sandbox_path, "/mnt/data/quarterly-deck.pptx");
        assert_eq!(primary.name, "quarterly-deck.pptx");
        assert!(primary.id.is_empty());
        assert_eq!(zip.sandbox_path, "/mnt/data/deck-assets.zip");
    }

    #[test]
    fn editable_output_name_adds_the_authoritative_artifact_extension() {
        let artifact = EditableArtifact {
            id: "file-primary".to_owned(),
            name: "editable-deck".to_owned(),
            mime_type: "application/vnd.openxmlformats-officedocument.presentationml.presentation"
                .to_owned(),
            sandbox_path: String::new(),
            message_id: "message-primary".to_owned(),
            created_at: 1.0,
        };

        assert_eq!(
            artifact_output_name(&artifact, "ppt", true),
            "editable-deck.pptx"
        );
    }

    #[tokio::test]
    async fn duplicate_editable_submission_is_idempotent_before_queue_admission() {
        struct AdmissionFailureGuard;

        impl Drop for AdmissionFailureGuard {
            fn drop(&mut self) {
                FORCE_NEXT_EDITABLE_ADMISSION_FAILURE.store(false, Ordering::Release);
            }
        }

        let _admission_failure_guard = AdmissionFailureGuard;
        let root = crate::project_local_test_tmp_dir().join(format!(
            "editable-idempotency-admission-{}-{}",
            std::process::id(),
            current_task_time().1.to_bits()
        ));
        fs::create_dir_all(&root).expect("editable idempotency root");
        let accounts_path = root.join("accounts.json");
        let task_path = root.join("editable_file_tasks.json");
        fs::write(&accounts_path, b"[]\n").expect("editable idempotency accounts");
        fs::write(
            &task_path,
            serde_json::to_vec(&json!({
                "tasks": [{
                    "id": "same-task",
                    "owner_id": "admin",
                    "status": "error",
                    "kind": "ppt",
                    "model": EDITABLE_MODEL,
                    "created_at": "2026-08-26 12:00:00",
                    "updated_at": "2026-08-26 12:00:01",
                    "created_ts": 1.0,
                    "updated_ts": 2.0,
                    "ended_ts": 2.0,
                    "error": GENERIC_TASK_ERROR
                }]
            }))
            .expect("editable idempotency task snapshot"),
        )
        .expect("editable idempotency task file");
        let state = crate::AppState::new(crate::AppConfig {
            version: "editable-idempotency".to_owned(),
            auth_key: Some("admin-key".to_owned()),
            models: Vec::new(),
            upstream_base_url: None,
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(accounts_path),
            upstream_protocol: crate::UpstreamProtocol::ChatGpt,
        })
        .expect("editable idempotency state");
        let before = fs::read(&task_path).expect("editable idempotency before bytes");
        FORCE_NEXT_EDITABLE_ADMISSION_FAILURE.store(true, Ordering::Release);
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Bearer admin-key".parse().expect("authorization header"),
        );
        let response = submit(
            state,
            headers,
            Body::from(
                json!({
                    "client_task_id": "same-task",
                    "prompt": "must not replace the existing task"
                })
                .to_string(),
            ),
            "ppt",
        )
        .await
        .expect("duplicate editable submission response");
        assert_eq!(response.status(), StatusCode::OK);
        let public = response
            .into_body()
            .collect()
            .await
            .expect("duplicate editable response body")
            .to_bytes();
        let public: Value = serde_json::from_slice(&public).expect("duplicate editable JSON");
        assert_eq!(public["id"], "same-task");
        assert_eq!(public["status"], "error");
        assert_eq!(public["error"], GENERIC_TASK_ERROR);
        assert_eq!(
            fs::read(&task_path).expect("editable idempotency after bytes"),
            before
        );
        assert!(FORCE_NEXT_EDITABLE_ADMISSION_FAILURE.load(Ordering::Acquire));
        fs::remove_dir_all(root).expect("editable idempotency cleanup");
    }
}
