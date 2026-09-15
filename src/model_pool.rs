use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use file_identity::FileVersion;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use super::{
    AppInitError, MAX_MODEL_SNAPSHOT_BYTES, MAX_MODEL_TEXT_LENGTH, MAX_MODELS, bounded_text,
    normalized_list, normalized_reasoning_efforts, parse_created, read_bounded_validated_file,
    validated_file_version,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ModelProvenance {
    Configured,
    Web,
    Image,
    Codex,
}

fn model_provenance_from_object(
    object: &serde_json::Map<String, Value>,
    default: ModelProvenance,
) -> ModelProvenance {
    let source = ["provenance", "source", "source_type", "endpoint"]
        .iter()
        .find_map(|key| object.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase);
    let Some(source) = source.as_deref() else {
        return default;
    };
    match source {
        "codex" | "codex_api" | "codex_endpoint" => ModelProvenance::Codex,
        value if value.contains("/backend-api/codex/models") => ModelProvenance::Codex,
        "image" | "image_generation" => ModelProvenance::Image,
        "web" | "tpp" | "chatgpt_web" => ModelProvenance::Web,
        value
            if value.contains("/backend-api/models")
                || value.contains("/backend-api/tpp/models") =>
        {
            ModelProvenance::Web
        }
        _ => default,
    }
}

fn model_provenance_rank(provenance: ModelProvenance) -> u8 {
    match provenance {
        ModelProvenance::Codex => 0,
        ModelProvenance::Configured => 1,
        ModelProvenance::Web => 2,
        ModelProvenance::Image => 3,
    }
}

pub(super) fn project_imported_model_entries(
    value: Option<&Value>,
    default: ModelProvenance,
) -> Vec<(String, ModelProvenance)> {
    let Some(items) = value.and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut indexes = HashMap::<String, usize>::new();
    let mut entries: Vec<(String, ModelProvenance)> = Vec::new();
    for item in items.iter().take(MAX_MODELS) {
        let (text, provenance) = match item {
            Value::String(text) => (text.as_str(), default),
            Value::Object(object) => {
                let Some(text) = ["id", "model", "slug"]
                    .iter()
                    .find_map(|key| object.get(*key).and_then(Value::as_str))
                else {
                    continue;
                };
                (text, model_provenance_from_object(object, default))
            }
            _ => continue,
        };
        let Some(text) = bounded_text(Some(&Value::String(text.to_owned())), MAX_MODEL_TEXT_LENGTH)
        else {
            continue;
        };
        if let Some(index) = indexes.get(&text).copied() {
            if model_provenance_rank(provenance) > model_provenance_rank(entries[index].1) {
                entries[index].1 = provenance;
            }
        } else {
            indexes.insert(text.clone(), entries.len());
            entries.push((text, provenance));
        }
    }
    entries
}

pub(super) fn project_imported_model_ids(
    value: Option<&Value>,
    default: ModelProvenance,
) -> Vec<String> {
    project_imported_model_entries(value, default)
        .into_iter()
        .filter(|(_, provenance)| *provenance != ModelProvenance::Codex)
        .map(|(id, _)| id)
        .collect()
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct PublicModel {
    pub(super) id: String,
    pub(super) object: &'static str,
    pub(super) created: i64,
    pub(super) owned_by: String,
    pub(super) permission: Vec<Value>,
    pub(super) root: String,
    pub(super) parent: Option<String>,
    pub(super) allow_anonymous: bool,
    pub(super) supported_account_types: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(super) supported_reasoning_efforts: Vec<String>,
    #[serde(skip)]
    pub(super) provenance: ModelProvenance,
}

#[derive(Clone)]
struct ModelSnapshot {
    generation: u64,
    fingerprint: [u8; 32],
    file_version: Option<FileVersion>,
    valid: bool,
    models: Arc<Vec<PublicModel>>,
}

#[derive(Clone)]
pub(super) struct ModelStore {
    path: Option<Arc<PathBuf>>,
    snapshot: Arc<RwLock<ModelSnapshot>>,
    reload_gate: Arc<Mutex<()>>,
}

impl ModelStore {
    pub(super) fn load(path: Option<&Path>, configured: &[String]) -> Result<Self, AppInitError> {
        let (models, fingerprint, file_version) =
            ModelCatalog::load_with_fingerprint(path, configured)?;
        Ok(Self {
            path: path.map(|path| Arc::new(path.to_owned())),
            snapshot: Arc::new(RwLock::new(ModelSnapshot {
                generation: 0,
                fingerprint,
                file_version,
                valid: true,
                models: Arc::new(models),
            })),
            reload_gate: Arc::new(Mutex::new(())),
        })
    }

    pub(super) async fn reload(&self) -> bool {
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
            let snapshot = self.snapshot.read().expect("model snapshot lock");
            if snapshot.valid && snapshot.file_version == Some(version) {
                return true;
            }
        }
        let result = tokio::task::spawn_blocking(move || {
            ModelCatalog::load_with_fingerprint(Some(&path), &[])
        })
        .await;
        let mut snapshot = self.snapshot.write().expect("model snapshot lock");
        match result {
            Ok(Ok((models, fingerprint, file_version))) => {
                if !snapshot.valid
                    || snapshot.fingerprint != fingerprint
                    || snapshot.file_version != file_version
                {
                    snapshot.generation = snapshot.generation.saturating_add(1);
                    snapshot.fingerprint = fingerprint;
                    snapshot.file_version = file_version;
                    snapshot.models = Arc::new(models);
                    snapshot.valid = true;
                }
                true
            }
            _ => {
                snapshot.generation = snapshot.generation.saturating_add(1);
                snapshot.valid = false;
                snapshot.models = Arc::new(Vec::new());
                false
            }
        }
    }

    fn invalidate(&self) {
        let mut snapshot = self.snapshot.write().expect("model snapshot lock");
        snapshot.generation = snapshot.generation.saturating_add(1);
        snapshot.valid = false;
        snapshot.models = Arc::new(Vec::new());
    }

    pub(super) fn current(&self) -> Arc<Vec<PublicModel>> {
        self.snapshot
            .read()
            .expect("model snapshot lock")
            .models
            .clone()
    }
}

pub(super) struct ModelCatalog;

type LoadedModelCatalog = (Vec<PublicModel>, [u8; 32], Option<FileVersion>);

impl ModelCatalog {
    pub(super) fn load_with_fingerprint(
        path: Option<&Path>,
        configured: &[String],
    ) -> Result<LoadedModelCatalog, AppInitError> {
        let raw_items = if let Some(path) = path {
            let (bytes, file_version) = read_bounded_validated_file(path, MAX_MODEL_SNAPSHOT_BYTES)
                .map_err(|()| AppInitError::ModelSnapshot)?;
            #[cfg(test)]
            super::record_model_document_parse(path);
            let fingerprint = Sha256::digest(&bytes).into();
            let value: Value =
                serde_json::from_slice(&bytes).map_err(|_| AppInitError::ModelSnapshot)?;
            let items = value
                .as_object()
                .and_then(|object| object.get("data"))
                .and_then(Value::as_array)
                .cloned()
                .ok_or(AppInitError::ModelSnapshot)?;
            (items, fingerprint, Some(file_version))
        } else {
            (
                configured.iter().map(|id| json!({ "id": id })).collect(),
                [0; 32],
                None,
            )
        };
        let (raw_items, fingerprint, file_version) = raw_items;
        if raw_items.len() > MAX_MODELS {
            return Err(AppInitError::ModelSnapshot);
        }
        let mut models = Vec::new();
        let mut seen = std::collections::HashSet::with_capacity(raw_items.len());
        for item in raw_items {
            let Some(model) = Self::project(&item) else {
                continue;
            };
            if seen.insert(model.id.clone()) {
                models.push(model);
            }
        }
        Ok((models, fingerprint, file_version))
    }

    pub(super) fn project(item: &Value) -> Option<PublicModel> {
        let object = item.as_object()?;
        let id = bounded_text(object.get("id"), MAX_MODEL_TEXT_LENGTH)?;
        let owned_by = bounded_text(object.get("owned_by"), MAX_MODEL_TEXT_LENGTH)
            .unwrap_or_else(|| "chatgpt".to_owned());
        let root =
            bounded_text(object.get("root"), MAX_MODEL_TEXT_LENGTH).unwrap_or_else(|| id.clone());
        let parent = bounded_text(object.get("parent"), MAX_MODEL_TEXT_LENGTH);
        let created = parse_created(object.get("created"));
        let mut supported_account_types =
            normalized_list(object.get("supported_account_types"), 64);
        supported_account_types.sort();
        let supported_reasoning_efforts = normalized_reasoning_efforts(object);
        Some(PublicModel {
            id,
            object: "model",
            created,
            owned_by,
            permission: Vec::new(),
            root,
            parent,
            allow_anonymous: object
                .get("allow_anonymous")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            supported_account_types,
            supported_reasoning_efforts,
            provenance: ModelProvenance::Configured,
        })
    }
}

pub(super) fn project_remote_model_list(
    value: &Value,
    field: &str,
    allow_anonymous: bool,
    account_type: Option<&str>,
    native_chatgpt: bool,
    codex_api_only: bool,
) -> Option<Vec<PublicModel>> {
    project_remote_model_list_with_provenance(
        value,
        field,
        allow_anonymous,
        account_type,
        native_chatgpt,
        codex_api_only,
        if native_chatgpt {
            ModelProvenance::Web
        } else {
            ModelProvenance::Configured
        },
    )
}

pub(super) fn project_remote_model_list_with_provenance(
    value: &Value,
    field: &str,
    allow_anonymous: bool,
    account_type: Option<&str>,
    native_chatgpt: bool,
    codex_api_only: bool,
    provenance: ModelProvenance,
) -> Option<Vec<PublicModel>> {
    let items = value.get(field).and_then(Value::as_array)?;
    let mut seen = HashSet::new();
    let mut models = Vec::new();
    for raw_item in items.iter().take(MAX_MODELS) {
        let mut item = raw_item.clone();
        if codex_api_only && item.get("supported_in_api").and_then(Value::as_bool) != Some(true) {
            continue;
        }
        if native_chatgpt {
            let Some(object) = item.as_object_mut() else {
                continue;
            };
            if !object.contains_key("id") {
                let Some(slug) = ["slug", "model_slug", "model_id", "model"]
                    .iter()
                    .find_map(|key| object.get(*key).and_then(Value::as_str))
                else {
                    continue;
                };
                object.insert("id".to_owned(), Value::String(slug.to_owned()));
            }
        }
        let Some(mut model) = ModelCatalog::project(&item) else {
            continue;
        };
        model.provenance = provenance;
        if !seen.insert(model.id.clone()) {
            continue;
        }
        model.allow_anonymous = allow_anonymous;
        model.supported_account_types = account_type
            .map(|value| vec![value.to_owned()])
            .unwrap_or_default();
        models.push(model);
    }
    (!models.is_empty()).then_some(models)
}
