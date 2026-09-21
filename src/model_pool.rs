use std::{
    collections::HashMap,
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
    Unknown,
    Configured,
    Web,
    Image,
    Codex,
}

pub(super) const WEB_IMAGE_MODELS: &[&str] = &[
    "gpt-image-2",
    "gpt-image-2.5",
    "gpt-image-2.5-flare",
    "gpt-image-2.5-sunburst",
];

pub(super) fn is_web_image_model_id(id: &str) -> bool {
    WEB_IMAGE_MODELS
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(id.trim()))
}

fn model_provenance_from_object(
    object: &serde_json::Map<String, Value>,
    default: ModelProvenance,
) -> ModelProvenance {
    const CANONICAL_WEB_MODEL_ENDPOINTS: &[&str] = &[
        "/backend-api/models?iim=false&is_gizmo=false&supports_model_picker_upgrade_presets=true",
        "/backend-api/tpp/models/?supports_model_picker_upgrade_presets=true",
    ];
    let mut explicit = None;
    for source in ["provenance", "source", "source_type", "endpoint"]
        .iter()
        .filter_map(|key| object.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
    {
        let provenance = match source.as_str() {
            "codex" | "codex_api" | "codex_endpoint" => Some(ModelProvenance::Codex),
            value if value.contains("/backend-api/codex/models") => Some(ModelProvenance::Codex),
            "image" | "image_generation" => Some(ModelProvenance::Image),
            "configured" | "manual" | "static" => Some(ModelProvenance::Configured),
            "unknown" | "untrusted" | "unavailable" => Some(ModelProvenance::Unknown),
            value if CANONICAL_WEB_MODEL_ENDPOINTS.contains(&value) => Some(ModelProvenance::Web),
            _ => None,
        };
        if provenance == Some(ModelProvenance::Codex) {
            return ModelProvenance::Codex;
        }
        explicit = explicit.or(provenance);
    }
    explicit.unwrap_or(default)
}

pub(super) fn model_provenance_from_value(
    value: Option<&Value>,
    default: ModelProvenance,
) -> ModelProvenance {
    match value {
        Some(Value::Object(object)) => model_provenance_from_object(object, default),
        Some(Value::String(source)) => {
            let mut object = serde_json::Map::new();
            object.insert("source".to_owned(), Value::String(source.clone()));
            model_provenance_from_object(&object, default)
        }
        _ => default,
    }
}

pub(super) fn model_provenance_label(provenance: ModelProvenance) -> &'static str {
    match provenance {
        ModelProvenance::Unknown => "unknown",
        ModelProvenance::Configured => "configured",
        ModelProvenance::Web => "web",
        ModelProvenance::Image => "image",
        ModelProvenance::Codex => "codex",
    }
}

pub(super) fn model_provenance_is_untrusted(provenance: ModelProvenance) -> bool {
    matches!(
        provenance,
        ModelProvenance::Unknown | ModelProvenance::Codex
    )
}

pub(super) fn model_provenance_rank(provenance: ModelProvenance) -> u8 {
    match provenance {
        ModelProvenance::Unknown => 0,
        ModelProvenance::Codex => 0,
        ModelProvenance::Configured => 1,
        ModelProvenance::Web => 2,
        ModelProvenance::Image => 3,
    }
}

pub(super) fn project_imported_model_entries_with_sources(
    value: Option<&Value>,
    sources: Option<&Value>,
    default: ModelProvenance,
) -> Vec<(String, ModelProvenance)> {
    let Some(items) = value.and_then(Value::as_array) else {
        return Vec::new();
    };
    let source_map = sources.and_then(Value::as_object);
    let mut indexes = HashMap::<String, usize>::new();
    let mut entries: Vec<(String, ModelProvenance)> = Vec::new();
    for item in items.iter().take(MAX_MODELS) {
        let (text, mut provenance) = match item {
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
        if let Some(source) = source_map.and_then(|map| map.get(&text)) {
            provenance = model_provenance_from_value(Some(source), provenance);
        }
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

pub(super) fn project_imported_model_entries(
    value: Option<&Value>,
    default: ModelProvenance,
) -> Vec<(String, ModelProvenance)> {
    project_imported_model_entries_with_sources(value, None, default)
}

pub(super) fn project_account_model_entries(
    object: &serde_json::Map<String, Value>,
    default: ModelProvenance,
) -> Vec<(String, ModelProvenance)> {
    let mut entries = project_imported_model_entries_with_sources(
        object.get("models"),
        object.get("model_sources"),
        default,
    );
    let verified_web_catalog = object
        .get("_model_source_version")
        .and_then(Value::as_u64)
        == Some(1)
        && object
        .get("_verified_web_model_paths")
        .and_then(Value::as_array)
        .is_some_and(|paths| {
            !paths.is_empty()
                && paths.iter().all(|path| {
                    path.as_str().is_some_and(|path| {
                        matches!(
                            path,
                            "/backend-api/models?iim=false&is_gizmo=false&supports_model_picker_upgrade_presets=true"
                                | "/backend-api/tpp/models/?supports_model_picker_upgrade_presets=true"
                        )
                    })
                })
        });
    if verified_web_catalog {
        let web_ids = object
            .get("model_sources")
            .and_then(Value::as_object)
            .into_iter()
            .flat_map(|sources| sources.iter())
            .filter_map(|(id, source)| {
                (source.as_str().map(str::trim) == Some("web")).then_some(id.as_str())
            })
            .collect::<std::collections::HashSet<_>>();
        for (id, provenance) in &mut entries {
            if *provenance == ModelProvenance::Unknown && web_ids.contains(id.as_str()) {
                *provenance = ModelProvenance::Web;
            }
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
        .filter(|(_, provenance)| !model_provenance_is_untrusted(*provenance))
        .map(|(id, _)| id)
        .collect()
}

#[derive(Clone, Debug, PartialEq, Serialize)]
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
            if model_provenance_is_untrusted(model.provenance) {
                continue;
            }
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
            provenance: model_provenance_from_object(object, ModelProvenance::Configured),
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
    let mut indexes = HashMap::<String, usize>::new();
    let mut models: Vec<PublicModel> = Vec::new();
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
        model.provenance = if provenance == ModelProvenance::Codex {
            ModelProvenance::Codex
        } else {
            item.as_object()
                .map(|object| model_provenance_from_object(object, provenance))
                .unwrap_or(provenance)
        };
        if model.provenance == ModelProvenance::Web && is_web_image_model_id(&model.id) {
            model.provenance = ModelProvenance::Image;
        }
        if let Some(index) = indexes.get(&model.id).copied() {
            if model_provenance_rank(model.provenance)
                > model_provenance_rank(models[index].provenance)
            {
                models[index] = model;
            }
            continue;
        }
        indexes.insert(model.id.clone(), models.len());
        model.allow_anonymous = allow_anonymous;
        model.supported_account_types = account_type
            .map(|value| vec![value.to_owned()])
            .unwrap_or_default();
        models.push(model);
    }
    (!models.is_empty()).then_some(models)
}
