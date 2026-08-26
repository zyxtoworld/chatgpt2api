use std::{
    env, fs,
    path::{Path, PathBuf},
};

use serde_json::Value;

#[derive(Clone)]
pub struct AppConfig {
    pub version: String,
    pub auth_key: Option<String>,
    pub models: Vec<String>,
    pub upstream_base_url: Option<String>,
    pub upstream_auth: Option<String>,
    pub auth_keys_path: Option<PathBuf>,
    pub models_path: Option<PathBuf>,
    pub accounts_path: Option<PathBuf>,
    pub upstream_protocol: UpstreamProtocol,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UpstreamProtocol {
    #[default]
    OpenAi,
    ChatGpt,
}

impl AppConfig {
    pub fn from_env() -> Self {
        let config_path = config_path();
        let legacy = read_json_object(&config_path);
        let data_dir = runtime_data_dir();
        let auth_key = first_nonempty([
            env::var("RUST_AUTH_KEY").ok(),
            env::var("CHATGPT2API_AUTH_KEY").ok(),
            json_string(&legacy, "auth-key"),
        ]);
        let models = nonempty(env::var("RUST_MODELS").ok())
            .or_else(|| {
                json_string(&legacy, "default_upstream_model_name")
                    .map(|model| format!("auto,{model}"))
            })
            .unwrap_or_else(|| "auto".to_owned())
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        let version = nonempty(env::var("RUST_VERSION").ok())
            .or_else(|| read_version_file(Path::new("/app/VERSION")))
            .or_else(|| read_version_file(Path::new("VERSION")))
            .unwrap_or_else(|| "rust-canary".to_owned());
        Self {
            version,
            auth_key,
            models,
            upstream_base_url: env::var("RUST_UPSTREAM_BASE_URL")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            upstream_auth: env::var("RUST_UPSTREAM_AUTH")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            auth_keys_path: path_override_or_default(
                "RUST_AUTH_KEYS_PATH",
                &data_dir,
                "auth_keys.json",
            ),
            models_path: path_override_or_default("RUST_MODELS_PATH", &data_dir, "models.json"),
            accounts_path: path_override_or_default(
                "RUST_ACCOUNTS_PATH",
                &data_dir,
                "accounts.json",
            ),
            upstream_protocol: match env::var("RUST_UPSTREAM_PROTOCOL")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str()
            {
                "chatgpt" | "native" => UpstreamProtocol::ChatGpt,
                _ => UpstreamProtocol::OpenAi,
            },
        }
    }
}

pub(crate) fn runtime_data_dir() -> PathBuf {
    env::var_os("RUST_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            if Path::new("/app/data").is_dir() {
                PathBuf::from("/app/data")
            } else {
                PathBuf::from("data")
            }
        })
}

fn first_nonempty<const N: usize>(values: [Option<String>; N]) -> Option<String> {
    values
        .into_iter()
        .flatten()
        .map(|value| value.trim().to_owned())
        .find(|value| !value.is_empty())
}

fn nonempty(value: Option<String>) -> Option<String> {
    first_nonempty([value])
}

fn config_path() -> PathBuf {
    env::var_os("CHATGPT2API_CONFIG_FILE")
        .map(PathBuf::from)
        .or_else(|| {
            let app = Path::new("/app/config.json");
            app.is_file().then(|| app.to_owned())
        })
        .unwrap_or_else(|| PathBuf::from("config.json"))
}

fn read_json_object(path: &Path) -> Value {
    fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .filter(Value::is_object)
        .unwrap_or_else(|| Value::Object(Default::default()))
}

fn json_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(ToOwned::to_owned)
}

fn read_version_file(path: &Path) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn path_override_or_default(name: &str, data_dir: &Path, file_name: &str) -> Option<PathBuf> {
    env::var_os(name).map(PathBuf::from).or_else(|| {
        let path = data_dir.join(file_name);
        path.is_file().then_some(path)
    })
}

#[derive(Debug)]
pub enum AppInitError {
    Client(reqwest::Error),
    AuthSnapshot,
    ModelSnapshot,
    AccountSnapshot,
    EditableTaskSnapshot,
    StorageBackend,
}

impl std::fmt::Display for AppInitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Client(_) => formatter.write_str("HTTP client initialization failed"),
            Self::AuthSnapshot => formatter.write_str("authentication snapshot is invalid"),
            Self::ModelSnapshot => formatter.write_str("model snapshot is invalid"),
            Self::AccountSnapshot => formatter.write_str("account snapshot is invalid"),
            Self::EditableTaskSnapshot => formatter.write_str("editable task snapshot is invalid"),
            Self::StorageBackend => formatter.write_str("storage backend initialization failed"),
        }
    }
}

impl std::error::Error for AppInitError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_config_helpers_ignore_invalid_or_empty_values() {
        let value = serde_json::json!({"auth-key":"", "default_upstream_model_name":"gpt-test"});
        assert_eq!(json_string(&value, "auth-key"), None);
        assert_eq!(
            json_string(&value, "default_upstream_model_name"),
            Some("gpt-test".into())
        );
        assert_eq!(
            first_nonempty([Some("  ".into()), Some("ok".into())]),
            Some("ok".into())
        );
    }
}
