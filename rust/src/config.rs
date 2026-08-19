use std::{env, path::PathBuf};

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
        let auth_key = env::var("RUST_AUTH_KEY")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let models = env::var("RUST_MODELS")
            .unwrap_or_else(|_| "auto".to_owned())
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        Self {
            version: env::var("RUST_VERSION").unwrap_or_else(|_| "rust-canary".to_owned()),
            auth_key,
            models,
            upstream_base_url: env::var("RUST_UPSTREAM_BASE_URL")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            upstream_auth: env::var("RUST_UPSTREAM_AUTH")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            auth_keys_path: env::var_os("RUST_AUTH_KEYS_PATH").map(PathBuf::from),
            models_path: env::var_os("RUST_MODELS_PATH").map(PathBuf::from),
            accounts_path: env::var_os("RUST_ACCOUNTS_PATH").map(PathBuf::from),
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

#[derive(Debug)]
pub enum AppInitError {
    Client(reqwest::Error),
    AuthSnapshot,
    ModelSnapshot,
    AccountSnapshot,
}

impl std::fmt::Display for AppInitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Client(_) => formatter.write_str("HTTP client initialization failed"),
            Self::AuthSnapshot => formatter.write_str("authentication snapshot is invalid"),
            Self::ModelSnapshot => formatter.write_str("model snapshot is invalid"),
            Self::AccountSnapshot => formatter.write_str("account snapshot is invalid"),
        }
    }
}

impl std::error::Error for AppInitError {}
