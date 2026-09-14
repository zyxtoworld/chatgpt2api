use std::{
    env, fs,
    path::{Path, PathBuf},
};

use serde_json::{Value, json};

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

pub(crate) const DEFAULT_PROXY_RUNTIME_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36";

pub(crate) fn proxy_runtime_defaults_from_environment() -> Value {
    proxy_runtime_defaults_with(&|name| env::var(name).ok())
}

fn proxy_runtime_defaults_with(get: &impl Fn(&str) -> Option<String>) -> Value {
    let clearance_enabled = parse_bool(get("CHATGPT2API_PROXY_RUNTIME_CLEARANCE_ENABLED"), false);
    let clearance_mode = if !clearance_enabled {
        "none".to_owned()
    } else {
        match text_or(
            get("CHATGPT2API_PROXY_RUNTIME_CLEARANCE_MODE"),
            "flaresolverr",
        )
        .to_ascii_lowercase()
        .as_str()
        {
            "none" | "manual" | "flaresolverr" => text_or(
                get("CHATGPT2API_PROXY_RUNTIME_CLEARANCE_MODE"),
                "flaresolverr",
            )
            .to_ascii_lowercase(),
            _ => "flaresolverr".to_owned(),
        }
    };
    let egress_mode = match text_or(get("CHATGPT2API_PROXY_RUNTIME_EGRESS_MODE"), "direct")
        .to_ascii_lowercase()
        .as_str()
    {
        "single_proxy" => "single_proxy",
        _ => "direct",
    };
    let reset_session_status_codes = get("CHATGPT2API_PROXY_RUNTIME_RESET_STATUS_CODES")
        .unwrap_or_else(|| "403".to_owned())
        .split(',')
        .filter_map(|part| part.trim().parse::<u16>().ok())
        .filter(|status| (100..=599).contains(status))
        .collect::<Vec<_>>();
    let reset_session_status_codes = if reset_session_status_codes.is_empty() {
        vec![403]
    } else {
        reset_session_status_codes
    };

    json!({
        "enabled": parse_bool(get("CHATGPT2API_PROXY_RUNTIME_ENABLED"), false),
        "egress_mode": egress_mode,
        "proxy_url": text_or(get("CHATGPT2API_PROXY_RUNTIME_PROXY_URL"), ""),
        "resource_proxy_url": text_or(get("CHATGPT2API_PROXY_RUNTIME_RESOURCE_PROXY_URL"), ""),
        "skip_ssl_verify": parse_bool(get("CHATGPT2API_PROXY_RUNTIME_SKIP_SSL_VERIFY"), false),
        "reset_session_status_codes": reset_session_status_codes,
        "clearance": {
            "enabled": clearance_enabled,
            "mode": clearance_mode,
            "cf_cookies": "",
            "cf_clearance": "",
            "user_agent": text_or(
                get("CHATGPT2API_PROXY_RUNTIME_USER_AGENT"),
                DEFAULT_PROXY_RUNTIME_USER_AGENT,
            ),
            "browser": text_or(get("CHATGPT2API_PROXY_RUNTIME_BROWSER"), "chrome"),
            "flaresolverr_url": text_or(get("CHATGPT2API_FLARESOLVERR_URL"), ""),
            "timeout_sec": parse_u64(
                get("CHATGPT2API_PROXY_RUNTIME_CLEARANCE_TIMEOUT_SEC"),
                60,
                1,
            ),
            "refresh_interval": parse_u64(
                get("CHATGPT2API_PROXY_RUNTIME_CLEARANCE_REFRESH_INTERVAL"),
                3600,
                60,
            ),
            "warm_up_on_start": parse_bool(
                get("CHATGPT2API_PROXY_RUNTIME_WARM_UP_ON_START"),
                false,
            ),
        }
    })
}

fn parse_bool(value: Option<String>, default: bool) -> bool {
    match value
        .as_deref()
        .map(str::trim)
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        Some("1" | "true" | "yes" | "on") => true,
        Some("0" | "false" | "no" | "off") => false,
        _ => default,
    }
}

fn parse_u64(value: Option<String>, default: u64, minimum: u64) -> u64 {
    value
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|value| value.max(minimum))
        .unwrap_or(default)
}

fn text_or(value: Option<String>, default: &str) -> String {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned())
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

    #[test]
    fn proxy_runtime_defaults_are_derived_from_environment_without_persistence() {
        let values = [
            ("CHATGPT2API_PROXY_RUNTIME_ENABLED", "true"),
            ("CHATGPT2API_PROXY_RUNTIME_EGRESS_MODE", "single_proxy"),
            ("CHATGPT2API_PROXY_RUNTIME_PROXY_URL", "http://privoxy:8118"),
            ("CHATGPT2API_PROXY_RUNTIME_CLEARANCE_ENABLED", "true"),
            ("CHATGPT2API_PROXY_RUNTIME_CLEARANCE_MODE", "flaresolverr"),
            ("CHATGPT2API_FLARESOLVERR_URL", "http://flaresolverr:8191"),
        ];
        let runtime = proxy_runtime_defaults_with(&|name| {
            values
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
        });

        assert_eq!(
            runtime,
            json!({
                "enabled": true,
                "egress_mode": "single_proxy",
                "proxy_url": "http://privoxy:8118",
                "resource_proxy_url": "",
                "skip_ssl_verify": false,
                "reset_session_status_codes": [403],
                "clearance": {
                    "enabled": true,
                    "mode": "flaresolverr",
                    "cf_cookies": "",
                    "cf_clearance": "",
                    "user_agent": DEFAULT_PROXY_RUNTIME_USER_AGENT,
                    "browser": "chrome",
                    "flaresolverr_url": "http://flaresolverr:8191",
                    "timeout_sec": 60,
                    "refresh_interval": 3600,
                    "warm_up_on_start": false,
                },
            })
        );
    }

    #[test]
    fn proxy_runtime_defaults_are_safe_when_warp_environment_is_absent() {
        let runtime = proxy_runtime_defaults_with(&|_| None);
        assert_eq!(runtime["enabled"], false);
        assert_eq!(runtime["egress_mode"], "direct");
        assert_eq!(runtime["proxy_url"], "");
        assert_eq!(runtime["clearance"]["mode"], "none");
        assert_eq!(runtime["clearance"]["flaresolverr_url"], "");
    }
}
