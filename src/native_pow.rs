use super::{DEFAULT_POW_SCRIPT, MAX_POW_SCRIPT_SOURCES, Value};
use serde_json::json;
use std::{
    fs,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use time::{OffsetDateTime, UtcOffset, format_description};

#[derive(Clone, Default)]
pub(crate) struct NativePowResources {
    pub(crate) script_sources: Vec<String>,
    pub(crate) data_build: String,
}

pub(crate) struct NativePowConfigInputs {
    pub(crate) screen_sum: u64,
    pub(crate) legacy_time: String,
    pub(crate) random_value: f64,
    pub(crate) navigator_key: String,
    pub(crate) document_key: String,
    pub(crate) window_key: String,
    pub(crate) performance_ms: f64,
    pub(crate) uuid: String,
    pub(crate) cores: u64,
    pub(crate) epoch_minus_performance_ms: f64,
    pub(crate) edge_flag: u64,
}

pub(crate) fn native_pow_config_from_inputs(
    user_agent: &str,
    resources: &NativePowResources,
    inputs: &NativePowConfigInputs,
) -> Vec<Value> {
    vec![
        json!(inputs.screen_sum),
        json!(inputs.legacy_time),
        json!(4294705152u64),
        json!(1),
        json!(user_agent),
        json!(
            resources
                .script_sources
                .first()
                .map(String::as_str)
                .unwrap_or(DEFAULT_POW_SCRIPT)
        ),
        json!(resources.data_build),
        json!("en-US"),
        json!("en-US,es-US,en,es"),
        json!(inputs.random_value),
        json!(inputs.navigator_key),
        json!(inputs.document_key),
        json!(inputs.window_key),
        json!(inputs.performance_ms),
        json!(inputs.uuid),
        json!(""),
        json!(inputs.cores),
        json!(inputs.epoch_minus_performance_ms),
        json!(0),
        json!(0),
        json!(0),
        json!(0),
        json!(0),
        json!(0),
        json!(inputs.edge_flag),
    ]
}

pub(crate) fn native_pow_config(user_agent: &str, resources: &NativePowResources) -> Vec<Value> {
    native_pow_config_from_inputs(
        user_agent,
        resources,
        &NativePowConfigInputs {
            // These values make the isolated canary deterministic. They are not
            // a claim of browser/runtime parity with Python's live random inputs.
            screen_sum: 3000,
            legacy_time: "Mon Jan 01 2024 00:00:00 GMT-0500 (Eastern Standard Time)".to_owned(),
            random_value: 0.5,
            navigator_key: "hardwareConcurrency−32".to_owned(),
            document_key: "location".to_owned(),
            window_key: "navigator".to_owned(),
            performance_ms: 1234.0,
            uuid: "00000000-0000-4000-8000-000000000001".to_owned(),
            cores: 32,
            epoch_minus_performance_ms: 1_700_000_000_000.0,
            edge_flag: 0,
        },
    )
}

fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    let _ = getrandom::getrandom(&mut bytes);
    bytes
}

fn random_index(length: usize) -> usize {
    if length == 0 {
        return 0;
    }
    usize::from(u16::from_le_bytes(random_bytes::<2>())) % length
}

fn random_unit() -> f64 {
    let value = u64::from_le_bytes(random_bytes::<8>());
    (value as f64) / (u64::MAX as f64)
}

fn random_uuid() -> String {
    let mut bytes = random_bytes::<16>();
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

fn legacy_time() -> String {
    let offset = UtcOffset::from_hms(-5, 0, 0).expect("fixed Eastern offset");
    let now = OffsetDateTime::now_utc().to_offset(offset);
    let description = format_description::parse_borrowed::<2>(
        "[weekday repr:short] [month repr:short] [day padding:zero] [year] [hour repr:24]:[minute]:[second] GMT-0500 (Eastern Standard Time)",
    )
    .expect("valid PoW time format");
    now.format(&description)
        .unwrap_or_else(|_| "Mon Jan  1 1970 00:00:00 GMT-0500 (Eastern Standard Time)".to_owned())
}

fn process_elapsed_ms() -> f64 {
    // Python's time.perf_counter() is monotonic time since system boot, not
    // process uptime. Linux exposes the same clock through /proc/uptime.
    if let Ok(value) = fs::read_to_string("/proc/uptime")
        && let Some(seconds) = value.split_whitespace().next()
        && let Ok(seconds) = seconds.parse::<f64>()
    {
        return seconds * 1000.0;
    }
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_secs_f64() * 1000.0
}

pub(crate) fn native_pow_config_runtime(
    user_agent: &str,
    resources: &NativePowResources,
) -> Vec<Value> {
    // Keep protocol fixtures deterministic while production handshakes use a
    // fresh browser-like snapshot for each request.
    if cfg!(test) {
        return native_pow_config(user_agent, resources);
    }
    const RESOLUTIONS: &[[u64; 2]] = &[[1920, 1080], [1440, 900], [2560, 1440], [3840, 2160]];
    const CORES: &[u64] = &[8, 16, 24, 32];
    const NAVIGATOR_KEYS: &[&str] = &[
        "registerProtocolHandler−function registerProtocolHandler() { [native code] }",
        "storage−[object StorageManager]",
        "locks−[object LockManager]",
        "appCodeName−Mozilla",
        "permissions−[object Permissions]",
        "share−function share() { [native code] }",
        "webdriver−false",
        "managed−[object NavigatorManagedData]",
        "canShare−function canShare() { [native code] }",
        "vendor−Google Inc.",
        "mediaDevices−[object MediaDevices]",
        "vibrate−function vibrate() { [native code] }",
        "storageBuckets−[object StorageBucketManager]",
        "mediaCapabilities−[object MediaCapabilities]",
        "cookieEnabled−true",
        "virtualKeyboard−[object VirtualKeyboard]",
        "product−Gecko",
        "presentation−[object Presentation]",
        "onLine−true",
        "mimeTypes−[object MimeTypeArray]",
        "credentials−[object CredentialsContainer]",
        "serviceWorker−[object ServiceWorkerContainer]",
        "keyboard−[object Keyboard]",
        "gpu−[object GPU]",
        "doNotTrack",
        "serial−[object Serial]",
        "pdfViewerEnabled−true",
        "language−zh-CN",
        "geolocation−[object Geolocation]",
        "userAgentData−[object NavigatorUAData]",
        "getUserMedia−function getUserMedia() { [native code] }",
        "sendBeacon−function sendBeacon() { [native code] }",
        "hardwareConcurrency−32",
        "windowControlsOverlay−[object WindowControlsOverlay]",
    ];
    const DOCUMENT_KEYS: &[&str] = &[
        "__reactContainer$fzelfjyxej8",
        "_reactListening5dehydibo78",
        "location",
    ];
    const WINDOW_KEYS: &[&str] = &[
        "0",
        "window",
        "self",
        "document",
        "name",
        "location",
        "customElements",
        "history",
        "navigation",
        "innerWidth",
        "innerHeight",
        "scrollX",
        "scrollY",
        "visualViewport",
        "screenX",
        "screenY",
        "outerWidth",
        "outerHeight",
        "devicePixelRatio",
        "screen",
        "navigator",
        "onresize",
        "performance",
        "crypto",
        "indexedDB",
        "sessionStorage",
        "localStorage",
        "scheduler",
        "alert",
        "atob",
        "btoa",
        "fetch",
        "matchMedia",
        "postMessage",
        "queueMicrotask",
        "requestAnimationFrame",
        "setInterval",
        "setTimeout",
        "caches",
        "__NEXT_DATA__",
        "__BUILD_MANIFEST",
        "__NEXT_PRELOADREADY",
    ];
    let resolution = RESOLUTIONS[random_index(RESOLUTIONS.len())];
    let performance_ms = process_elapsed_ms();
    let epoch_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs_f64()
        * 1000.0;
    let script = resources
        .script_sources
        .get(random_index(resources.script_sources.len()))
        .cloned()
        .unwrap_or_else(|| DEFAULT_POW_SCRIPT.to_owned());
    native_pow_config_from_inputs(
        user_agent,
        &NativePowResources {
            script_sources: vec![script],
            data_build: resources.data_build.clone(),
        },
        &NativePowConfigInputs {
            screen_sum: resolution[0] + resolution[1],
            legacy_time: legacy_time(),
            random_value: random_unit(),
            navigator_key: NAVIGATOR_KEYS[random_index(NAVIGATOR_KEYS.len())].to_owned(),
            document_key: DOCUMENT_KEYS[random_index(DOCUMENT_KEYS.len())].to_owned(),
            window_key: WINDOW_KEYS[random_index(WINDOW_KEYS.len())].to_owned(),
            performance_ms,
            uuid: random_uuid(),
            cores: CORES[random_index(CORES.len())],
            epoch_minus_performance_ms: epoch_ms - performance_ms,
            edge_flag: 0,
        },
    )
}

fn html_attribute(tag: &str, name: &str) -> Option<String> {
    let bytes = tag.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        while index < bytes.len()
            && (bytes[index].is_ascii_whitespace() || matches!(bytes[index], b'<' | b'/' | b'>'))
        {
            index += 1;
        }
        let name_start = index;
        while index < bytes.len()
            && !bytes[index].is_ascii_whitespace()
            && !matches!(bytes[index], b'=' | b'/' | b'>')
        {
            index += 1;
        }
        if name_start == index {
            index = index.saturating_add(1);
            continue;
        }
        let attribute_name = &tag[name_start..index];
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index >= bytes.len() || bytes[index] != b'=' {
            continue;
        }
        index += 1;
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        let value = if index < bytes.len() && matches!(bytes[index], b'"' | b'\'') {
            let quote = bytes[index];
            index += 1;
            let value_start = index;
            while index < bytes.len() && bytes[index] != quote {
                index += 1;
            }
            let value = tag[value_start..index].to_owned();
            if index < bytes.len() {
                index += 1;
            }
            value
        } else {
            let value_start = index;
            while index < bytes.len() && !bytes[index].is_ascii_whitespace() && bytes[index] != b'>'
            {
                index += 1;
            }
            tag[value_start..index].to_owned()
        };
        if attribute_name.eq_ignore_ascii_case(name) {
            return Some(value);
        }
    }
    None
}

fn pow_data_build_from_source(source: &str) -> Option<String> {
    let start = source.find("c/")?;
    let suffix = &source[start + 2..];
    let component_end = suffix.find("/_")?;
    Some(source[start..start + 2 + component_end + 2].to_owned())
}

pub(crate) fn parse_native_pow_resources(body: &[u8]) -> NativePowResources {
    let html = String::from_utf8_lossy(body);
    let lowercase_html = html.to_ascii_lowercase();
    let mut script_sources = Vec::new();
    let mut cursor = 0usize;
    while script_sources.len() < MAX_POW_SCRIPT_SOURCES {
        let Some(relative_start) = lowercase_html[cursor..].find("<script") else {
            break;
        };
        let start = cursor + relative_start;
        let has_script_tag_boundary = lowercase_html
            .as_bytes()
            .get(start + "<script".len())
            .is_none_or(|byte| byte.is_ascii_whitespace() || matches!(*byte, b'/' | b'>'));
        if !has_script_tag_boundary {
            cursor = start + "<script".len();
            continue;
        }
        let Some(relative_end) = html[start..].find('>') else {
            break;
        };
        let end = start + relative_end;
        if let Some(source) = html_attribute(&html[start..=end], "src")
            && !source.is_empty()
            && source.chars().count() <= 4096
        {
            script_sources.push(source);
        }
        cursor = end + 1;
    }
    if script_sources.is_empty() {
        script_sources.push(DEFAULT_POW_SCRIPT.to_owned());
    }
    let data_build = script_sources
        .iter()
        .find_map(|source| pow_data_build_from_source(source))
        .or_else(|| {
            html.find("<html")
                .or_else(|| lowercase_html.find("<html"))
                .and_then(|start| html[start..].find('>').map(|end| &html[start..start + end]))
                .and_then(|tag| html_attribute(tag, "data-build"))
                .filter(|value| !value.is_empty() && value.chars().count() <= 4096)
        })
        .unwrap_or_default();
    NativePowResources {
        script_sources,
        data_build,
    }
}
