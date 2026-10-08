use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};

use tokio::sync::Mutex;

use serde_json::{Value, json};

type ClearanceKey = (String, String);
type ClearanceFlight = Arc<tokio::sync::Notify>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProxyProfile {
    pub(crate) proxy_url: String,
    pub(crate) proxy_source: String,
    pub(crate) resource: bool,
    pub(crate) runtime_enabled: bool,
    pub(crate) egress_mode: String,
    pub(crate) skip_ssl_verify: bool,
    pub(crate) reset_session_status_codes: Vec<u16>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ClearanceBundle {
    pub(crate) target_host: String,
    pub(crate) proxy_url: String,
    pub(crate) cookies: HashMap<String, String>,
    pub(crate) user_agent: String,
    pub(crate) expires_at: Option<Instant>,
}

#[derive(Clone, Default)]
pub(crate) struct ClearanceStore {
    entries: Arc<Mutex<HashMap<ClearanceKey, ClearanceBundle>>>,
    flights: Arc<StdMutex<HashMap<ClearanceKey, ClearanceFlight>>>,
}

struct ClearanceFlightOwner {
    flights: Arc<StdMutex<HashMap<ClearanceKey, ClearanceFlight>>>,
    key: ClearanceKey,
    notify: ClearanceFlight,
}

impl ClearanceFlightOwner {
    fn new(
        flights: Arc<StdMutex<HashMap<ClearanceKey, ClearanceFlight>>>,
        key: ClearanceKey,
        notify: ClearanceFlight,
    ) -> Self {
        Self {
            flights,
            key,
            notify,
        }
    }
}

impl Drop for ClearanceFlightOwner {
    fn drop(&mut self) {
        let mut flights = self
            .flights
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if flights
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.notify))
        {
            flights.remove(&self.key);
        }
        drop(flights);
        self.notify.notify_waiters();
    }
}

impl ClearanceStore {
    async fn wait_for_flight(
        &self,
        key: &ClearanceKey,
        proxy_url: &str,
        target_url: &str,
        notify: &ClearanceFlight,
    ) -> Option<ClearanceBundle> {
        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(bundle) = self.get(proxy_url, target_url).await {
                return Some(bundle);
            }
            let still_running = self
                .flights
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, notify));
            if !still_running {
                return self.get(proxy_url, target_url).await;
            }
            notified.await;
        }
    }

    pub(crate) async fn get(&self, proxy_url: &str, target_url: &str) -> Option<ClearanceBundle> {
        let key = (normalize_proxy_url(proxy_url), normalize_host(target_url));
        let mut entries = self.entries.lock().await;
        let bundle = entries.get(&key).cloned();
        if bundle.as_ref().is_some_and(|item| {
            item.expires_at
                .is_some_and(|expires_at| Instant::now() >= expires_at)
        }) {
            entries.remove(&key);
            return None;
        }
        bundle
    }

    pub(crate) async fn put(
        &self,
        proxy_url: &str,
        target_url: &str,
        mut bundle: ClearanceBundle,
        refresh_interval: u64,
    ) {
        bundle.target_host = normalize_host(target_url);
        bundle.proxy_url = normalize_proxy_url(proxy_url);
        bundle.expires_at =
            (refresh_interval > 0).then(|| Instant::now() + Duration::from_secs(refresh_interval));
        self.entries.lock().await.insert(
            (bundle.proxy_url.clone(), bundle.target_host.clone()),
            bundle,
        );
    }

    pub(crate) async fn invalidate(&self, proxy_url: &str, target_url: &str) {
        self.entries
            .lock()
            .await
            .remove(&(normalize_proxy_url(proxy_url), normalize_host(target_url)));
    }

    pub(crate) async fn refresh_flaresolverr(
        &self,
        client: &reqwest::Client,
        endpoint: &str,
        target_url: &str,
        proxy_url: &str,
        timeout_sec: u64,
        refresh_interval: u64,
    ) -> Option<ClearanceBundle> {
        let key = (normalize_proxy_url(proxy_url), normalize_host(target_url));
        if let Some(bundle) = self.get(proxy_url, target_url).await {
            return Some(bundle);
        }
        let (notify, owner) = {
            let mut flights = self
                .flights
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(notify) = flights.get(&key) {
                (notify.clone(), false)
            } else {
                let notify = Arc::new(tokio::sync::Notify::new());
                flights.insert(key.clone(), notify.clone());
                (notify, true)
            }
        };
        if !owner {
            return self
                .wait_for_flight(&key, proxy_url, target_url, &notify)
                .await;
        }
        let _flight_owner =
            ClearanceFlightOwner::new(self.flights.clone(), key.clone(), notify.clone());
        async {
            let endpoint = endpoint.trim_end_matches('/');
            if endpoint.is_empty() {
                return None;
            }
            let response = client
                .post(format!("{endpoint}/v1"))
                .json(&flaresolverr_payload(target_url, proxy_url, timeout_sec))
                .send()
                .await
                .ok()?;
            if !response.status().is_success() {
                return None;
            }
            let value = response.json::<Value>().await.ok()?;
            let bundle = parse_flaresolverr_bundle(&value, target_url, proxy_url)?;
            self.put(proxy_url, target_url, bundle, refresh_interval)
                .await;
            self.get(proxy_url, target_url).await
        }
        .await
    }

    pub(crate) async fn hosts(&self) -> Vec<String> {
        let mut hosts = self
            .entries
            .lock()
            .await
            .values()
            .map(|bundle| bundle.target_host.clone())
            .collect::<Vec<_>>();
        hosts.sort();
        hosts.dedup();
        hosts
    }
    pub(crate) fn cached_hosts_now(&self) -> Vec<String> {
        let Ok(entries) = self.entries.try_lock() else {
            return Vec::new();
        };
        let now = Instant::now();
        let mut hosts = entries
            .values()
            .filter(|bundle| bundle.expires_at.is_none_or(|expires_at| now < expires_at))
            .map(|bundle| bundle.target_host.clone())
            .collect::<Vec<_>>();
        hosts.sort();
        hosts.dedup();
        hosts
    }
}

fn runtime_bool(value: Option<&Value>, default: bool) -> bool {
    match value {
        None | Some(Value::Null) => default,
        Some(Value::Bool(value)) => *value,
        Some(Value::Number(value)) => value.as_f64().is_some_and(|value| value != 0.0),
        // Python's `bool(value)` treats every non-empty string as true,
        // including strings such as "false" and "0".
        Some(Value::String(value)) => !value.is_empty(),
        Some(Value::Array(value)) => !value.is_empty(),
        Some(Value::Object(value)) => !value.is_empty(),
    }
}

pub(crate) fn profile_from_runtime(
    runtime: &Value,
    account_proxy: Option<&str>,
    explicit_proxy: Option<&str>,
    legacy_proxy: Option<&str>,
    resource: bool,
    upstream: bool,
) -> ProxyProfile {
    let object = runtime.as_object();
    let enabled = runtime_bool(object.and_then(|value| value.get("enabled")), false);
    let egress_mode = object
        .and_then(|value| value.get("egress_mode"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| value.eq_ignore_ascii_case("single_proxy"))
        .map(|_| "single_proxy")
        .unwrap_or("direct")
        .to_owned();
    let runtime_proxy = if upstream && enabled && egress_mode == "single_proxy" {
        let key = if resource {
            "resource_proxy_url"
        } else {
            "proxy_url"
        };
        object
            .and_then(|value| value.get(key))
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                object
                    .and_then(|value| value.get("proxy_url"))
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
            })
    } else {
        None
    };
    let (proxy_url, proxy_source) = [
        (account_proxy, "account"),
        (
            runtime_proxy,
            if resource {
                "runtime_resource"
            } else {
                "runtime"
            },
        ),
        (explicit_proxy, "explicit"),
        (legacy_proxy, "global"),
    ]
    .into_iter()
    .find_map(|(value, source)| {
        value
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| (normalize_proxy_url(value), source.to_owned()))
    })
    .unwrap_or_else(|| (String::new(), "direct".to_owned()));
    let reset_session_status_codes = object
        .and_then(|value| value.get("reset_session_status_codes"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| value.as_u64().and_then(|value| u16::try_from(value).ok()))
        .filter(|value| (100..=599).contains(value))
        .collect::<Vec<_>>();
    ProxyProfile {
        proxy_url,
        proxy_source,
        resource,
        runtime_enabled: enabled,
        egress_mode,
        skip_ssl_verify: enabled
            && runtime_bool(object.and_then(|value| value.get("skip_ssl_verify")), false),
        reset_session_status_codes: if reset_session_status_codes.is_empty() {
            vec![403]
        } else {
            reset_session_status_codes
        },
    }
}

pub(crate) fn should_reset_session(profile: &ProxyProfile, status: u16) -> bool {
    profile.reset_session_status_codes.contains(&status)
}

pub(crate) fn normalize_proxy_url(raw: &str) -> String {
    let mut value = raw.trim().to_owned();
    if !value.is_empty() && !value.contains("://") {
        let parts = value.splitn(4, ':').collect::<Vec<_>>();
        if parts.len() == 2
            && !parts[1].is_empty()
            && parts[1].bytes().all(|byte| byte.is_ascii_digit())
        {
            value = format!("http://{value}");
        } else if parts.len() == 4
            && !parts[1].is_empty()
            && parts[1].bytes().all(|byte| byte.is_ascii_digit())
        {
            value = format!(
                "http://{}:{}@{}:{}",
                quote_proxy_component(parts[2]),
                quote_proxy_component(parts[3]),
                parts[0],
                parts[1]
            );
        }
    }
    if value
        .get(..8)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("socks://"))
    {
        return format!("socks5h://{}", &value[8..]);
    }
    if value
        .get(..9)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("socks5://"))
    {
        return format!("socks5h://{}", &value[9..]);
    }
    value
}

fn quote_proxy_component(raw: &str) -> String {
    let mut encoded = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

pub(crate) fn normalize_host(raw: &str) -> String {
    let value = raw.trim();
    let candidate = if value.contains("://") {
        value.to_owned()
    } else {
        format!("https://{value}")
    };
    if let Ok(url) = url::Url::parse(&candidate)
        && let Some(host) = url.host_str()
    {
        return host.trim_matches('.').to_ascii_lowercase();
    }
    let value = value.to_ascii_lowercase();
    let value = value
        .strip_prefix("https://")
        .or_else(|| value.strip_prefix("http://"))
        .unwrap_or(&value);
    value
        .split('/')
        .next()
        .unwrap_or(value)
        .trim_matches('.')
        .to_owned()
}

pub(crate) fn domain_matches(host: &str, domain: &str) -> bool {
    let host = normalize_host(host);
    let domain = normalize_host(domain).trim_start_matches('.').to_owned();
    domain.is_empty() || host == domain || host.ends_with(&format!(".{domain}"))
}

pub(crate) fn parse_cookie_header(raw: &str) -> HashMap<String, String> {
    raw.split(';')
        .filter_map(|part| {
            let (name, value) = part.trim().split_once('=')?;
            let name = name.trim();
            (!name.is_empty()).then(|| (name.to_owned(), value.trim().to_owned()))
        })
        .collect()
}

pub(crate) fn cookie_header(cookies: &HashMap<String, String>) -> String {
    let mut names = cookies.keys().cloned().collect::<Vec<_>>();
    names.sort();
    names
        .into_iter()
        .filter_map(|name| cookies.get(&name).map(|value| format!("{name}={value}")))
        .collect::<Vec<_>>()
        .join("; ")
}

pub(crate) fn merge_cookie_header(existing: &str, additions: &HashMap<String, String>) -> String {
    let existing = existing.trim();
    let existing_names = parse_cookie_header(existing)
        .into_keys()
        .collect::<std::collections::HashSet<_>>();
    let mut additions = additions
        .iter()
        .filter(|(name, _)| !name.is_empty() && !existing_names.contains(*name))
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>();
    additions.sort();
    if existing.is_empty() {
        return additions.join("; ");
    }
    if additions.is_empty() {
        return existing.to_owned();
    }
    format!(
        "{}; {}",
        existing.trim_end_matches([';', ' ']),
        additions.join("; ")
    )
}

pub(crate) fn flaresolverr_payload(target_url: &str, proxy_url: &str, timeout_sec: u64) -> Value {
    let mut payload = json!({
        "cmd": "request.get",
        "url": target_url,
        "maxTimeout": timeout_sec.saturating_mul(1000),
    });
    if !proxy_url.trim().is_empty() {
        payload["proxy"] = json!({"url": normalize_proxy_url(proxy_url)});
    }
    payload
}

pub(crate) fn parse_flaresolverr_bundle(
    value: &Value,
    target_url: &str,
    proxy_url: &str,
) -> Option<ClearanceBundle> {
    let status = value
        .get("status")
        .map(|value| super::protocol_anthropic::python_text(Some(value)))
        .unwrap_or_default();
    if !status.eq_ignore_ascii_case("ok") {
        return None;
    }
    let solution = value.get("solution").and_then(Value::as_object)?;
    let target_host = normalize_host(target_url);
    let mut cookies = HashMap::new();
    if let Some(raw_cookies) = solution.get("cookies").and_then(Value::as_array) {
        for cookie in raw_cookies {
            let Some(object) = cookie.as_object() else {
                continue;
            };
            let name = object
                .get("name")
                .map(|value| super::protocol_anthropic::python_text(Some(value)))
                .unwrap_or_default();
            let name = name.trim();
            if name.is_empty() {
                continue;
            }
            let value = object
                .get("value")
                .map(|value| super::protocol_anthropic::python_text(Some(value)))
                .unwrap_or_default();
            let domain = object
                .get("domain")
                .map(|value| super::protocol_anthropic::python_text(Some(value)))
                .unwrap_or_default();
            if !name.is_empty() && (domain.is_empty() || domain_matches(&target_host, &domain)) {
                cookies.insert(name.to_owned(), value.to_owned());
            }
        }
    }
    let user_agent = solution
        .get("userAgent")
        .map(|value| super::protocol_anthropic::python_text(Some(value)))
        .unwrap_or_default()
        .trim()
        .to_owned();
    (!cookies.is_empty() || !user_agent.is_empty()).then_some(ClearanceBundle {
        target_host,
        proxy_url: normalize_proxy_url(proxy_url),
        cookies,
        user_agent,
        expires_at: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn clearance_flight_waiters_observe_completion_before_and_after_wait_registration() {
        let store = ClearanceStore::default();
        let proxy = "http://proxy.example.test:8080";
        let target = "https://chatgpt.com/";
        let key = (normalize_proxy_url(proxy), normalize_host(target));
        let notify = Arc::new(tokio::sync::Notify::new());
        store
            .flights
            .lock()
            .expect("flight map lock")
            .insert(key.clone(), notify.clone());

        let waiting_store = store.clone();
        let waiting_key = key.clone();
        let waiting_notify = notify.clone();
        let waiter = tokio::spawn(async move {
            waiting_store
                .wait_for_flight(&waiting_key, proxy, target, &waiting_notify)
                .await
        });
        tokio::task::yield_now().await;

        let guard = ClearanceFlightOwner::new(store.flights.clone(), key.clone(), notify.clone());
        drop(guard);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .expect("registered waiter wakes")
                .expect("waiter task")
                .is_none()
        );

        let already_finished = Arc::new(tokio::sync::Notify::new());
        let finished_key = (normalize_proxy_url(proxy), normalize_host(target));
        store
            .flights
            .lock()
            .expect("flight map lock")
            .insert(finished_key.clone(), already_finished.clone());
        store
            .flights
            .lock()
            .expect("flight map lock")
            .remove(&finished_key);
        already_finished.notify_waiters();
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                store.wait_for_flight(&finished_key, proxy, target, &already_finished),
            )
            .await
            .expect("late waiter does not lose completion")
            .is_none()
        );
    }

    #[test]
    fn proxy_and_cookie_helpers_match_python_precedence() {
        assert_eq!(
            normalize_proxy_url("socks5://proxy:1080"),
            "socks5h://proxy:1080"
        );
        assert_eq!(
            normalize_proxy_url("proxy.example:8080"),
            "http://proxy.example:8080"
        );
        assert_eq!(
            normalize_proxy_url("proxy.example:8080:user name:p@ss:word"),
            "http://user%20name:p%40ss%3Aword@proxy.example:8080"
        );
        let normalized_runtime = profile_from_runtime(
            &json!({
                "enabled":" true ",
                "egress_mode":" SINGLE_PROXY ",
                "proxy_url":"proxy.example:8080",
                "skip_ssl_verify":"yes"
            }),
            None,
            None,
            None,
            false,
            true,
        );
        assert_eq!(normalized_runtime.proxy_source, "runtime");
        assert_eq!(normalized_runtime.proxy_url, "http://proxy.example:8080");
        assert!(normalized_runtime.runtime_enabled);
        assert_eq!(normalized_runtime.egress_mode, "single_proxy");
        assert!(normalized_runtime.skip_ssl_verify);
        assert_eq!(normalize_proxy_url("代理代理"), "代理代理");
        let numeric_runtime = profile_from_runtime(
            &json!({
                "enabled": 1,
                "egress_mode": " SINGLE_PROXY ",
                "proxy_url": "http://numeric-runtime:8080"
            }),
            None,
            None,
            None,
            false,
            true,
        );
        assert_eq!(numeric_runtime.proxy_source, "runtime");
        let mut additions = HashMap::new();
        additions.insert("cf_clearance".to_owned(), "new".to_owned());
        additions.insert("foo".to_owned(), "bar".to_owned());
        assert_eq!(
            merge_cookie_header("foo=old; sid=1", &additions),
            "foo=old; sid=1; cf_clearance=new"
        );
    }

    #[test]
    fn flaresolverr_filters_cookie_domains_and_builds_payload() {
        let payload = flaresolverr_payload("https://chatgpt.com/", "socks5://proxy:1080", 60);
        assert_eq!(payload["cmd"], "request.get");
        assert_eq!(payload["maxTimeout"], 60_000);
        assert_eq!(payload["proxy"]["url"], "socks5h://proxy:1080");
        let bundle = parse_flaresolverr_bundle(
            &json!({
                "status":"ok",
                "solution":{
                    "userAgent":"ua",
                    "cookies":[
                        {"name":"ok","value":"1","domain":".chatgpt.com"},
                        {"name":"bad","value":"2","domain":"other.test"}
                    ]
                }
            }),
            "https://chatgpt.com/",
            "socks5://proxy:1080",
        )
        .expect("bundle");
        assert!(bundle.cookies.contains_key("ok"));
        assert!(!bundle.cookies.contains_key("bad"));
    }
    #[test]
    fn flaresolverr_keeps_user_agent_without_cookies_and_skips_bad_entries() {
        let bundle = parse_flaresolverr_bundle(
            &json!({
                "status": "OK",
                "solution": {
                    "userAgent": " ua-only ",
                    "cookies": [
                        null,
                        {"name":"sid","value":"1","domain":".CHATGPT.COM"},
                        {"value":"missing-name"},
                        {"name":"","value":"empty-name"},
                        {"name":"wrong","value":"2","domain":"other.test"}
                    ]
                }
            }),
            "https://CHATGPT.com./path",
            "http://proxy:8080",
        )
        .expect("UA-only bundle");
        assert_eq!(bundle.user_agent, "ua-only");
        assert_eq!(bundle.cookies.get("sid"), Some(&"1".to_owned()));
        assert!(!bundle.cookies.contains_key("wrong"));
    }
    #[test]
    fn flaresolverr_projection_uses_python_string_coercion() {
        let bundle = parse_flaresolverr_bundle(
            &json!({
                "status": "OK",
                "solution": {
                    "userAgent": 7,
                    "cookies": [{"name": 8, "value": true}]
                }
            }),
            "https://chatgpt.com",
            "",
        )
        .expect("coerced FlareSolverr bundle");
        assert_eq!(bundle.user_agent, "7");
        assert_eq!(bundle.cookies.get("8"), Some(&"True".to_owned()));
    }

    #[test]
    fn merge_cookie_header_preserves_existing_wire_order_and_whitespace() {
        let mut additions = HashMap::new();
        additions.insert("foo".to_owned(), "new".to_owned());
        additions.insert("cf_clearance".to_owned(), "clear".to_owned());
        assert_eq!(
            merge_cookie_header("  sid=1; foo=old;  ", &additions),
            "sid=1; foo=old; cf_clearance=clear"
        );
    }

    #[test]
    fn profile_priority_matches_python_proxy_service() {
        let profile = profile_from_runtime(
            &json!({
                "enabled": true,
                "egress_mode": "single_proxy",
                "proxy_url": "http://runtime:1",
                "resource_proxy_url": "http://resource:2",
                "skip_ssl_verify": true
            }),
            Some("socks5://account:3"),
            Some("http://explicit:4"),
            Some("http://global:5"),
            true,
            true,
        );
        assert_eq!(profile.proxy_source, "account");
        assert_eq!(profile.proxy_url, "socks5h://account:3");
        assert!(profile.skip_ssl_verify);
        let runtime = profile_from_runtime(
            &json!({
                "enabled": true,
                "egress_mode": "single_proxy",
                "proxy_url": "http://runtime:1",
                "resource_proxy_url": "http://resource:2"
            }),
            None,
            None,
            None,
            true,
            true,
        );
        assert_eq!(runtime.proxy_source, "runtime_resource");
        assert_eq!(runtime.proxy_url, "http://resource:2");
        assert!(should_reset_session(&runtime, 403));
        assert!(!should_reset_session(&runtime, 500));
    }

    #[test]
    fn account_session_profile_matches_python_default_session_kwargs() {
        let runtime = json!({
            "enabled": true,
            "egress_mode": "single_proxy",
            "proxy_url": "http://runtime:1",
            "resource_proxy_url": "http://runtime-resource:2",
            "skip_ssl_verify": true
        });
        let global =
            profile_from_runtime(&runtime, None, None, Some("http://global:3"), false, false);
        assert_eq!(global.proxy_source, "global");
        assert_eq!(global.proxy_url, "http://global:3");
        assert!(global.skip_ssl_verify);

        let account = profile_from_runtime(
            &runtime,
            Some("http://account:4"),
            None,
            Some("http://global:3"),
            true,
            false,
        );
        assert_eq!(account.proxy_source, "account");
        assert_eq!(account.proxy_url, "http://account:4");

        let direct = profile_from_runtime(&runtime, None, None, None, false, false);
        assert_eq!(direct.proxy_source, "direct");
        assert!(direct.proxy_url.is_empty());
        assert!(direct.skip_ssl_verify);
    }
}
