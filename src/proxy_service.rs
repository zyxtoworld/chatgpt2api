use std::collections::HashMap;

use serde_json::{Value, json};

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
    let enabled = object
        .and_then(|value| value.get("enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let egress_mode = object
        .and_then(|value| value.get("egress_mode"))
        .and_then(Value::as_str)
        .filter(|value| *value == "single_proxy")
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
            && object
                .and_then(|value| value.get("skip_ssl_verify"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
        reset_session_status_codes: if reset_session_status_codes.is_empty() {
            vec![403]
        } else {
            reset_session_status_codes
        },
    }
}

pub(crate) fn should_reset_session(profile: &ProxyProfile, status: u16) -> bool {
    profile
        .reset_session_status_codes
        .contains(&status)
}

pub(crate) fn normalize_proxy_url(raw: &str) -> String {
    let value = raw.trim();
    if value.len() >= 8 && value[..8].eq_ignore_ascii_case("socks://") {
        return format!("socks5h://{}", &value[8..]);
    }
    if value.len() >= 9 && value[..9].eq_ignore_ascii_case("socks5://") {
        return format!("socks5h://{}", &value[9..]);
    }
    value.to_owned()
}

pub(crate) fn normalize_host(raw: &str) -> String {
    let value = raw.trim().to_ascii_lowercase();
    let value = value
        .strip_prefix("https://")
        .or_else(|| value.strip_prefix("http://"))
        .unwrap_or(&value);
    value.split('/').next().unwrap_or(value).to_owned()
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
    let mut merged = parse_cookie_header(existing);
    for (name, value) in additions {
        merged.entry(name.clone()).or_insert_with(|| value.clone());
    }
    cookie_header(&merged)
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
    if value.get("status").and_then(Value::as_str) != Some("ok") {
        return None;
    }
    let solution = value.get("solution").and_then(Value::as_object)?;
    let target_host = normalize_host(target_url);
    let mut cookies = HashMap::new();
    for cookie in solution.get("cookies")?.as_array()? {
        let object = cookie.as_object()?;
        let name = object.get("name").and_then(Value::as_str)?.trim();
        let value = object
            .get("value")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let domain = object
            .get("domain")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !name.is_empty() && (domain.is_empty() || domain_matches(&target_host, domain)) {
            cookies.insert(name.to_owned(), value.to_owned());
        }
    }
    let user_agent = solution
        .get("userAgent")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    (!cookies.is_empty() || !user_agent.is_empty()).then_some(ClearanceBundle {
        target_host,
        proxy_url: normalize_proxy_url(proxy_url),
        cookies,
        user_agent,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_and_cookie_helpers_match_python_precedence() {
        assert_eq!(
            normalize_proxy_url("socks5://proxy:1080"),
            "socks5h://proxy:1080"
        );
        let mut additions = HashMap::new();
        additions.insert("cf_clearance".to_owned(), "new".to_owned());
        additions.insert("foo".to_owned(), "bar".to_owned());
        assert_eq!(
            merge_cookie_header("foo=old; sid=1", &additions),
            "cf_clearance=new; foo=old; sid=1"
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
}
