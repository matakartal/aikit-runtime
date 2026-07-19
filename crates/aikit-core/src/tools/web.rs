//! Governed web and browser tools with deny-by-default network access.

use crate::{AikitError, Result, ToolExecutor, ToolSpec};
use async_trait::async_trait;
use futures::StreamExt;
use regex::Regex;
use reqwest::header::{CONTENT_TYPE, LOCATION};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    time::Duration,
};
use url::Url;

const MAX_REDIRECTS: usize = 5;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_URL_BYTES: usize = 8 * 1024;
const MAX_SEARCH_QUERY_BYTES: usize = 4 * 1024;
const MAX_BROWSER_SELECTOR_BYTES: usize = 4 * 1024;
const MAX_BROWSER_TYPE_BYTES: usize = 64 * 1024;
const MAX_WEBDRIVER_SESSION_ID_BYTES: usize = 256;
const MAX_WEBDRIVER_ELEMENT_ID_BYTES: usize = 1024;
const MAX_WEBDRIVER_RESPONSE_BYTES: usize = 2 * 1024 * 1024;

fn bounded<'a>(value: &'a str, label: &str, max_bytes: usize) -> Result<&'a str> {
    if value.len() > max_bytes {
        return Err(AikitError::ToolExecution(format!(
            "{label} exceeds {max_bytes} bytes"
        )));
    }
    Ok(value)
}

fn denied_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => denied_ipv4(ip),
        IpAddr::V6(ip) => denied_ipv6(ip),
    }
}

fn denied_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, _, _] = ip.octets();
    ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_unspecified()
        || ip.is_multicast()
        || a == 0
        || (a == 100 && (64..=127).contains(&b))
        || (a == 192 && b == 0)
        || (a == 192 && b == 88)
        || (a == 198 && (18..=19).contains(&b))
        || a >= 240
}

fn denied_ipv6(ip: Ipv6Addr) -> bool {
    if let Some(mapped) = ip.to_ipv4_mapped() {
        return denied_ipv4(mapped);
    }
    let segments = ip.segments();
    if segments[..6].iter().all(|segment| *segment == 0) {
        let embedded = Ipv4Addr::new(
            (segments[6] >> 8) as u8,
            segments[6] as u8,
            (segments[7] >> 8) as u8,
            segments[7] as u8,
        );
        return denied_ipv4(embedded);
    }
    let first = segments[0];
    ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_unique_local()
        || ip.is_unicast_link_local()
        || ip.is_multicast()
        || (first & 0xffc0) == 0xfec0
        || (first == 0x0064 && segments[1] == 0xff9b)
        || first == 0x2002
        || (first == 0x2001 && matches!(segments[1], 0x0000 | 0x0db8))
}

fn allowed_url(raw: &str, hosts: &BTreeSet<String>) -> Result<Url> {
    bounded(raw, "URL", MAX_URL_BYTES)?;
    let url = Url::parse(raw).map_err(|e| AikitError::ToolExecution(e.to_string()))?;
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        return Err(AikitError::ToolExecution(
            "URL must use HTTPS and contain no credentials".into(),
        ));
    }
    if url.port_or_known_default() != Some(443) {
        return Err(AikitError::ToolExecution(
            "web URLs must use the standard HTTPS port 443".into(),
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| AikitError::ToolExecution("URL has no host".into()))?
        .to_ascii_lowercase();
    if !hosts.contains(&host) {
        return Err(AikitError::ToolExecution(format!(
            "host '{host}' is not allowlisted"
        )));
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        if denied_ip(ip) {
            return Err(AikitError::ToolExecution(
                "private and local IP addresses are denied".into(),
            ));
        }
    }
    Ok(url)
}

async fn pinned_https_client(url: &Url) -> Result<reqwest::Client> {
    let host = url
        .host_str()
        .ok_or_else(|| AikitError::ToolExecution("URL has no host".into()))?
        .to_owned();
    let lookup_host = host.clone();
    let lookup = tokio::task::spawn_blocking(move || {
        (lookup_host.as_str(), 443)
            .to_socket_addrs()
            .map(|addresses| addresses.collect::<Vec<_>>())
    });
    let addresses = tokio::time::timeout(CONNECT_TIMEOUT, lookup)
        .await
        .map_err(|_| AikitError::ToolExecution("DNS lookup timed out".into()))?
        .map_err(|error| AikitError::ToolExecution(format!("DNS lookup task failed: {error}")))?
        .map_err(|error| AikitError::ToolExecution(format!("DNS lookup failed: {error}")))?;
    if addresses.is_empty() {
        return Err(AikitError::ToolExecution(
            "DNS lookup returned no addresses".into(),
        ));
    }
    if addresses.iter().any(|address| denied_ip(address.ip())) {
        return Err(AikitError::ToolExecution(
            "DNS resolved to a private, local, or non-routable address".into(),
        ));
    }
    let pinned: Vec<SocketAddr> = addresses
        .into_iter()
        .map(|address| SocketAddr::new(address.ip(), 443))
        .collect();
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .resolve_to_addrs(&host, &pinned)
        .build()
        .map_err(|error| AikitError::ToolExecution(error.to_string()))
}

fn redirect_target(current: &Url, location: &str, hosts: &BTreeSet<String>) -> Result<Url> {
    let next = current
        .join(location)
        .map_err(|e| AikitError::ToolExecution(format!("invalid redirect target: {e}")))?;
    allowed_url(next.as_str(), hosts)
}

/// Lightweight retrieval plus optional search through a caller-owned HTTPS endpoint.
pub struct WebTools {
    hosts: BTreeSet<String>,
    search_endpoint: Option<String>,
    max_bytes: usize,
}

impl WebTools {
    pub fn new(hosts: impl IntoIterator<Item = impl Into<String>>) -> Result<Self> {
        Ok(Self {
            hosts: hosts
                .into_iter()
                .map(|host| host.into().to_ascii_lowercase())
                .collect(),
            search_endpoint: None,
            max_bytes: 1_000_000,
        })
    }

    pub fn with_search_endpoint(mut self, endpoint: impl Into<String>) -> Result<Self> {
        let endpoint = endpoint.into();
        bounded(&endpoint, "search endpoint", MAX_URL_BYTES)?;
        if endpoint.matches("{query}").count() != 1 {
            return Err(AikitError::ToolExecution(
                "search endpoint needs exactly one {query} marker".into(),
            ));
        }
        allowed_url(&endpoint.replace("{query}", "probe"), &self.hosts)?;
        self.search_endpoint = Some(endpoint);
        Ok(self)
    }

    pub fn with_max_response_bytes(mut self, bytes: usize) -> Self {
        self.max_bytes = bytes.max(1);
        self
    }

    pub fn specs(&self) -> Vec<ToolSpec> {
        let mut specs = vec![ToolSpec::new(
            "WebFetch",
            "Fetch readable text from an allowlisted HTTPS URL",
            json!({"type":"object","properties":{"url":{"type":"string","maxLength":MAX_URL_BYTES}},"required":["url"],"additionalProperties":false}),
        )];
        if self.search_endpoint.is_some() {
            specs.push(ToolSpec::new(
                "WebSearch",
                "Search through the configured allowlisted endpoint",
                json!({"type":"object","properties":{"query":{"type":"string","maxLength":MAX_SEARCH_QUERY_BYTES}},"required":["query"],"additionalProperties":false}),
            ));
        }
        specs
    }

    async fn fetch(&self, raw: &str) -> Result<String> {
        let mut url = allowed_url(raw, &self.hosts)?;
        let mut redirects = 0usize;
        let response = loop {
            let client = pinned_https_client(&url).await?;
            let response = client
                .get(url.clone())
                .send()
                .await
                .map_err(|e| AikitError::ToolExecution(e.to_string()))?;
            if !response.status().is_redirection() {
                // Keep this postcondition even though redirects are disabled on the client. It
                // prevents a future client-policy change from silently bypassing the allowlist.
                allowed_url(response.url().as_str(), &self.hosts)?;
                break response;
            }
            let location = response
                .headers()
                .get(LOCATION)
                .ok_or_else(|| {
                    AikitError::ToolExecution("web redirect has no Location header".into())
                })?
                .to_str()
                .map_err(|_| {
                    AikitError::ToolExecution("web redirect has an invalid Location header".into())
                })?;
            if response.url() != &url {
                return Err(AikitError::ToolExecution(
                    "HTTP client followed an unvalidated redirect".into(),
                ));
            }
            let next = redirect_target(&url, location, &self.hosts)?;
            if url == next {
                return Err(AikitError::ToolExecution(
                    "web redirect looped to the same URL".into(),
                ));
            }
            redirects += 1;
            if redirects > MAX_REDIRECTS {
                return Err(AikitError::ToolExecution(format!(
                    "web request exceeded {MAX_REDIRECTS} redirects"
                )));
            }
            url = next;
        };
        if !response.status().is_success() {
            return Err(AikitError::ToolExecution(format!(
                "web request returned HTTP {}",
                response.status()
            )));
        }
        let kind = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();
        if !(kind.starts_with("text/") || kind.contains("json") || kind.contains("xml")) {
            return Err(AikitError::ToolExecution(format!(
                "unsupported content type '{kind}'"
            )));
        }
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| AikitError::ToolExecution(e.to_string()))?;
            if bytes.len().saturating_add(chunk.len()) > self.max_bytes {
                return Err(AikitError::ToolExecution(format!(
                    "response exceeds {} bytes",
                    self.max_bytes
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        let text = String::from_utf8_lossy(&bytes);
        Ok(if kind.contains("html") {
            html_text(&text)
        } else {
            text.into_owned()
        })
    }
}

#[async_trait]
impl ToolExecutor for WebTools {
    async fn execute(&self, name: &str, input: Value) -> Result<String> {
        match name {
            "WebFetch" => self.fetch(field(&input, "url")?).await,
            "WebSearch" => {
                let endpoint = self.search_endpoint.as_ref().ok_or_else(|| {
                    AikitError::ToolExecution("WebSearch is not configured".into())
                })?;
                let query = bounded_field(&input, "query", MAX_SEARCH_QUERY_BYTES)?;
                let encoded: String =
                    url::form_urlencoded::byte_serialize(query.as_bytes()).collect();
                self.fetch(&endpoint.replace("{query}", &encoded)).await
            }
            _ => Err(AikitError::ToolExecution(format!(
                "unknown web tool '{name}'"
            ))),
        }
    }
}

/// Browser egress posture asserted by the caller.
///
/// [`BrowserEgressPolicy::ExternallyEnforced`] is an explicit assertion that a proxy, WebDriver
/// BiDi interceptor, or equivalent pre-request boundary already enforces the exact `hosts`
/// allowlist and rejects private, local, and non-routable destination IPs. Aikit cannot verify or
/// install that boundary because the browser owns DNS resolution and network I/O.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BrowserEgressPolicy {
    /// Browser tools are disabled. This is the safe default.
    #[default]
    Deny,
    /// The caller asserts that browser egress is externally enforced before every request.
    ExternallyEnforced,
}

fn require_browser_egress(policy: BrowserEgressPolicy) -> Result<()> {
    if policy != BrowserEgressPolicy::ExternallyEnforced {
        return Err(AikitError::Configuration(
            "BrowserTools require BrowserEgressPolicy::ExternallyEnforced: the caller must assert \
             that an external proxy, BiDi interceptor, or equivalent boundary enforces the exact \
             allowed hosts and public-IP policy before every browser request"
                .into(),
        ));
    }
    Ok(())
}

/// Browser automation over an existing caller-managed W3C WebDriver session.
///
/// Construction fails closed unless the caller explicitly selects
/// [`BrowserEgressPolicy::ExternallyEnforced`]. Current-URL validation remains defense in depth;
/// it is not a pre-request SSRF or egress boundary.
pub struct BrowserTools {
    client: reqwest::Client,
    base: Url,
    hosts: BTreeSet<String>,
    max_source_chars: usize,
    egress_policy: BrowserEgressPolicy,
}

impl BrowserTools {
    pub fn new(
        webdriver_endpoint: &str,
        session_id: &str,
        hosts: impl IntoIterator<Item = impl Into<String>>,
        egress_policy: BrowserEgressPolicy,
    ) -> Result<Self> {
        require_browser_egress(egress_policy)?;
        bounded(webdriver_endpoint, "WebDriver endpoint", MAX_URL_BYTES)?;
        bounded(
            session_id,
            "WebDriver session id",
            MAX_WEBDRIVER_SESSION_ID_BYTES,
        )?;
        if session_id.is_empty()
            || matches!(session_id, "." | "..")
            || !session_id.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
            })
        {
            return Err(AikitError::ToolExecution(
                "invalid WebDriver session id".into(),
            ));
        }
        let mut endpoint =
            Url::parse(webdriver_endpoint).map_err(|e| AikitError::ToolExecution(e.to_string()))?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err(AikitError::ToolExecution(
                "WebDriver endpoint must use HTTP or HTTPS".into(),
            ));
        }
        if endpoint.scheme() == "http" {
            let loopback = endpoint.host_str().is_some_and(|host| {
                host.eq_ignore_ascii_case("localhost")
                    || host
                        .parse::<IpAddr>()
                        .is_ok_and(|address| address.is_loopback())
            });
            if !loopback {
                return Err(AikitError::ToolExecution(
                    "remote WebDriver endpoints must use HTTPS".into(),
                ));
            }
        }
        if !endpoint.path().ends_with('/') {
            endpoint.set_path(&format!("{}/", endpoint.path()));
        }
        Ok(Self {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(CONNECT_TIMEOUT)
                .timeout(REQUEST_TIMEOUT)
                .build()
                .map_err(|e| AikitError::ToolExecution(e.to_string()))?,
            base: endpoint
                .join(&format!("session/{session_id}/"))
                .map_err(|e| AikitError::ToolExecution(e.to_string()))?,
            hosts: hosts
                .into_iter()
                .map(|host| host.into().to_ascii_lowercase())
                .collect(),
            max_source_chars: 200_000,
            egress_policy,
        })
    }

    pub fn specs(&self) -> Vec<ToolSpec> {
        vec![
            ToolSpec::new(
                "BrowserNavigate",
                "Navigate to an allowlisted HTTPS URL",
                json!({"type":"object","properties":{"url":{"type":"string","maxLength":MAX_URL_BYTES}},"required":["url"],"additionalProperties":false}),
            ),
            ToolSpec::new(
                "BrowserSnapshot",
                "Read current page URL, title, and source",
                json!({"type":"object","additionalProperties":false}),
            ),
            ToolSpec::new(
                "BrowserClick",
                "Click a CSS selector",
                json!({"type":"object","properties":{"selector":{"type":"string","maxLength":MAX_BROWSER_SELECTOR_BYTES}},"required":["selector"],"additionalProperties":false}),
            ),
            ToolSpec::new(
                "BrowserType",
                "Replace a CSS-selected element's value",
                json!({"type":"object","properties":{"selector":{"type":"string","maxLength":MAX_BROWSER_SELECTOR_BYTES},"text":{"type":"string","maxLength":MAX_BROWSER_TYPE_BYTES}},"required":["selector","text"],"additionalProperties":false}),
            ),
        ]
    }

    async fn command(&self, method: reqwest::Method, path: &str, body: Value) -> Result<Value> {
        let url = self
            .base
            .join(path)
            .map_err(|e| AikitError::ToolExecution(e.to_string()))?;
        let response = self
            .client
            .request(method, url)
            .json(&body)
            .send()
            .await
            .map_err(|_| AikitError::ToolExecution("WebDriver command transport failed".into()))?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|length| length > MAX_WEBDRIVER_RESPONSE_BYTES as u64)
        {
            return Err(AikitError::ToolExecution(format!(
                "WebDriver response exceeds {MAX_WEBDRIVER_RESPONSE_BYTES} bytes"
            )));
        }
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| {
                AikitError::ToolExecution("WebDriver response body failed while streaming".into())
            })?;
            if bytes.len().saturating_add(chunk.len()) > MAX_WEBDRIVER_RESPONSE_BYTES {
                return Err(AikitError::ToolExecution(format!(
                    "WebDriver response exceeds {MAX_WEBDRIVER_RESPONSE_BYTES} bytes"
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        let payload: Value = serde_json::from_slice(&bytes).map_err(|_| {
            AikitError::ToolExecution("WebDriver returned invalid JSON (payload redacted)".into())
        })?;
        if !status.is_success() || payload.pointer("/value/error").is_some() {
            return Err(AikitError::ToolExecution(format!(
                "WebDriver command failed with HTTP {status} (payload redacted)"
            )));
        }
        Ok(payload.get("value").cloned().unwrap_or(Value::Null))
    }

    async fn element(&self, selector: &str) -> Result<String> {
        let value = self
            .command(
                reqwest::Method::POST,
                "element",
                json!({"using":"css selector","value":selector}),
            )
            .await?;
        value
            .get("element-6066-11e4-a52e-4f735466cecf")
            .or_else(|| value.get("ELEMENT"))
            .and_then(Value::as_str)
            .map(|id| bounded(id, "WebDriver element id", MAX_WEBDRIVER_ELEMENT_ID_BYTES))
            .transpose()?
            .map(str::to_owned)
            .ok_or_else(|| AikitError::ToolExecution("WebDriver returned no element id".into()))
    }

    async fn current_allowed_url(&self) -> Result<Url> {
        let value = self.command(reqwest::Method::GET, "url", json!({})).await?;
        let raw = value.as_str().ok_or_else(|| {
            AikitError::ToolExecution("WebDriver returned a non-string page URL".into())
        })?;
        allowed_url(raw, &self.hosts)
    }
}

#[async_trait]
impl ToolExecutor for BrowserTools {
    async fn execute(&self, name: &str, input: Value) -> Result<String> {
        require_browser_egress(self.egress_policy)?;
        match name {
            "BrowserNavigate" => {
                let url = allowed_url(field(&input, "url")?, &self.hosts)?;
                self.command(reqwest::Method::POST, "url", json!({"url":url.as_str()}))
                    .await?;
                // WebDriver follows HTTP redirects inside the browser. Check the committed target,
                // not only the requested URL, before returning control or page data to the model.
                Ok(self.current_allowed_url().await?.into())
            }
            "BrowserSnapshot" => {
                self.current_allowed_url().await?;
                let title = self
                    .command(reqwest::Method::GET, "title", json!({}))
                    .await?;
                let source = self
                    .command(reqwest::Method::GET, "source", json!({}))
                    .await?;
                let source: String = source
                    .as_str()
                    .unwrap_or_default()
                    .chars()
                    .take(self.max_source_chars)
                    .collect();
                // A page can navigate itself while the snapshot commands are in flight.
                let final_url = self.current_allowed_url().await?;
                Ok(json!({"url":final_url.as_str(),"title":title,"source":source}).to_string())
            }
            "BrowserClick" => {
                let selector = bounded_field(&input, "selector", MAX_BROWSER_SELECTOR_BYTES)?;
                self.current_allowed_url().await?;
                let id = self.element(selector).await?;
                self.command(
                    reqwest::Method::POST,
                    &format!("element/{id}/click"),
                    json!({}),
                )
                .await?;
                self.current_allowed_url().await?;
                Ok("clicked".into())
            }
            "BrowserType" => {
                let selector = bounded_field(&input, "selector", MAX_BROWSER_SELECTOR_BYTES)?;
                let text = bounded_field(&input, "text", MAX_BROWSER_TYPE_BYTES)?;
                self.current_allowed_url().await?;
                let id = self.element(selector).await?;
                self.command(
                    reqwest::Method::POST,
                    &format!("element/{id}/clear"),
                    json!({}),
                )
                .await?;
                self.current_allowed_url().await?;
                self.command(
                    reqwest::Method::POST,
                    &format!("element/{id}/value"),
                    // W3C WebDriver requires one bounded `text` string. Do not also build the
                    // legacy JSON Wire Protocol array containing one allocation per character.
                    json!({"text": text}),
                )
                .await?;
                self.current_allowed_url().await?;
                Ok("typed".into())
            }
            _ => Err(AikitError::ToolExecution(format!(
                "unknown browser tool '{name}'"
            ))),
        }
    }
}

fn field<'a>(input: &'a Value, key: &str) -> Result<&'a str> {
    input
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| AikitError::ToolExecution(format!("'{key}' must be a string")))
}

fn bounded_field<'a>(input: &'a Value, key: &str, max_bytes: usize) -> Result<&'a str> {
    bounded(field(input, key)?, &format!("'{key}'"), max_bytes)
}

fn html_text(html: &str) -> String {
    let scripts = Regex::new(r"(?is)<script[^>]*>.*?</script>").expect("static regex");
    let styles = Regex::new(r"(?is)<style[^>]*>.*?</style>").expect("static regex");
    let tags = Regex::new(r"(?s)<[^>]+>").expect("static regex");
    let whitespace = Regex::new(r"\s+").expect("static regex");
    let clean = scripts.replace_all(html, " ");
    let clean = styles.replace_all(&clean, " ");
    whitespace
        .replace_all(&tags.replace_all(&clean, " "), " ")
        .trim()
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn network_is_deny_by_default() {
        let hosts = BTreeSet::from(["example.com".into()]);
        assert!(allowed_url("https://example.com/a", &hosts).is_ok());
        assert!(allowed_url("http://example.com/a", &hosts).is_err());
        assert!(allowed_url("https://example.com:8443/a", &hosts).is_err());
        assert!(allowed_url("https://other.example/a", &hosts).is_err());
        assert!(allowed_url("https://127.0.0.1/a", &BTreeSet::from(["127.0.0.1".into()])).is_err());
    }

    #[test]
    fn dns_targets_fail_closed_for_non_public_ranges() {
        for address in [
            "0.1.2.3",
            "10.0.0.1",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.1.1",
            "192.0.0.1",
            "198.18.0.1",
            "224.0.0.1",
            "240.0.0.1",
            "::1",
            "::ffff:127.0.0.1",
            "::127.0.0.1",
            "64:ff9b::7f00:1",
            "2002:7f00:1::",
            "fc00::1",
            "fe80::1",
            "fec0::1",
            "ff00::1",
            "2001:db8::1",
        ] {
            assert!(denied_ip(address.parse().unwrap()), "accepted {address}");
        }
        for address in ["1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"] {
            assert!(!denied_ip(address.parse().unwrap()), "rejected {address}");
        }
    }

    #[test]
    fn search_is_only_advertised_when_configured() {
        let web = WebTools::new(["example.com"]).unwrap();
        assert_eq!(web.specs().len(), 1);
        let web = web
            .with_search_endpoint("https://example.com/search?q={query}")
            .unwrap();
        assert_eq!(web.specs().len(), 2);
    }

    #[test]
    fn redirect_targets_are_revalidated_against_the_allowlist() {
        let hosts = BTreeSet::from(["example.com".into(), "127.0.0.1".into()]);
        let current = Url::parse("https://example.com/start").unwrap();

        assert_eq!(
            redirect_target(&current, "/next", &hosts).unwrap().as_str(),
            "https://example.com/next"
        );
        assert!(redirect_target(&current, "https://not-allowed.example/", &hosts).is_err());
        assert!(redirect_target(&current, "https://127.0.0.1/admin", &hosts).is_err());
    }

    #[test]
    fn browser_tools_default_to_denied_without_an_external_egress_assertion() {
        let error = BrowserTools::new(
            "http://127.0.0.1:4444",
            "test-session",
            ["example.com"],
            BrowserEgressPolicy::default(),
        )
        .err()
        .expect("the safe default must reject BrowserTools construction");
        assert!(error
            .to_string()
            .contains("BrowserEgressPolicy::ExternallyEnforced"));
    }

    #[tokio::test]
    async fn browser_executor_rechecks_the_external_egress_assertion() {
        let mut browser = BrowserTools::new(
            "http://127.0.0.1:4444",
            "test-session",
            ["example.com"],
            BrowserEgressPolicy::ExternallyEnforced,
        )
        .unwrap();
        browser.egress_policy = BrowserEgressPolicy::Deny;

        let error = browser
            .execute("BrowserSnapshot", json!({}))
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("BrowserEgressPolicy::ExternallyEnforced"));
    }

    #[tokio::test]
    async fn web_and_browser_inputs_are_bounded_before_io() {
        let hosts = BTreeSet::from(["example.com".into()]);
        let oversized_url = format!("https://example.com/{}", "a".repeat(MAX_URL_BYTES));
        assert!(allowed_url(&oversized_url, &hosts)
            .unwrap_err()
            .to_string()
            .contains("URL exceeds"));

        let web = WebTools::new(["example.com"])
            .unwrap()
            .with_search_endpoint("https://example.com/search?q={query}")
            .unwrap();
        let query_error = web
            .execute(
                "WebSearch",
                json!({"query": "q".repeat(MAX_SEARCH_QUERY_BYTES + 1)}),
            )
            .await
            .unwrap_err();
        assert!(query_error.to_string().contains("'query' exceeds"));

        let oversized_session = "s".repeat(MAX_WEBDRIVER_SESSION_ID_BYTES + 1);
        let session_error = BrowserTools::new(
            "http://127.0.0.1:4444",
            &oversized_session,
            ["example.com"],
            BrowserEgressPolicy::ExternallyEnforced,
        )
        .err()
        .expect("oversized session ids must fail before client construction");
        assert!(session_error
            .to_string()
            .contains("WebDriver session id exceeds"));
        for traversal in [".", ".."] {
            let error = BrowserTools::new(
                "http://127.0.0.1:4444",
                traversal,
                ["example.com"],
                BrowserEgressPolicy::ExternallyEnforced,
            )
            .err()
            .expect("dot-segment session ids must not escape the WebDriver session path");
            assert!(error.to_string().contains("invalid WebDriver session id"));
        }

        let browser = BrowserTools::new(
            "http://127.0.0.1:9",
            "test-session",
            ["example.com"],
            BrowserEgressPolicy::ExternallyEnforced,
        )
        .unwrap();
        let selector_error = browser
            .execute(
                "BrowserClick",
                json!({"selector": "s".repeat(MAX_BROWSER_SELECTOR_BYTES + 1)}),
            )
            .await
            .unwrap_err();
        assert!(selector_error.to_string().contains("'selector' exceeds"));
        let type_error = browser
            .execute(
                "BrowserType",
                json!({
                    "selector": "input",
                    "text": "x".repeat(MAX_BROWSER_TYPE_BYTES + 1)
                }),
            )
            .await
            .unwrap_err();
        assert!(type_error.to_string().contains("'text' exceeds"));
    }

    #[tokio::test]
    async fn webdriver_response_is_bounded_before_json_parsing() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let secret = "oversized-response-secret";
        let body = format!(
            "{{\"value\":\"{}{}\"}}",
            "x".repeat(MAX_WEBDRIVER_RESPONSE_BYTES),
            secret
        );
        Mock::given(method("GET"))
            .and(path("/session/test-session/url"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .expect(1)
            .mount(&server)
            .await;

        let browser = BrowserTools::new(
            &server.uri(),
            "test-session",
            ["example.com"],
            BrowserEgressPolicy::ExternallyEnforced,
        )
        .unwrap();
        let error = browser
            .command(reqwest::Method::GET, "url", json!({}))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("WebDriver response exceeds"));
        assert!(!error.contains(secret));
    }

    #[tokio::test]
    async fn webdriver_failure_payload_is_redacted() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let secret = "webdriver-stacktrace-secret";
        Mock::given(method("GET"))
            .and(path("/session/test-session/title"))
            .respond_with(ResponseTemplate::new(500).set_body_json(json!({
                "value": {
                    "error": "unknown error",
                    "message": secret,
                    "stacktrace": secret
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let browser = BrowserTools::new(
            &server.uri(),
            "test-session",
            ["example.com"],
            BrowserEgressPolicy::ExternallyEnforced,
        )
        .unwrap();
        let error = browser
            .command(reqwest::Method::GET, "title", json!({}))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("payload redacted"));
        assert!(!error.contains(secret));
    }

    #[tokio::test]
    async fn browser_type_sends_only_the_bounded_w3c_text_field() {
        use wiremock::matchers::{body_json, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/session/test-session/url"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"value": "https://example.com/form"})),
            )
            .expect(3)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/session/test-session/element"))
            .and(body_json(json!({"using":"css selector","value":"input"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "value": {"element-6066-11e4-a52e-4f735466cecf": "element-1"}
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/session/test-session/element/element-1/clear"))
            .and(body_json(json!({})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"value": null})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/session/test-session/element/element-1/value"))
            .and(body_json(json!({"text": "hello"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"value": null})))
            .expect(1)
            .mount(&server)
            .await;

        let browser = BrowserTools::new(
            &server.uri(),
            "test-session",
            ["example.com"],
            BrowserEgressPolicy::ExternallyEnforced,
        )
        .unwrap();
        assert_eq!(
            browser
                .execute("BrowserType", json!({"selector": "input", "text": "hello"}),)
                .await
                .unwrap(),
            "typed"
        );
    }

    #[tokio::test]
    async fn browser_navigation_rejects_a_disallowed_committed_target() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/session/test-session/url"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"value": null})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/session/test-session/url"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"value": "https://127.0.0.1/admin"})),
            )
            .expect(1)
            .mount(&server)
            .await;

        let browser = BrowserTools::new(
            &server.uri(),
            "test-session",
            ["example.com", "127.0.0.1"],
            BrowserEgressPolicy::ExternallyEnforced,
        )
        .unwrap();
        let error = browser
            .execute(
                "BrowserNavigate",
                json!({"url": "https://example.com/start"}),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("private and local"));
    }

    #[tokio::test]
    async fn browser_navigation_returns_an_allowlisted_committed_target() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/session/test-session/url"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"value": null})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/session/test-session/url"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"value": "https://example.com/final"})),
            )
            .expect(1)
            .mount(&server)
            .await;

        let browser = BrowserTools::new(
            &server.uri(),
            "test-session",
            ["example.com"],
            BrowserEgressPolicy::ExternallyEnforced,
        )
        .unwrap();
        let final_url = browser
            .execute(
                "BrowserNavigate",
                json!({"url": "https://example.com/start"}),
            )
            .await
            .unwrap();
        assert_eq!(final_url, "https://example.com/final");
    }
}
