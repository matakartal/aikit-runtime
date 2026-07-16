//! Governed web and browser tools with deny-by-default network access.

use crate::{AikitError, Result, ToolExecutor, ToolSpec};
use async_trait::async_trait;
use futures::StreamExt;
use regex::Regex;
use reqwest::header::CONTENT_TYPE;
use serde_json::{json, Value};
use std::{collections::BTreeSet, net::IpAddr};
use url::Url;

fn allowed_url(raw: &str, hosts: &BTreeSet<String>) -> Result<Url> {
    let url = Url::parse(raw).map_err(|e| AikitError::ToolExecution(e.to_string()))?;
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        return Err(AikitError::ToolExecution(
            "URL must use HTTPS and contain no credentials".into(),
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
        let local = match ip {
            IpAddr::V4(ip) => {
                ip.is_private()
                    || ip.is_loopback()
                    || ip.is_link_local()
                    || ip.is_broadcast()
                    || ip.is_documentation()
                    || ip.is_unspecified()
            }
            IpAddr::V6(ip) => {
                ip.is_loopback()
                    || ip.is_unique_local()
                    || ip.is_unicast_link_local()
                    || ip.is_unspecified()
            }
        };
        if local {
            return Err(AikitError::ToolExecution(
                "private and local IP addresses are denied".into(),
            ));
        }
    }
    Ok(url)
}

/// Lightweight retrieval plus optional search through a caller-owned HTTPS endpoint.
pub struct WebTools {
    client: reqwest::Client,
    hosts: BTreeSet<String>,
    search_endpoint: Option<String>,
    max_bytes: usize,
}

impl WebTools {
    pub fn new(hosts: impl IntoIterator<Item = impl Into<String>>) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| AikitError::ToolExecution(e.to_string()))?,
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
            json!({"type":"object","properties":{"url":{"type":"string"}},"required":["url"],"additionalProperties":false}),
        )];
        if self.search_endpoint.is_some() {
            specs.push(ToolSpec::new(
                "WebSearch",
                "Search through the configured allowlisted endpoint",
                json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}),
            ));
        }
        specs
    }

    async fn fetch(&self, raw: &str) -> Result<String> {
        let response = self
            .client
            .get(allowed_url(raw, &self.hosts)?)
            .send()
            .await
            .map_err(|e| AikitError::ToolExecution(e.to_string()))?;
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
                let encoded: String =
                    url::form_urlencoded::byte_serialize(field(&input, "query")?.as_bytes())
                        .collect();
                self.fetch(&endpoint.replace("{query}", &encoded)).await
            }
            _ => Err(AikitError::ToolExecution(format!(
                "unknown web tool '{name}'"
            ))),
        }
    }
}

/// Browser automation over an existing caller-managed W3C WebDriver session.
pub struct BrowserTools {
    client: reqwest::Client,
    base: Url,
    hosts: BTreeSet<String>,
    max_source_chars: usize,
}

impl BrowserTools {
    pub fn new(
        webdriver_endpoint: &str,
        session_id: &str,
        hosts: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        if session_id.is_empty() || session_id.contains('/') {
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
            client: reqwest::Client::new(),
            base: endpoint
                .join(&format!("session/{session_id}/"))
                .map_err(|e| AikitError::ToolExecution(e.to_string()))?,
            hosts: hosts
                .into_iter()
                .map(|host| host.into().to_ascii_lowercase())
                .collect(),
            max_source_chars: 200_000,
        })
    }

    pub fn specs(&self) -> Vec<ToolSpec> {
        vec![
            ToolSpec::new(
                "BrowserNavigate",
                "Navigate to an allowlisted HTTPS URL",
                json!({"type":"object","properties":{"url":{"type":"string"}},"required":["url"],"additionalProperties":false}),
            ),
            ToolSpec::new(
                "BrowserSnapshot",
                "Read current page URL, title, and source",
                json!({"type":"object","additionalProperties":false}),
            ),
            ToolSpec::new(
                "BrowserClick",
                "Click a CSS selector",
                json!({"type":"object","properties":{"selector":{"type":"string"}},"required":["selector"],"additionalProperties":false}),
            ),
            ToolSpec::new(
                "BrowserType",
                "Replace a CSS-selected element's value",
                json!({"type":"object","properties":{"selector":{"type":"string"},"text":{"type":"string"}},"required":["selector","text"],"additionalProperties":false}),
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
            .map_err(|e| AikitError::ToolExecution(e.to_string()))?;
        let status = response.status();
        let payload: Value = response
            .json()
            .await
            .map_err(|e| AikitError::ToolExecution(e.to_string()))?;
        if !status.is_success() || payload.pointer("/value/error").is_some() {
            return Err(AikitError::ToolExecution(format!(
                "WebDriver command failed: {payload}"
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
            .map(str::to_owned)
            .ok_or_else(|| AikitError::ToolExecution("WebDriver returned no element id".into()))
    }
}

#[async_trait]
impl ToolExecutor for BrowserTools {
    async fn execute(&self, name: &str, input: Value) -> Result<String> {
        match name {
            "BrowserNavigate" => {
                let url = allowed_url(field(&input, "url")?, &self.hosts)?;
                self.command(reqwest::Method::POST, "url", json!({"url":url.as_str()}))
                    .await?;
                Ok(url.into())
            }
            "BrowserSnapshot" => {
                let url = self.command(reqwest::Method::GET, "url", json!({})).await?;
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
                Ok(json!({"url":url,"title":title,"source":source}).to_string())
            }
            "BrowserClick" => {
                let id = self.element(field(&input, "selector")?).await?;
                self.command(
                    reqwest::Method::POST,
                    &format!("element/{id}/click"),
                    json!({}),
                )
                .await?;
                Ok("clicked".into())
            }
            "BrowserType" => {
                let id = self.element(field(&input, "selector")?).await?;
                self.command(
                    reqwest::Method::POST,
                    &format!("element/{id}/clear"),
                    json!({}),
                )
                .await?;
                let text = field(&input, "text")?;
                self.command(
                    reqwest::Method::POST,
                    &format!("element/{id}/value"),
                    json!({
                        "text": text,
                        "value": text.chars().map(|c| c.to_string()).collect::<Vec<_>>()
                    }),
                )
                .await?;
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

    #[test]
    fn network_is_deny_by_default() {
        let hosts = BTreeSet::from(["example.com".into()]);
        assert!(allowed_url("https://example.com/a", &hosts).is_ok());
        assert!(allowed_url("http://example.com/a", &hosts).is_err());
        assert!(allowed_url("https://other.example/a", &hosts).is_err());
        assert!(allowed_url("https://127.0.0.1/a", &BTreeSet::from(["127.0.0.1".into()])).is_err());
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
}
