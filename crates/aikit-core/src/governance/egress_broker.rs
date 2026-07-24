//! Deny-by-default outbound HTTP broker.
//!
//! Every request hop is validated before I/O, resolved independently, checked against the
//! configured address posture, and pinned into a redirect-disabled HTTP client. This closes the
//! usual DNS-rebinding window between policy evaluation and connect. The broker is an explicit
//! request API; it is not a transparent child-process proxy and does not claim to provide a
//! Firecracker or other VM boundary.

use super::{EgressDecision, EgressPolicy};
use crate::tools::web::BrowserEgressPolicy;
use futures::StreamExt;
use reqwest::{
    header::{
        HeaderMap, HeaderName, HeaderValue, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, COOKIE, HOST,
        LOCATION, PROXY_AUTHORIZATION, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
    },
    Method, StatusCode,
};
use std::{
    collections::{BTreeSet, HashSet},
    fmt, io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::sync::Semaphore;
use url::{Host, Url};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_DNS_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_MAX_CONCURRENT_DNS_LOOKUPS: usize = 8;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_REDIRECTS: usize = 5;
const DEFAULT_MAX_URL_BYTES: usize = 8 * 1024;
const DEFAULT_MAX_REQUEST_HEADER_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_MAX_RESPONSE_HEADER_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_RESPONSE_BODY_BYTES: usize = 2 * 1024 * 1024;

/// A protocol a broker instance may explicitly permit.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum EgressScheme {
    Http,
    Https,
}

impl EgressScheme {
    fn parse(value: &str) -> Result<Self, EgressBrokerError> {
        match value {
            "http" => Ok(Self::Http),
            "https" => Ok(Self::Https),
            _ => Err(EgressBrokerError::SchemeDenied),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

/// Fail-closed broker errors. Variants deliberately omit URLs, DNS answers, headers, and bodies.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum EgressBrokerError {
    #[error("egress policy must use deny as its default decision")]
    PolicyMustDenyByDefault,
    #[error("egress policy contains an invalid domain pattern")]
    InvalidDomainPattern,
    #[error("egress policy contains an invalid port")]
    InvalidPortPolicy,
    #[error("request URL is invalid")]
    InvalidUrl,
    #[error("request URL exceeds the configured {max_bytes}-byte limit")]
    UrlTooLong { max_bytes: usize },
    #[error("URL credentials are forbidden")]
    UrlCredentialsForbidden,
    #[error("URL fragments are forbidden")]
    UrlFragmentForbidden,
    #[error("request scheme is not allowlisted")]
    SchemeDenied,
    #[error("request destination has no host")]
    HostMissing,
    #[error("request domain is not allowlisted")]
    DomainDenied,
    #[error("request port is not allowlisted")]
    PortDenied,
    #[error("request method is not allowlisted")]
    MethodDenied,
    #[error("request contains a forbidden hop-by-hop or routing header")]
    ForbiddenRequestHeader,
    #[error("request headers exceed the configured {max_bytes}-byte limit")]
    RequestHeadersTooLarge { max_bytes: usize },
    #[error("request body exceeds the configured {max_bytes}-byte limit")]
    RequestBodyTooLarge { max_bytes: usize },
    #[error("DNS resolution timed out")]
    DnsTimeout,
    #[error("DNS resolution failed")]
    DnsFailed,
    #[error("DNS resolution returned no addresses")]
    DnsReturnedNoAddresses,
    #[error("DNS resolution returned a forbidden destination address")]
    DestinationAddressDenied,
    #[error("HTTP client construction failed")]
    ClientConstructionFailed,
    #[error("outbound HTTP transport failed")]
    TransportFailed,
    #[error("HTTP client returned an unexpected response URL")]
    UnexpectedResponseUrl,
    #[error("redirect response has no valid Location header")]
    RedirectLocationInvalid,
    #[error("redirect status is not supported")]
    RedirectStatusDenied,
    #[error("redirect loop detected")]
    RedirectLoop,
    #[error("request exceeded the configured {max_redirects}-redirect limit")]
    RedirectLimitExceeded { max_redirects: usize },
    #[error("cross-origin redirects may not replay a request body")]
    CrossOriginBodyRedirectDenied,
    #[error("response headers exceed the configured {max_bytes}-byte limit")]
    ResponseHeadersTooLarge { max_bytes: usize },
    #[error("response body exceeds the configured {max_bytes}-byte limit")]
    ResponseBodyTooLarge { max_bytes: usize },
    #[error("browser proxy assertion requires exact public HTTPS port 443 policy")]
    BrowserProxyPolicyIncompatible,
}

/// Resolver boundary used immediately before each connect.
///
/// Implementations may return duplicate addresses or arbitrary ports; the broker deduplicates
/// answers and replaces every returned port with the already-authorized request port.
pub trait EgressDnsResolver: Send + Sync + 'static {
    fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>>;
}

/// Operating-system DNS resolver used by production broker instances.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemDnsResolver;

impl EgressDnsResolver for SystemDnsResolver {
    fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        (host, port).to_socket_addrs().map(Iterator::collect)
    }
}

/// A single outbound request. Its `Debug` implementation never reveals its URL, headers, or body.
#[derive(Clone)]
pub struct EgressRequest {
    method: Method,
    url: Url,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl EgressRequest {
    pub fn new(method: Method, url: &str) -> Result<Self, EgressBrokerError> {
        // Apply the hard URL cap before parsing so an attacker cannot force an unbounded parser
        // allocation and rely on the broker's later, configurable limit.
        if url.len() > DEFAULT_MAX_URL_BYTES {
            return Err(EgressBrokerError::UrlTooLong {
                max_bytes: DEFAULT_MAX_URL_BYTES,
            });
        }
        let url = Url::parse(url).map_err(|_| EgressBrokerError::InvalidUrl)?;
        Ok(Self {
            method,
            url,
            headers: HeaderMap::new(),
            body: Vec::new(),
        })
    }

    pub fn with_header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.insert(name, value);
        self
    }

    pub fn with_body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }

    pub fn method(&self) -> &Method {
        &self.method
    }

    pub fn url(&self) -> &Url {
        &self.url
    }
}

impl fmt::Debug for EgressRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EgressRequest")
            .field("method", &self.method)
            .field("url", &"[REDACTED]")
            .field("headers", &"[REDACTED]")
            .field("body", &"[REDACTED]")
            .finish()
    }
}

/// Bounded broker response. Its `Debug` implementation reveals only metadata and byte counts.
#[derive(Clone)]
pub struct EgressResponse {
    status: StatusCode,
    final_url: Url,
    headers: HeaderMap,
    body: Vec<u8>,
    redirect_count: usize,
}

impl EgressResponse {
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn final_url(&self) -> &Url {
        &self.final_url
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn into_body(self) -> Vec<u8> {
        self.body
    }

    pub fn redirect_count(&self) -> usize {
        self.redirect_count
    }
}

impl fmt::Debug for EgressResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EgressResponse")
            .field("status", &self.status)
            .field("final_url", &"[REDACTED]")
            .field("header_count", &self.headers.len())
            .field("body_bytes", &self.body.len())
            .field("redirect_count", &self.redirect_count)
            .finish()
    }
}

/// Proof object consumed by a browser integration after it has routed all browser traffic through
/// this broker. The object validates policy compatibility; it does not install or discover a
/// browser proxy by itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserProxyAssertion {
    allowed_hosts: BTreeSet<String>,
}

impl BrowserProxyAssertion {
    pub fn allowed_hosts(&self) -> &BTreeSet<String> {
        &self.allowed_hosts
    }

    /// Convert the broker assertion into the existing BrowserTools construction assertion.
    /// Callers remain responsible for attaching a proxy that routes every browser request through
    /// the broker before passing this value to BrowserTools.
    pub fn browser_egress_policy(&self) -> BrowserEgressPolicy {
        BrowserEgressPolicy::ExternallyEnforced
    }
}

/// Builder whose empty allowlists make a newly-created broker deny every request.
pub struct EgressBrokerBuilder {
    policy: EgressPolicy,
    allowed_schemes: BTreeSet<EgressScheme>,
    allowed_ports: BTreeSet<u16>,
    allowed_methods: BTreeSet<String>,
    resolver: Arc<dyn EgressDnsResolver>,
    dns_timeout: Duration,
    connect_timeout: Duration,
    request_timeout: Duration,
    max_redirects: usize,
    max_url_bytes: usize,
    max_request_header_bytes: usize,
    max_request_body_bytes: usize,
    max_response_header_bytes: usize,
    max_response_body_bytes: usize,
}

impl EgressBrokerBuilder {
    pub fn allow_scheme(mut self, scheme: EgressScheme) -> Self {
        self.allowed_schemes.insert(scheme);
        self
    }

    pub fn allow_port(mut self, port: u16) -> Self {
        self.allowed_ports.insert(port);
        self
    }

    pub fn allow_method(mut self, method: Method) -> Self {
        self.allowed_methods.insert(method.as_str().to_owned());
        self
    }

    pub fn with_resolver(mut self, resolver: Arc<dyn EgressDnsResolver>) -> Self {
        self.resolver = resolver;
        self
    }

    pub fn with_dns_timeout(mut self, timeout: Duration) -> Self {
        self.dns_timeout = timeout;
        self
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn with_max_redirects(mut self, max_redirects: usize) -> Self {
        self.max_redirects = max_redirects;
        self
    }

    pub fn with_max_url_bytes(mut self, max_bytes: usize) -> Self {
        self.max_url_bytes = max_bytes;
        self
    }

    pub fn with_max_request_header_bytes(mut self, max_bytes: usize) -> Self {
        self.max_request_header_bytes = max_bytes;
        self
    }

    pub fn with_max_request_body_bytes(mut self, max_bytes: usize) -> Self {
        self.max_request_body_bytes = max_bytes;
        self
    }

    pub fn with_max_response_header_bytes(mut self, max_bytes: usize) -> Self {
        self.max_response_header_bytes = max_bytes;
        self
    }

    pub fn with_max_response_body_bytes(mut self, max_bytes: usize) -> Self {
        self.max_response_body_bytes = max_bytes;
        self
    }

    pub fn build(mut self) -> Result<EgressBroker, EgressBrokerError> {
        if self.policy.default_decision != EgressDecision::Deny {
            return Err(EgressBrokerError::PolicyMustDenyByDefault);
        }
        if self.allowed_ports.contains(&0) {
            return Err(EgressBrokerError::InvalidPortPolicy);
        }
        self.policy.allowed_domains = normalize_patterns(&self.policy.allowed_domains)?;
        self.policy.denied_domains = normalize_patterns(&self.policy.denied_domains)?;
        Ok(EgressBroker {
            policy: self.policy,
            allowed_schemes: self.allowed_schemes,
            allowed_ports: self.allowed_ports,
            allowed_methods: self.allowed_methods,
            resolver: self.resolver,
            dns_resolution_slots: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_DNS_LOOKUPS)),
            dns_timeout: self.dns_timeout,
            connect_timeout: self.connect_timeout,
            request_timeout: self.request_timeout,
            max_redirects: self.max_redirects,
            max_url_bytes: self.max_url_bytes.max(1),
            max_request_header_bytes: self.max_request_header_bytes.max(1),
            max_request_body_bytes: self.max_request_body_bytes,
            max_response_header_bytes: self.max_response_header_bytes.max(1),
            max_response_body_bytes: self.max_response_body_bytes,
        })
    }
}

/// Broker configuration and execution boundary.
#[derive(Clone)]
pub struct EgressBroker {
    policy: EgressPolicy,
    allowed_schemes: BTreeSet<EgressScheme>,
    allowed_ports: BTreeSet<u16>,
    allowed_methods: BTreeSet<String>,
    resolver: Arc<dyn EgressDnsResolver>,
    dns_resolution_slots: Arc<Semaphore>,
    dns_timeout: Duration,
    connect_timeout: Duration,
    request_timeout: Duration,
    max_redirects: usize,
    max_url_bytes: usize,
    max_request_header_bytes: usize,
    max_request_body_bytes: usize,
    max_response_header_bytes: usize,
    max_response_body_bytes: usize,
}

impl EgressBroker {
    pub fn builder(policy: EgressPolicy) -> EgressBrokerBuilder {
        EgressBrokerBuilder {
            policy,
            allowed_schemes: BTreeSet::new(),
            allowed_ports: BTreeSet::new(),
            allowed_methods: BTreeSet::new(),
            resolver: Arc::new(SystemDnsResolver),
            dns_timeout: DEFAULT_DNS_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_redirects: DEFAULT_MAX_REDIRECTS,
            max_url_bytes: DEFAULT_MAX_URL_BYTES,
            max_request_header_bytes: DEFAULT_MAX_REQUEST_HEADER_BYTES,
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            max_response_header_bytes: DEFAULT_MAX_RESPONSE_HEADER_BYTES,
            max_response_body_bytes: DEFAULT_MAX_RESPONSE_BODY_BYTES,
        }
    }

    /// Build an assertion compatible with BrowserTools' existing external-egress contract.
    ///
    /// This succeeds only for exact domains, HTTPS, port 443, and public addresses. Wildcards,
    /// cleartext HTTP, alternate ports, or local/private exceptions cannot be represented by the
    /// BrowserTools assertion and therefore fail closed.
    pub fn browser_proxy_assertion(&self) -> Result<BrowserProxyAssertion, EgressBrokerError> {
        let exact_https = self.allowed_schemes == BTreeSet::from([EgressScheme::Https]);
        let exact_port = self.allowed_ports == BTreeSet::from([443]);
        let exact_hosts = !self.policy.allowed_domains.is_empty()
            && self.policy.allowed_domains.iter().all(|host| {
                !host.starts_with("*.")
                    && self.policy.evaluate_destination(host) == EgressDecision::Allow
            });
        if !exact_https
            || !exact_port
            || !exact_hosts
            || self.policy.allow_loopback
            || self.policy.allow_private_networks
        {
            return Err(EgressBrokerError::BrowserProxyPolicyIncompatible);
        }
        Ok(BrowserProxyAssertion {
            allowed_hosts: self.policy.allowed_domains.clone(),
        })
    }

    pub async fn execute(
        &self,
        mut request: EgressRequest,
    ) -> Result<EgressResponse, EgressBrokerError> {
        self.validate_request(&request)?;
        let mut current = self.validate_url(request.url.clone())?;
        let mut visited = HashSet::from([current.url.as_str().to_owned()]);
        let mut redirect_count = 0usize;

        loop {
            // Redirect rewriting can change POST into GET. Recheck the method and request bounds
            // before every hop so a method that was never allowlisted cannot be introduced by the
            // broker itself.
            self.validate_request(&request)?;
            let client = self.pinned_client(&current).await?;
            let mut outbound = client.request(request.method.clone(), current.url.clone());
            outbound = outbound.headers(request.headers.clone());
            if !request.body.is_empty() {
                outbound = outbound.body(request.body.clone());
            }
            let response = outbound
                .send()
                .await
                .map_err(|_| EgressBrokerError::TransportFailed)?;
            if response.url() != &current.url {
                return Err(EgressBrokerError::UnexpectedResponseUrl);
            }
            self.validate_response_headers(response.headers())?;

            if response.status().is_redirection() {
                if !matches!(
                    response.status(),
                    StatusCode::MOVED_PERMANENTLY
                        | StatusCode::FOUND
                        | StatusCode::SEE_OTHER
                        | StatusCode::TEMPORARY_REDIRECT
                        | StatusCode::PERMANENT_REDIRECT
                ) {
                    return Err(EgressBrokerError::RedirectStatusDenied);
                }
                if redirect_count >= self.max_redirects {
                    return Err(EgressBrokerError::RedirectLimitExceeded {
                        max_redirects: self.max_redirects,
                    });
                }
                let location = response
                    .headers()
                    .get(LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .ok_or(EgressBrokerError::RedirectLocationInvalid)?;
                if location.len() > self.max_url_bytes {
                    return Err(EgressBrokerError::UrlTooLong {
                        max_bytes: self.max_url_bytes,
                    });
                }
                let next_url = current
                    .url
                    .join(location)
                    .map_err(|_| EgressBrokerError::RedirectLocationInvalid)?;
                let next = self.validate_url(next_url)?;
                let cross_origin = current.origin() != next.origin();
                rewrite_redirect_request(response.status(), cross_origin, &mut request)?;
                if !visited.insert(next.url.as_str().to_owned()) {
                    return Err(EgressBrokerError::RedirectLoop);
                }
                redirect_count += 1;
                current = next;
                // The next loop iteration performs fresh DNS resolution and creates a new pinned
                // client even when the redirect stays on the same hostname.
                continue;
            }

            let status = response.status();
            let headers = response.headers().clone();
            if let Some(length) = response.content_length() {
                if length > self.max_response_body_bytes as u64 {
                    return Err(EgressBrokerError::ResponseBodyTooLarge {
                        max_bytes: self.max_response_body_bytes,
                    });
                }
            }
            let mut body = Vec::new();
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| EgressBrokerError::TransportFailed)?;
                if body.len().saturating_add(chunk.len()) > self.max_response_body_bytes {
                    return Err(EgressBrokerError::ResponseBodyTooLarge {
                        max_bytes: self.max_response_body_bytes,
                    });
                }
                body.extend_from_slice(&chunk);
            }
            return Ok(EgressResponse {
                status,
                final_url: current.url,
                headers,
                body,
                redirect_count,
            });
        }
    }

    fn validate_request(&self, request: &EgressRequest) -> Result<(), EgressBrokerError> {
        if !self.allowed_methods.contains(request.method.as_str())
            || matches!(request.method, Method::CONNECT | Method::TRACE)
        {
            return Err(EgressBrokerError::MethodDenied);
        }
        if request.body.len() > self.max_request_body_bytes {
            return Err(EgressBrokerError::RequestBodyTooLarge {
                max_bytes: self.max_request_body_bytes,
            });
        }
        let mut header_bytes = 0usize;
        for (name, value) in &request.headers {
            if forbidden_request_header(name) {
                return Err(EgressBrokerError::ForbiddenRequestHeader);
            }
            header_bytes = header_bytes
                .saturating_add(name.as_str().len())
                .saturating_add(value.as_bytes().len());
            if header_bytes > self.max_request_header_bytes {
                return Err(EgressBrokerError::RequestHeadersTooLarge {
                    max_bytes: self.max_request_header_bytes,
                });
            }
        }
        Ok(())
    }

    fn validate_url(&self, mut url: Url) -> Result<ValidatedDestination, EgressBrokerError> {
        if url.as_str().len() > self.max_url_bytes {
            return Err(EgressBrokerError::UrlTooLong {
                max_bytes: self.max_url_bytes,
            });
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(EgressBrokerError::UrlCredentialsForbidden);
        }
        if url.fragment().is_some() {
            return Err(EgressBrokerError::UrlFragmentForbidden);
        }
        let scheme = EgressScheme::parse(url.scheme())?;
        if !self.allowed_schemes.contains(&scheme) {
            return Err(EgressBrokerError::SchemeDenied);
        }
        let domain_host = matches!(url.host(), Some(Host::Domain(_)));
        let host = canonical_url_host(&url)?;
        if domain_host {
            url.set_host(Some(&host))
                .map_err(|_| EgressBrokerError::InvalidUrl)?;
        }
        // `Url::set_host` reparses unbracketed IPv6 at the first colon (for example, the first
        // hextet `2606` becomes legacy numeric IPv4 `0.0.10.46`). Preserve typed IP literals and
        // fail closed if any future normalization changes the authority that policy and DNS pin.
        if canonical_url_host(&url)? != host {
            return Err(EgressBrokerError::InvalidUrl);
        }
        if self.policy.evaluate_destination(&host) != EgressDecision::Allow {
            return Err(EgressBrokerError::DomainDenied);
        }
        let port = url
            .port_or_known_default()
            .ok_or(EgressBrokerError::PortDenied)?;
        if !self.allowed_ports.contains(&port) {
            return Err(EgressBrokerError::PortDenied);
        }
        Ok(ValidatedDestination {
            url,
            host,
            port,
            scheme,
        })
    }

    async fn pinned_client(
        &self,
        destination: &ValidatedDestination,
    ) -> Result<reqwest::Client, EgressBrokerError> {
        let resolver = self.resolver.clone();
        let host = destination.host.clone();
        let port = destination.port;
        let deadline = tokio::time::Instant::now() + self.dns_timeout;
        let permit =
            tokio::time::timeout_at(deadline, self.dns_resolution_slots.clone().acquire_owned())
                .await
                .map_err(|_| EgressBrokerError::DnsTimeout)?
                .map_err(|_| EgressBrokerError::DnsFailed)?;
        let lookup = tokio::task::spawn_blocking(move || {
            // Keep the permit inside the blocking job. Dropping the JoinHandle on timeout cannot
            // release this slot early and allow abandoned resolver calls to accumulate.
            let _permit = permit;
            resolver.resolve(&host, port)
        });
        let addresses = tokio::time::timeout_at(deadline, lookup)
            .await
            .map_err(|_| EgressBrokerError::DnsTimeout)?
            .map_err(|_| EgressBrokerError::DnsFailed)?
            .map_err(|_| EgressBrokerError::DnsFailed)?;
        if addresses.is_empty() {
            return Err(EgressBrokerError::DnsReturnedNoAddresses);
        }
        let mut unique_ips = BTreeSet::new();
        for address in addresses {
            if !destination_ip_allowed(&self.policy, address.ip()) {
                return Err(EgressBrokerError::DestinationAddressDenied);
            }
            unique_ips.insert(address.ip());
        }
        if unique_ips.is_empty() {
            return Err(EgressBrokerError::DnsReturnedNoAddresses);
        }
        let pinned = unique_ips
            .into_iter()
            .map(|ip| SocketAddr::new(ip, port))
            .collect::<Vec<_>>();
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .referer(false)
            .pool_max_idle_per_host(0)
            .connect_timeout(self.connect_timeout)
            .timeout(self.request_timeout)
            .resolve_to_addrs(&destination.host, &pinned)
            .build()
            .map_err(|_| EgressBrokerError::ClientConstructionFailed)
    }

    fn validate_response_headers(&self, headers: &HeaderMap) -> Result<(), EgressBrokerError> {
        let bytes = headers.iter().fold(0usize, |total, (name, value)| {
            total
                .saturating_add(name.as_str().len())
                .saturating_add(value.as_bytes().len())
        });
        if bytes > self.max_response_header_bytes {
            return Err(EgressBrokerError::ResponseHeadersTooLarge {
                max_bytes: self.max_response_header_bytes,
            });
        }
        Ok(())
    }
}

struct ValidatedDestination {
    url: Url,
    host: String,
    port: u16,
    #[allow(dead_code)]
    scheme: EgressScheme,
}

impl ValidatedDestination {
    fn origin(&self) -> (&str, &str, u16) {
        (self.scheme.as_str(), &self.host, self.port)
    }
}

fn normalize_patterns(patterns: &BTreeSet<String>) -> Result<BTreeSet<String>, EgressBrokerError> {
    patterns
        .iter()
        .map(|pattern| normalize_domain_pattern(pattern))
        .collect()
}

fn normalize_domain_pattern(pattern: &str) -> Result<String, EgressBrokerError> {
    let normalized = pattern.trim().trim_end_matches('.').to_ascii_lowercase();
    let domain = normalized.strip_prefix("*.").unwrap_or(&normalized);
    if normalized == "*"
        || domain.is_empty()
        || domain.len() > 253
        || domain.starts_with('.')
        || domain.parse::<IpAddr>().is_err()
            && !domain.split('.').all(|label| {
                !label.is_empty()
                    && label.len() <= 63
                    && !label.starts_with('-')
                    && !label.ends_with('-')
                    && label
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            })
    {
        return Err(EgressBrokerError::InvalidDomainPattern);
    }
    Ok(normalized)
}

fn canonical_url_host(url: &Url) -> Result<String, EgressBrokerError> {
    match url.host().ok_or(EgressBrokerError::HostMissing)? {
        Host::Domain(domain) => Ok(domain.trim_end_matches('.').to_ascii_lowercase()),
        Host::Ipv4(address) => Ok(address.to_string()),
        Host::Ipv6(address) => Ok(address.to_string()),
    }
}

fn forbidden_request_header(name: &HeaderName) -> bool {
    matches!(
        name,
        &HOST
            | &CONNECTION
            | &CONTENT_LENGTH
            | &PROXY_AUTHORIZATION
            | &TE
            | &TRAILER
            | &TRANSFER_ENCODING
            | &UPGRADE
    ) || name.as_str().eq_ignore_ascii_case("proxy-connection")
}

fn rewrite_redirect_request(
    status: StatusCode,
    cross_origin: bool,
    request: &mut EgressRequest,
) -> Result<(), EgressBrokerError> {
    let converts_to_get = matches!(
        status,
        StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND | StatusCode::SEE_OTHER
    ) && request.method != Method::GET
        && request.method != Method::HEAD;
    if converts_to_get {
        request.method = Method::GET;
        request.body.clear();
        request.headers.remove(CONTENT_TYPE);
        request.headers.remove(CONTENT_LENGTH);
    }
    if cross_origin {
        if !request.body.is_empty() {
            return Err(EgressBrokerError::CrossOriginBodyRedirectDenied);
        }
        // Do not guess which caller headers contain credentials. Drop all of them at an origin
        // boundary; the caller may issue a new explicitly-authorized request if needed.
        request.headers.clear();
    } else {
        request.headers.remove(PROXY_AUTHORIZATION);
    }
    request.headers.remove(COOKIE);
    Ok(())
}

fn destination_ip_allowed(policy: &EgressPolicy, address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => destination_ipv4_allowed(policy, address),
        IpAddr::V6(address) => destination_ipv6_allowed(policy, address),
    }
}

fn destination_ipv4_allowed(policy: &EgressPolicy, address: Ipv4Addr) -> bool {
    let [first, second, _, _] = address.octets();
    if address.is_loopback() {
        return policy.allow_loopback;
    }
    if address.is_private() {
        return policy.allow_private_networks;
    }
    !(address.is_link_local()
        || address.is_broadcast()
        || address.is_documentation()
        || address.is_unspecified()
        || address.is_multicast()
        || first == 0
        || (first == 100 && (64..=127).contains(&second))
        || (first == 192 && second == 0)
        || (first == 192 && second == 88)
        || (first == 198 && (18..=19).contains(&second))
        || first >= 240)
}

/// Whether a native IPv6 destination belongs to the currently delegated public-unicast space and
/// is globally reachable under the IANA special-purpose registry. IPv4 embedding/translation and
/// caller-configured loopback/private exceptions are handled before this shared final gate.
pub(crate) fn ipv6_destination_is_public(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    let first = segments[0];

    // IANA currently delegates global unicast from 2000::/3. Everything else is reserved,
    // local, multicast, translation, or otherwise non-public and therefore fails closed.
    if first & 0xe000 != 0x2000 {
        return false;
    }

    if first == 0x2001 && segments[1] <= 0x01ff {
        // 2001::/23 is non-global unless a more-specific IANA allocation says otherwise.
        let protocol_anycast = segments[1] == 0x0001
            && segments[2..7].iter().all(|segment| *segment == 0)
            && matches!(segments[7], 0x0001 | 0x0002 | 0x0003);
        let amt = segments[1] == 0x0003;
        let as112 = segments[1] == 0x0004 && segments[2] == 0x0112;
        let orchid_v2 = segments[1] & 0xfff0 == 0x0020;
        let drone_remote_id = segments[1] & 0xfff0 == 0x0030;
        return protocol_anycast || amt || as112 || orchid_v2 || drone_remote_id;
    }

    !(first == 0x2001 && segments[1] == 0x0db8
        || first == 0x2002
        || first == 0x3ffe
        || first == 0x3fff && segments[1] & 0xf000 == 0)
}

fn destination_ipv6_allowed(policy: &EgressPolicy, address: Ipv6Addr) -> bool {
    if let Some(mapped) = address.to_ipv4_mapped() {
        return destination_ipv4_allowed(policy, mapped);
    }
    if address.is_loopback() {
        return policy.allow_loopback;
    }
    let segments = address.segments();
    if segments[..6].iter().all(|segment| *segment == 0) {
        let embedded = Ipv4Addr::new(
            (segments[6] >> 8) as u8,
            segments[6] as u8,
            (segments[7] >> 8) as u8,
            segments[7] as u8,
        );
        return destination_ipv4_allowed(policy, embedded);
    }
    if address.is_unique_local() {
        return policy.allow_private_networks;
    }
    ipv6_destination_is_public(address)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::VecDeque,
        io::{Read, Write},
        net::TcpListener,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
        thread,
    };

    #[derive(Default)]
    struct SequenceResolver {
        answers: Mutex<VecDeque<Vec<SocketAddr>>>,
    }

    impl SequenceResolver {
        fn new(answers: impl IntoIterator<Item = Vec<SocketAddr>>) -> Self {
            Self {
                answers: Mutex::new(answers.into_iter().collect()),
            }
        }
    }

    impl EgressDnsResolver for SequenceResolver {
        fn resolve(&self, _host: &str, _port: u16) -> io::Result<Vec<SocketAddr>> {
            self.answers
                .lock()
                .map_err(|_| io::Error::other("resolver state unavailable"))?
                .pop_front()
                .ok_or_else(|| io::Error::other("no scripted DNS answer"))
        }
    }

    struct SlowCountingResolver {
        active: AtomicUsize,
        peak: AtomicUsize,
    }

    impl SlowCountingResolver {
        fn new() -> Self {
            Self {
                active: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            }
        }
    }

    impl EgressDnsResolver for SlowCountingResolver {
        fn resolve(&self, _host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(100));
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)])
        }
    }

    fn policy(hosts: impl IntoIterator<Item = &'static str>) -> EgressPolicy {
        EgressPolicy {
            allowed_domains: hosts.into_iter().map(str::to_owned).collect(),
            ..EgressPolicy::deny_all()
        }
    }

    fn local_policy(hosts: impl IntoIterator<Item = &'static str>) -> EgressPolicy {
        EgressPolicy {
            allow_loopback: true,
            allowed_domains: hosts.into_iter().map(str::to_owned).collect(),
            ..EgressPolicy::deny_all()
        }
    }

    fn spawn_server(responses: Vec<String>) -> (SocketAddr, thread::JoinHandle<()>) {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = [0u8; 4096];
                let _ = stream.read(&mut request);
                stream.write_all(response.as_bytes()).unwrap();
            }
        });
        (address, handle)
    }

    fn local_broker(address: SocketAddr, resolver: Arc<dyn EgressDnsResolver>) -> EgressBroker {
        EgressBroker::builder(local_policy(["broker.test"]))
            .allow_scheme(EgressScheme::Http)
            .allow_port(address.port())
            .allow_method(Method::GET)
            .allow_method(Method::POST)
            .with_resolver(resolver)
            .build()
            .unwrap()
    }

    #[test]
    fn builder_and_empty_allowlists_fail_closed() {
        let allow_default = EgressPolicy {
            default_decision: EgressDecision::Allow,
            ..EgressPolicy::deny_all()
        };
        assert_eq!(
            EgressBroker::builder(allow_default).build().err(),
            Some(EgressBrokerError::PolicyMustDenyByDefault)
        );

        let broker = EgressBroker::builder(policy(["example.com"]))
            .build()
            .unwrap();
        let request = EgressRequest::new(Method::GET, "https://example.com/").unwrap();
        assert_eq!(
            broker.validate_request(&request),
            Err(EgressBrokerError::MethodDenied)
        );
        assert_eq!(
            broker.validate_url(request.url.clone()).err(),
            Some(EgressBrokerError::SchemeDenied)
        );
    }

    #[test]
    fn scheme_domain_port_credentials_and_fragments_are_separate_gates() {
        let broker = EgressBroker::builder(policy(["example.com"]))
            .allow_scheme(EgressScheme::Https)
            .allow_port(443)
            .allow_method(Method::GET)
            .build()
            .unwrap();
        for (raw, expected) in [
            ("http://example.com/", EgressBrokerError::SchemeDenied),
            ("https://blocked.test/", EgressBrokerError::DomainDenied),
            ("https://example.com:444/", EgressBrokerError::PortDenied),
            (
                "https://user:secret@example.com/",
                EgressBrokerError::UrlCredentialsForbidden,
            ),
            (
                "https://example.com/#token",
                EgressBrokerError::UrlFragmentForbidden,
            ),
        ] {
            let request = EgressRequest::new(Method::GET, raw).unwrap();
            assert_eq!(broker.validate_url(request.url).err(), Some(expected));
        }
    }

    #[tokio::test]
    async fn public_ipv6_literal_keeps_its_authority_through_validation_and_pinning() {
        let address: Ipv6Addr = "2606:4700:4700::1111".parse().unwrap();
        let resolver = Arc::new(SequenceResolver::new([vec![SocketAddr::new(
            IpAddr::V6(address),
            443,
        )]]));
        let broker = EgressBroker::builder(policy(["2606:4700:4700::1111"]))
            .allow_scheme(EgressScheme::Https)
            .allow_port(443)
            .allow_method(Method::GET)
            .with_resolver(resolver)
            .build()
            .unwrap();
        let request =
            EgressRequest::new(Method::GET, "https://[2606:4700:4700::1111]/dns-query").unwrap();

        let destination = broker.validate_url(request.url).unwrap();
        assert_eq!(destination.host, address.to_string());
        assert_eq!(destination.url.host(), Some(Host::Ipv6(address)));
        assert_eq!(
            destination.url.as_str(),
            "https://[2606:4700:4700::1111]/dns-query"
        );
        assert_ne!(destination.url.host_str(), Some("0.0.10.46"));

        broker
            .pinned_client(&destination)
            .await
            .expect("the validated IPv6 destination must remain usable by the pinned client");
    }

    #[test]
    fn public_address_policy_rejects_local_reserved_and_mixed_answers() {
        let public_only = policy(["example.com"]);
        for denied in [
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
            "64:ff9b::7f00:1",
            "::ffff:0:127.0.0.1",
            "100::1",
            "100:0:0:1::1",
            "2001:1::4",
            "2001:2::1",
            "2001:4:111::1",
            "2001:4:113::1",
            "2001:10::1",
            "2001:40::1",
            "2002:7f00:1::",
            "3ffe::1",
            "4000::1",
            "6000::1",
            "fc00::1",
            "fe80::1",
            "fec0::1",
            "ff00::1",
            "2001:db8::1",
            "3fff::1",
            "5f00::1",
        ] {
            assert!(
                !destination_ip_allowed(&public_only, denied.parse().unwrap()),
                "accepted {denied}"
            );
        }
        for allowed in [
            "1.1.1.1",
            "8.8.8.8",
            "2001:1::1",
            "2001:1::2",
            "2001:1::3",
            "2001:3::1",
            "2001:4:112::1",
            "2001:20::1",
            "2001:30::1",
            "2620:4f:8000::1",
            "2606:4700:4700::1111",
        ] {
            assert!(
                destination_ip_allowed(&public_only, allowed.parse().unwrap()),
                "rejected {allowed}"
            );
        }
    }

    #[tokio::test]
    async fn domain_resolution_to_non_global_ipv6_is_denied_before_connect() {
        let reserved = SocketAddr::new("100:0:0:1::1".parse().unwrap(), 443);
        let resolver = Arc::new(SequenceResolver::new([vec![reserved]]));
        let broker = EgressBroker::builder(policy(["broker.test"]))
            .allow_scheme(EgressScheme::Https)
            .allow_port(443)
            .allow_method(Method::GET)
            .with_resolver(resolver)
            .build()
            .unwrap();

        let error = broker
            .execute(EgressRequest::new(Method::GET, "https://broker.test/").unwrap())
            .await
            .unwrap_err();

        assert_eq!(error, EgressBrokerError::DestinationAddressDenied);
    }

    #[test]
    fn debug_output_redacts_url_headers_and_body() {
        let secret = "do-not-log-this-secret";
        let request = EgressRequest::new(
            Method::POST,
            &format!("https://example.com/private?token={secret}"),
        )
        .unwrap()
        .with_header(
            reqwest::header::AUTHORIZATION,
            HeaderValue::from_str(secret).unwrap(),
        )
        .with_body(secret.as_bytes().to_vec());
        let debug = format!("{request:?}");
        assert!(!debug.contains(secret));
        assert!(!debug.contains("example.com"));
        assert!(debug.matches("[REDACTED]").count() >= 3);
    }

    #[test]
    fn redirect_method_rewrite_cannot_introduce_an_unallowlisted_method() {
        let broker = EgressBroker::builder(policy(["example.com"]))
            .allow_scheme(EgressScheme::Https)
            .allow_port(443)
            .allow_method(Method::POST)
            .build()
            .unwrap();
        let mut request = EgressRequest::new(Method::POST, "https://example.com/")
            .unwrap()
            .with_body(b"sensitive".to_vec());
        rewrite_redirect_request(StatusCode::FOUND, false, &mut request).unwrap();
        assert_eq!(request.method(), Method::GET);
        assert_eq!(
            broker.validate_request(&request),
            Err(EgressBrokerError::MethodDenied)
        );
    }

    #[tokio::test]
    async fn local_socket_request_is_pinned_and_response_is_bounded() {
        let response_body = "bounded local response";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
            response_body.len()
        );
        let (address, server) = spawn_server(vec![response]);
        let resolver = Arc::new(SequenceResolver::new([vec![address]]));
        let broker = local_broker(address, resolver);
        let result = broker
            .execute(
                EgressRequest::new(
                    Method::GET,
                    &format!("http://broker.test:{}/resource", address.port()),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        server.join().unwrap();
        assert_eq!(result.status(), StatusCode::OK);
        assert_eq!(result.body(), response_body.as_bytes());
        assert_eq!(result.redirect_count(), 0);
    }

    #[tokio::test]
    async fn oversized_response_and_transport_errors_do_not_expose_payloads() {
        let secret = "oversized-response-secret";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{secret}",
            secret.len()
        );
        let (address, server) = spawn_server(vec![response]);
        let resolver = Arc::new(SequenceResolver::new([vec![address]]));
        let broker = EgressBroker::builder(local_policy(["broker.test"]))
            .allow_scheme(EgressScheme::Http)
            .allow_port(address.port())
            .allow_method(Method::GET)
            .with_resolver(resolver)
            .with_max_response_body_bytes(4)
            .build()
            .unwrap();
        let error = broker
            .execute(
                EgressRequest::new(
                    Method::GET,
                    &format!("http://broker.test:{}/", address.port()),
                )
                .unwrap(),
            )
            .await
            .unwrap_err();
        server.join().unwrap();
        assert_eq!(
            error,
            EgressBrokerError::ResponseBodyTooLarge { max_bytes: 4 }
        );
        assert!(!format!("{error:?}").contains(secret));
    }

    #[tokio::test]
    async fn same_host_redirect_is_reresolved_and_rebinding_fails_before_connect() {
        let response = concat!(
            "HTTP/1.1 302 Found\r\n",
            "Location: /second\r\n",
            "Content-Length: 0\r\n",
            "Connection: close\r\n\r\n"
        )
        .to_owned();
        let (address, server) = spawn_server(vec![response]);
        let rebound = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 8)), address.port());
        let resolver = Arc::new(SequenceResolver::new([vec![address], vec![rebound]]));
        let broker = local_broker(address, resolver);
        let error = broker
            .execute(
                EgressRequest::new(
                    Method::GET,
                    &format!("http://broker.test:{}/first", address.port()),
                )
                .unwrap(),
            )
            .await
            .unwrap_err();
        server.join().unwrap();
        assert_eq!(error, EgressBrokerError::DestinationAddressDenied);
    }

    #[tokio::test]
    async fn redirect_target_is_revalidated_before_its_dns_lookup() {
        let response = concat!(
            "HTTP/1.1 302 Found\r\n",
            "Location: http://blocked.test/secret\r\n",
            "Content-Length: 0\r\n",
            "Connection: close\r\n\r\n"
        )
        .to_owned();
        let (address, server) = spawn_server(vec![response]);
        let resolver = Arc::new(SequenceResolver::new([vec![address]]));
        let broker = local_broker(address, resolver);
        let error = broker
            .execute(
                EgressRequest::new(
                    Method::GET,
                    &format!("http://broker.test:{}/first", address.port()),
                )
                .unwrap(),
            )
            .await
            .unwrap_err();
        server.join().unwrap();
        assert_eq!(error, EgressBrokerError::DomainDenied);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timed_out_dns_jobs_remain_concurrency_bounded() {
        let resolver = Arc::new(SlowCountingResolver::new());
        let broker = EgressBroker::builder(local_policy(["broker.test"]))
            .allow_scheme(EgressScheme::Http)
            .allow_port(80)
            .allow_method(Method::GET)
            .with_resolver(resolver.clone())
            .with_dns_timeout(Duration::from_millis(20))
            .build()
            .unwrap();
        let requests = (0..40).map(|_| {
            let broker = broker.clone();
            async move {
                broker
                    .execute(EgressRequest::new(Method::GET, "http://broker.test/").unwrap())
                    .await
            }
        });
        let results = futures::future::join_all(requests).await;

        assert!(results
            .iter()
            .all(|result| result.as_ref().err() == Some(&EgressBrokerError::DnsTimeout)));
        assert!(
            resolver.peak.load(Ordering::SeqCst) <= DEFAULT_MAX_CONCURRENT_DNS_LOOKUPS,
            "timed-out blocking DNS jobs exceeded the concurrency cap"
        );
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(resolver.active.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn browser_assertion_only_represents_exact_public_https_policy() {
        let broker = EgressBroker::builder(policy(["example.com", "api.example.com"]))
            .allow_scheme(EgressScheme::Https)
            .allow_port(443)
            .allow_method(Method::GET)
            .build()
            .unwrap();
        let assertion = broker.browser_proxy_assertion().unwrap();
        assert_eq!(
            assertion.browser_egress_policy(),
            BrowserEgressPolicy::ExternallyEnforced
        );
        assert_eq!(
            assertion.allowed_hosts(),
            &BTreeSet::from(["api.example.com".into(), "example.com".into()])
        );

        let wildcard = EgressBroker::builder(policy(["*.example.com"]))
            .allow_scheme(EgressScheme::Https)
            .allow_port(443)
            .build()
            .unwrap();
        assert_eq!(
            wildcard.browser_proxy_assertion().err(),
            Some(EgressBrokerError::BrowserProxyPolicyIncompatible)
        );
    }
}
