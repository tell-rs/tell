//! Rate limiting middleware
//!
//! Token bucket rate limiting based on client IP address.
//!
//! # Usage
//!
//! ```ignore
//! use tell_api::ratelimit::RateLimitLayer;
//!
//! let app = Router::new()
//!     .route("/api/v1/auth/login", post(login))
//!     .layer(RateLimitLayer::new(10, 60)); // 10 requests per 60 seconds
//! ```

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{Request, Response, StatusCode},
};
use futures_util::future::BoxFuture;
use tokio::sync::RwLock;
use tower::{Layer, Service};
use tracing::warn;

/// Rate limit configuration
#[derive(Clone, Debug)]
pub struct RateLimitConfig {
    /// Maximum requests per window
    pub requests_per_window: u32,
    /// Window duration in seconds
    pub window_seconds: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_window: 100,
            window_seconds: 60,
        }
    }
}

impl RateLimitConfig {
    /// Create a new rate limit config
    pub fn new(requests_per_window: u32, window_seconds: u64) -> Self {
        Self {
            requests_per_window,
            window_seconds,
        }
    }

    /// Stricter config for auth endpoints (10 req/min)
    pub fn auth() -> Self {
        Self::new(10, 60)
    }

    /// Default API config (100 req/min)
    pub fn api() -> Self {
        Self::new(100, 60)
    }
}

/// Token bucket entry for a single IP
#[derive(Clone, Debug)]
struct TokenBucket {
    tokens: u32,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(max_tokens: u32) -> Self {
        Self {
            tokens: max_tokens,
            last_refill: Instant::now(),
        }
    }

    /// Try to consume a token, returns true if allowed
    fn try_consume(&mut self, max_tokens: u32, window: Duration) -> bool {
        // Refill tokens based on time elapsed
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);

        if elapsed >= window {
            // Full refill
            self.tokens = max_tokens;
            self.last_refill = now;
        } else {
            // Partial refill based on elapsed time
            let refill_rate = max_tokens as f64 / window.as_secs_f64();
            let refill_amount = (elapsed.as_secs_f64() * refill_rate) as u32;

            if refill_amount > 0 {
                self.tokens = (self.tokens + refill_amount).min(max_tokens);
                self.last_refill = now;
            }
        }

        // Try to consume a token
        if self.tokens > 0 {
            self.tokens -= 1;
            true
        } else {
            false
        }
    }
}

/// Shared rate limiter state
#[derive(Clone)]
pub struct RateLimiter {
    buckets: Arc<RwLock<HashMap<IpAddr, TokenBucket>>>,
    config: RateLimitConfig,
}

impl RateLimiter {
    /// Create a new rate limiter
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            buckets: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Check if request is allowed for the given IP
    pub async fn check(&self, ip: IpAddr) -> bool {
        let mut buckets = self.buckets.write().await;
        let max_tokens = self.config.requests_per_window;
        let window = Duration::from_secs(self.config.window_seconds);

        let bucket = buckets
            .entry(ip)
            .or_insert_with(|| TokenBucket::new(max_tokens));

        bucket.try_consume(max_tokens, window)
    }

    /// Clean up old entries (call periodically)
    pub async fn cleanup(&self, max_age: Duration) {
        let mut buckets = self.buckets.write().await;
        let now = Instant::now();

        buckets.retain(|_, bucket| now.duration_since(bucket.last_refill) < max_age);
    }
}

/// Rate limiting layer for Tower middleware
#[derive(Clone)]
pub struct RateLimitLayer {
    limiter: RateLimiter,
}

impl RateLimitLayer {
    /// Create a new rate limit layer with the given config
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            limiter: RateLimiter::new(config),
        }
    }

    /// Create a rate limit layer for auth endpoints
    pub fn auth() -> Self {
        Self::new(RateLimitConfig::auth())
    }

    /// Create a rate limit layer for general API endpoints
    pub fn api() -> Self {
        Self::new(RateLimitConfig::api())
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService {
            inner,
            limiter: self.limiter.clone(),
        }
    }
}

/// Rate limiting service
#[derive(Clone)]
pub struct RateLimitService<S> {
    inner: S,
    limiter: RateLimiter,
}

impl<S> Service<Request<Body>> for RateLimitService<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let limiter = self.limiter.clone();
        let inner = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, inner);

        Box::pin(async move {
            // Extract client IP from ConnectInfo or X-Forwarded-For
            let ip = extract_client_ip(&req);

            // Check rate limit
            if !limiter.check(ip).await {
                warn!(ip = %ip, "Rate limit exceeded");
                return Ok(rate_limit_response());
            }

            inner.call(req).await
        })
    }
}

/// Extract client IP from request
fn extract_client_ip<B>(req: &Request<B>) -> IpAddr {
    // Try X-Forwarded-For header first (for proxies)
    if let Some(forwarded) = req.headers().get("x-forwarded-for")
        && let Ok(value) = forwarded.to_str()
        && let Some(first_ip) = value.split(',').next()
        && let Ok(ip) = first_ip.trim().parse()
    {
        return ip;
    }

    // Try X-Real-IP header
    if let Some(real_ip) = req.headers().get("x-real-ip")
        && let Ok(value) = real_ip.to_str()
        && let Ok(ip) = value.trim().parse()
    {
        return ip;
    }

    // Try ConnectInfo extension
    if let Some(connect_info) = req.extensions().get::<ConnectInfo<std::net::SocketAddr>>() {
        return connect_info.0.ip();
    }

    // Default to localhost
    IpAddr::from([127, 0, 0, 1])
}

/// Create a 429 Too Many Requests response
fn rate_limit_response() -> Response<Body> {
    let body = serde_json::json!({
        "error": {
            "code": "rate_limit_exceeded",
            "message": "Too many requests. Please try again later."
        }
    });

    Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header("Content-Type", "application/json")
        .header("Retry-After", "60")
        .body(Body::from(body.to_string()))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiter_allows_requests() {
        let limiter = RateLimiter::new(RateLimitConfig::new(10, 60));
        let ip: IpAddr = "192.168.1.1".parse().unwrap();

        // First 10 requests should be allowed
        for _ in 0..10 {
            assert!(limiter.check(ip).await);
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_blocks_excess() {
        let limiter = RateLimiter::new(RateLimitConfig::new(5, 60));
        let ip: IpAddr = "192.168.1.2".parse().unwrap();

        // First 5 requests should be allowed
        for _ in 0..5 {
            assert!(limiter.check(ip).await);
        }

        // 6th request should be blocked
        assert!(!limiter.check(ip).await);
    }

    #[tokio::test]
    async fn test_rate_limiter_different_ips() {
        let limiter = RateLimiter::new(RateLimitConfig::new(2, 60));
        let ip1: IpAddr = "192.168.1.3".parse().unwrap();
        let ip2: IpAddr = "192.168.1.4".parse().unwrap();

        // Each IP gets its own bucket
        assert!(limiter.check(ip1).await);
        assert!(limiter.check(ip1).await);
        assert!(!limiter.check(ip1).await); // ip1 exhausted

        assert!(limiter.check(ip2).await); // ip2 still has tokens
        assert!(limiter.check(ip2).await);
        assert!(!limiter.check(ip2).await); // ip2 exhausted
    }

    #[test]
    fn test_config_defaults() {
        let config = RateLimitConfig::default();
        assert_eq!(config.requests_per_window, 100);
        assert_eq!(config.window_seconds, 60);
    }

    #[test]
    fn test_config_auth() {
        let config = RateLimitConfig::auth();
        assert_eq!(config.requests_per_window, 10);
        assert_eq!(config.window_seconds, 60);
    }
}
