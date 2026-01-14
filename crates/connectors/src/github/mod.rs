//! GitHub connector for pulling repository metrics
//!
//! Fetches repository statistics (stars, forks, issues, etc.) from the GitHub API
//! and converts them into snapshots for the pipeline.

use crate::config::GitHubConnectorConfig;
use crate::error::ConnectorError;
use crate::resilience::{CircuitBreaker, ResilienceConfig, execute_with_retry, RetryError};
use crate::traits::Connector;
use cdp_protocol::{Batch, BatchBuilder, BatchType, SourceId};
use chrono::{Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};

/// GitHub connector configuration (simple version for CLI)
#[derive(Debug, Clone)]
pub struct GitHubConfig {
    /// GitHub API token (optional, increases rate limits)
    pub token: Option<String>,
    /// API base URL (default: https://api.github.com)
    pub api_url: String,
}

impl Default for GitHubConfig {
    fn default() -> Self {
        Self {
            token: None,
            api_url: "https://api.github.com".to_string(),
        }
    }
}

/// GitHub connector for fetching repository metrics
pub struct GitHub {
    token: Option<String>,
    api_url: String,
    client: reqwest::Client,
    source_id: SourceId,
    /// Which metrics to include (None = all)
    metrics_filter: Option<Vec<String>>,
    /// Workspace ID for batch routing
    workspace_id: u32,
    /// Resilience configuration
    resilience: ResilienceConfig,
    /// Circuit breaker for this connector
    circuit_breaker: Arc<CircuitBreaker>,
}

impl GitHub {
    /// Create a new GitHub connector with the given configuration
    ///
    /// # Errors
    ///
    /// Returns error if HTTP client creation fails (e.g., TLS or proxy misconfiguration)
    pub fn new(config: GitHubConfig) -> Result<Self, ConnectorError> {
        let resilience = ResilienceConfig::default();
        let client = reqwest::Client::builder()
            .user_agent("cdp-collector/0.1")
            .timeout(Duration::from_secs(resilience.timeout_secs))
            .build()
            .map_err(|e| ConnectorError::Init(format!("GitHub HTTP client: {}", e)))?;

        let circuit_breaker = Arc::new(CircuitBreaker::new("github", resilience.clone()));

        Ok(Self {
            token: config.token,
            api_url: config.api_url,
            client,
            source_id: SourceId::new("connector:github"),
            metrics_filter: None,
            workspace_id: 1,
            resilience,
            circuit_breaker,
        })
    }

    /// Create a GitHub connector from connector config (from TOML)
    ///
    /// # Errors
    ///
    /// Returns error if HTTP client creation fails
    pub fn from_config(config: &GitHubConnectorConfig) -> Result<Self, ConnectorError> {
        let resilience = config.resilience_config();
        let client = reqwest::Client::builder()
            .user_agent("cdp-collector/0.1")
            .timeout(Duration::from_secs(resilience.timeout_secs))
            .build()
            .map_err(|e| ConnectorError::Init(format!("GitHub HTTP client: {}", e)))?;

        let circuit_breaker = Arc::new(CircuitBreaker::new("github", resilience.clone()));

        Ok(Self {
            token: config.token.clone(),
            api_url: config.api_url.clone(),
            client,
            source_id: SourceId::new("connector:github"),
            metrics_filter: config.metrics.clone(),
            workspace_id: config.workspace_id,
            resilience,
            circuit_breaker,
        })
    }

    /// Create a GitHub connector with a token
    ///
    /// # Errors
    ///
    /// Returns error if HTTP client creation fails
    pub fn with_token(token: impl Into<String>) -> Result<Self, ConnectorError> {
        Self::new(GitHubConfig {
            token: Some(token.into()),
            ..Default::default()
        })
    }

    /// Build a request with optional auth
    fn build_request(&self, url: &str) -> reqwest::RequestBuilder {
        let mut request = self.client.get(url);
        if let Some(ref token) = self.token {
            request = request.bearer_auth(token);
        }
        request
    }

    /// Handle common HTTP response errors
    fn handle_error_status(
        &self,
        response: reqwest::Response,
        entity: &str,
    ) -> ConnectorError {
        match response.status() {
            reqwest::StatusCode::NOT_FOUND => {
                ConnectorError::NotFound(entity.to_string())
            }
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => {
                ConnectorError::AuthFailed("Invalid or missing token".into())
            }
            reqwest::StatusCode::TOO_MANY_REQUESTS => {
                ConnectorError::RateLimited { retry_after_secs: 60 }
            }
            _ => ConnectorError::Http(response.error_for_status().unwrap_err()),
        }
    }

    /// Fetch repository data from GitHub API (single attempt, no retry)
    async fn fetch_repo_once(&self, owner: &str, repo: &str) -> Result<GitHubRepo, ConnectorError> {
        let url = format!("{}/repos/{}/{}", self.api_url, owner, repo);
        let response = self.build_request(&url).send().await?;

        if response.status().is_success() {
            let repo: GitHubRepo = response.json().await?;
            Ok(repo)
        } else {
            Err(self.handle_error_status(response, &format!("{}/{}", owner, repo)))
        }
    }

    /// Fetch repository data with retry and circuit breaker
    async fn fetch_repo(&self, owner: &str, repo: &str) -> Result<GitHubRepo, ConnectorError> {
        let entity = format!("{}/{}", owner, repo);
        self.execute_with_resilience(&entity, || self.fetch_repo_once(owner, repo))
            .await
    }

    /// Execute an operation with retry and circuit breaker
    async fn execute_with_resilience<F, Fut, T>(
        &self,
        entity: &str,
        operation: F,
    ) -> Result<T, ConnectorError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, ConnectorError>>,
    {
        if self.circuit_breaker.is_open() {
            warn!(
                connector = "github",
                entity = %entity,
                "circuit breaker open, skipping request"
            );
            return Err(ConnectorError::ConfigError(
                "circuit breaker open".to_string(),
            ));
        }

        let result = execute_with_retry(
            &self.resilience,
            Some(self.circuit_breaker.as_ref()),
            entity,
            operation,
        )
        .await;

        match result {
            Ok(value) => Ok(value),
            Err(RetryError::CircuitOpen) => {
                Err(ConnectorError::ConfigError("circuit breaker open".to_string()))
            }
            Err(RetryError::Exhausted { attempts, last_error }) => {
                warn!(
                    connector = "github",
                    entity = %entity,
                    attempts,
                    error = %last_error,
                    "request failed after retries"
                );
                Err(ConnectorError::ConfigError(format!(
                    "failed after {} attempts: {}",
                    attempts, last_error
                )))
            }
            Err(RetryError::Permanent(e)) => Err(e),
        }
    }

    /// Fetch commit stats (last 30 days)
    async fn fetch_commits_once(
        &self,
        owner: &str,
        repo: &str,
    ) -> Result<CommitStats, ConnectorError> {
        let since = (Utc::now() - ChronoDuration::days(30)).to_rfc3339();
        let url = format!(
            "{}/repos/{}/{}/commits?since={}&per_page=100",
            self.api_url, owner, repo, since
        );

        let response = self.build_request(&url).send().await?;

        if !response.status().is_success() {
            return Err(self.handle_error_status(response, &format!("{}/{}", owner, repo)));
        }

        // Get count from response - may need to follow pagination
        let link_header = response
            .headers()
            .get("link")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let commits: Vec<CommitInfo> = response.json().await?;
        let page_count = commits.len() as u64;

        // Parse last page from Link header if present
        let total_count = if let Some(link) = link_header {
            if let Some(last_page) = parse_last_page(&link) {
                // Estimate: (last_page - 1) * 100 + current page count
                // For simplicity, just use last_page * 100 as upper bound
                last_page * 100
            } else {
                page_count
            }
        } else {
            page_count
        };

        let last_commit_at = commits.first().map(|c| c.commit.author.date.clone());

        Ok(CommitStats {
            count_30d: total_count,
            last_commit_at,
        })
    }

    /// Fetch commit stats with resilience
    async fn fetch_commits(&self, owner: &str, repo: &str) -> Result<CommitStats, ConnectorError> {
        let entity = format!("{}/{}/commits", owner, repo);
        self.execute_with_resilience(&entity, || self.fetch_commits_once(owner, repo))
            .await
    }

    /// Fetch issue/PR count using search API (more accurate)
    async fn fetch_issue_count_once(
        &self,
        owner: &str,
        repo: &str,
        state: &str,
        is_pr: bool,
    ) -> Result<u64, ConnectorError> {
        // Use search API for accurate counts
        let type_filter = if is_pr { "pr" } else { "issue" };
        let query = format!("repo:{}/{} is:{} state:{}", owner, repo, type_filter, state);
        let url = format!(
            "{}/search/issues?q={}&per_page=1",
            self.api_url,
            urlencoding::encode(&query)
        );

        let response = self.build_request(&url).send().await?;

        if !response.status().is_success() {
            return Err(self.handle_error_status(response, &format!("{}/{}", owner, repo)));
        }

        let result: SearchResult = response.json().await?;
        Ok(result.total_count)
    }

    /// Fetch issue counts with resilience
    async fn fetch_issue_counts(
        &self,
        owner: &str,
        repo: &str,
    ) -> Result<IssueCounts, ConnectorError> {
        let entity = format!("{}/{}/issues", owner, repo);

        let open = self
            .execute_with_resilience(&entity, || {
                self.fetch_issue_count_once(owner, repo, "open", false)
            })
            .await?;

        let closed = self
            .execute_with_resilience(&entity, || {
                self.fetch_issue_count_once(owner, repo, "closed", false)
            })
            .await?;

        Ok(IssueCounts { open, closed })
    }

    /// Fetch PR counts with resilience
    async fn fetch_pr_counts(&self, owner: &str, repo: &str) -> Result<PrCounts, ConnectorError> {
        let entity = format!("{}/{}/pulls", owner, repo);

        let open = self
            .execute_with_resilience(&entity, || {
                self.fetch_issue_count_once(owner, repo, "open", true)
            })
            .await?;

        let closed = self
            .execute_with_resilience(&entity, || {
                self.fetch_issue_count_once(owner, repo, "closed", true)
            })
            .await?;

        // Merged PRs are a subset of closed - use search with is:merged
        let merged = self
            .execute_with_resilience(&entity, || async {
                let query = format!("repo:{}/{} is:pr is:merged", owner, repo);
                let url = format!(
                    "{}/search/issues?q={}&per_page=1",
                    self.api_url,
                    urlencoding::encode(&query)
                );

                let response = self.build_request(&url).send().await?;

                if !response.status().is_success() {
                    return Err(self.handle_error_status(response, &format!("{}/{}", owner, repo)));
                }

                let result: SearchResult = response.json().await?;
                Ok(result.total_count)
            })
            .await?;

        Ok(PrCounts { open, closed, merged })
    }

    /// Check if a metric should be included
    fn include_metric(&self, metric: &str) -> bool {
        match &self.metrics_filter {
            None => true,
            Some(filter) => filter.iter().any(|m| m == metric),
        }
    }

    /// Check if any extended metric is needed
    fn needs_commits(&self) -> bool {
        self.include_metric("commits") || self.include_metric("last_commit_at")
    }

    fn needs_issues(&self) -> bool {
        self.include_metric("issues_open") || self.include_metric("issues_closed")
    }

    fn needs_prs(&self) -> bool {
        self.include_metric("prs_open")
            || self.include_metric("prs_closed")
            || self.include_metric("prs_merged")
    }

    /// Build snapshot payload with all requested metrics
    fn build_payload(
        &self,
        entity: &str,
        repo: &GitHubRepo,
        commits: Option<CommitStats>,
        issues: Option<IssueCounts>,
        prs: Option<PrCounts>,
    ) -> SnapshotPayload {
        let mut metrics = FilteredMetrics::default();

        // Basic metrics (from repo endpoint)
        if self.include_metric("stars") {
            metrics.stars = Some(repo.stargazers_count);
        }
        if self.include_metric("forks") {
            metrics.forks = Some(repo.forks_count);
        }
        if self.include_metric("watchers") {
            metrics.watchers = Some(repo.watchers_count);
        }
        if self.include_metric("open_issues") {
            metrics.open_issues = Some(repo.open_issues_count);
        }
        if self.include_metric("size") {
            metrics.size_kb = Some(repo.size);
        }
        if self.include_metric("default_branch") {
            metrics.default_branch = Some(repo.default_branch.clone());
        }

        // Extended: commits
        if let Some(commit_stats) = commits {
            if self.include_metric("commits") {
                metrics.commits_30d = Some(commit_stats.count_30d);
            }
            if self.include_metric("last_commit_at") {
                metrics.last_commit_at = commit_stats.last_commit_at;
            }
        }

        // Extended: issues
        if let Some(issue_counts) = issues {
            if self.include_metric("issues_open") {
                metrics.issues_open = Some(issue_counts.open);
            }
            if self.include_metric("issues_closed") {
                metrics.issues_closed = Some(issue_counts.closed);
            }
        }

        // Extended: PRs
        if let Some(pr_counts) = prs {
            if self.include_metric("prs_open") {
                metrics.prs_open = Some(pr_counts.open);
            }
            if self.include_metric("prs_closed") {
                metrics.prs_closed = Some(pr_counts.closed);
            }
            if self.include_metric("prs_merged") {
                metrics.prs_merged = Some(pr_counts.merged);
            }
        }

        SnapshotPayload {
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            source: "github".to_string(),
            entity: entity.to_string(),
            metrics,
        }
    }

    /// Get circuit breaker status
    pub fn circuit_breaker_open(&self) -> bool {
        self.circuit_breaker.is_open()
    }

    /// Get current failure count
    pub fn failure_count(&self) -> u32 {
        self.circuit_breaker.failure_count()
    }
}

impl Connector for GitHub {
    fn name(&self) -> &'static str {
        "github"
    }

    async fn pull(&self, entity: &str) -> Result<Batch, ConnectorError> {
        let (owner, repo) = parse_entity(entity)?;

        // Always fetch basic repo data
        let repo_data = self.fetch_repo(owner, repo).await?;

        // Fetch extended metrics only if configured
        let commits = if self.needs_commits() {
            match self.fetch_commits(owner, repo).await {
                Ok(stats) => Some(stats),
                Err(e) => {
                    warn!(
                        connector = "github",
                        entity = entity,
                        error = %e,
                        "failed to fetch commit stats"
                    );
                    None
                }
            }
        } else {
            None
        };

        let issues = if self.needs_issues() {
            match self.fetch_issue_counts(owner, repo).await {
                Ok(counts) => Some(counts),
                Err(e) => {
                    warn!(
                        connector = "github",
                        entity = entity,
                        error = %e,
                        "failed to fetch issue counts"
                    );
                    None
                }
            }
        } else {
            None
        };

        let prs = if self.needs_prs() {
            match self.fetch_pr_counts(owner, repo).await {
                Ok(counts) => Some(counts),
                Err(e) => {
                    warn!(
                        connector = "github",
                        entity = entity,
                        error = %e,
                        "failed to fetch PR counts"
                    );
                    None
                }
            }
        } else {
            None
        };

        // Build payload
        let payload = self.build_payload(entity, &repo_data, commits, issues, prs);
        let payload_bytes = serde_json::to_vec(&payload)?;

        // Build batch
        let mut builder = BatchBuilder::new(BatchType::Snapshot, self.source_id.clone());
        builder.set_workspace_id(self.workspace_id);
        builder.add(&payload_bytes, 1);

        debug!(
            connector = "github",
            entity = entity,
            workspace_id = self.workspace_id,
            "pulled snapshot"
        );

        Ok(builder.finish())
    }
}

/// Parse "owner/repo" entity format
pub(crate) fn parse_entity(entity: &str) -> Result<(&str, &str), ConnectorError> {
    let parts: Vec<&str> = entity.splitn(2, '/').collect();
    if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
        return Err(ConnectorError::InvalidEntity(format!(
            "Expected 'owner/repo' format, got: {}",
            entity
        )));
    }
    Ok((parts[0], parts[1]))
}

/// Parse last page number from Link header
fn parse_last_page(link: &str) -> Option<u64> {
    // Format: <url>; rel="next", <url?page=N>; rel="last"
    for part in link.split(',') {
        if part.contains("rel=\"last\"") {
            // Extract page number from URL
            if let Some(start) = part.find("page=") {
                let after_page = &part[start + 5..];
                let end = after_page.find(|c: char| !c.is_ascii_digit()).unwrap_or(after_page.len());
                return after_page[..end].parse().ok();
            }
        }
    }
    None
}

// --- API Response Types ---

/// GitHub API response for repository
#[derive(Debug, Deserialize)]
struct GitHubRepo {
    stargazers_count: u64,
    forks_count: u64,
    watchers_count: u64,
    open_issues_count: u64,
    size: u64,
    default_branch: String,
}

/// Commit info from commits endpoint
#[derive(Debug, Deserialize)]
struct CommitInfo {
    commit: CommitDetail,
}

#[derive(Debug, Deserialize)]
struct CommitDetail {
    author: CommitAuthor,
}

#[derive(Debug, Deserialize)]
struct CommitAuthor {
    date: String,
}

/// Search API response
#[derive(Debug, Deserialize)]
struct SearchResult {
    total_count: u64,
}

/// Commit statistics
struct CommitStats {
    count_30d: u64,
    last_commit_at: Option<String>,
}

/// Issue counts
struct IssueCounts {
    open: u64,
    closed: u64,
}

/// PR counts
struct PrCounts {
    open: u64,
    closed: u64,
    merged: u64,
}

// --- Snapshot Payload ---

/// Snapshot payload sent through pipeline
#[derive(Debug, Serialize)]
struct SnapshotPayload {
    timestamp_ms: u64,
    source: String,
    entity: String,
    metrics: FilteredMetrics,
}

/// GitHub metrics with optional fields for filtering
#[derive(Debug, Default, Serialize)]
struct FilteredMetrics {
    // Basic metrics (1 API call)
    #[serde(skip_serializing_if = "Option::is_none")]
    stars: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    forks: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    watchers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    open_issues: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_branch: Option<String>,

    // Extended: commits (+1 API call)
    #[serde(skip_serializing_if = "Option::is_none")]
    commits_30d: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_commit_at: Option<String>,

    // Extended: issues (+2 API calls)
    #[serde(skip_serializing_if = "Option::is_none")]
    issues_open: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    issues_closed: Option<u64>,

    // Extended: PRs (+3 API calls)
    #[serde(skip_serializing_if = "Option::is_none")]
    prs_open: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prs_closed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prs_merged: Option<u64>,
}


#[cfg(test)]
mod tests;
