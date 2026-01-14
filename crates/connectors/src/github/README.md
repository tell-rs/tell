# GitHub Connector

Pulls repository metrics from the GitHub API.

## Quick Start

```toml
[connectors.github_repos]
type = "github"
entities = ["rust-lang/rust", "tokio-rs/tokio"]
schedule = "0 0 */6 * * *"  # every 6 hours
token = "ghp_xxx"           # optional but recommended
```

## Setup

### Getting a Token (Recommended)

Without a token: **60 requests/hour**
With a token: **5,000 requests/hour**

1. Go to **GitHub** → **Settings** → **Developer settings** → **Personal access tokens**
2. Click **Generate new token (classic)**
3. Select scope: `public_repo` (for public repos only) or `repo` (for private repos)
4. Copy the token (starts with `ghp_`)

No special permissions needed for public repository metrics.

## Configuration

```toml
[connectors.github_repos]
type = "github"
entities = ["owner/repo", "org/another-repo"]  # required
schedule = "0 0 */6 * * *"    # 6-field cron (optional)
token = "ghp_xxx"             # optional, increases rate limit
metrics = ["stars", "forks"]  # optional, default: all
workspace_id = 1              # optional
api_url = "https://api.github.com"  # optional, for GitHub Enterprise
timeout_secs = 30             # optional
max_retries = 3               # optional
```

## Available Metrics

### Basic Metrics (1 API call)

| Metric | Type | Description |
|--------|------|-------------|
| `stars` | u64 | Stargazer count |
| `forks` | u64 | Fork count |
| `watchers` | u64 | Watcher count |
| `open_issues` | u64 | Open issues + PRs (GitHub's combined) |
| `size` | u64 | Repository size in KB |
| `default_branch` | string | Default branch name |

### Extended Metrics (additional API calls)

| Metric | Type | API Calls | Description |
|--------|------|-----------|-------------|
| `commits` | u64 | +1 | Commits in last 30 days |
| `last_commit_at` | string | +1 | ISO timestamp of most recent commit |
| `issues_open` | u64 | +1 | Open issues (excludes PRs) |
| `issues_closed` | u64 | +1 | Closed issues |
| `prs_open` | u64 | +1 | Open pull requests |
| `prs_closed` | u64 | +1 | Closed pull requests |
| `prs_merged` | u64 | +1 | Merged pull requests |

**Note:** Extended metrics use GitHub's Search API which has stricter rate limits (30 req/min). Use a token and avoid requesting all extended metrics for many repos on tight schedules.

## Output Example

```json
{
  "timestamp_ms": 1768134100369,
  "source": "github",
  "entity": "rust-lang/rust",
  "metrics": {
    "stars": 98000,
    "forks": 12500,
    "watchers": 1500,
    "open_issues": 9800,
    "size": 450000,
    "default_branch": "master",
    "commits_30d": 450,
    "last_commit_at": "2026-01-12T10:30:00Z",
    "issues_open": 8500,
    "issues_closed": 45000,
    "prs_open": 650,
    "prs_closed": 12000,
    "prs_merged": 45000
  }
}
```

Only requested metrics appear in output.

## Rate Limits

| Auth | Limit | Notes |
|------|-------|-------|
| No token | 60/hour | IP-based |
| With token | 5,000/hour | Per-user |
| Search API | 30/min | For extended metrics |

## GitHub Enterprise

For GitHub Enterprise, set the `api_url`:

```toml
[connectors.github_enterprise]
type = "github"
api_url = "https://github.example.com/api/v3"
entities = ["org/repo"]
token = "ghp_xxx"
```

## Testing

```bash
# Manual pull (no scheduling)
collector pull github rust-lang/rust --output json

# Test with fast schedule
[connectors.test]
type = "github"
entities = ["get-convex/convex-backend"]
schedule = "*/30 * * * * *"  # every 30 seconds
```
