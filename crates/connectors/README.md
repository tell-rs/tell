# Connectors

Pull-based data connectors for external sources. Connectors fetch snapshots from third-party APIs on a schedule and feed them into the pipeline.

## Available Connectors

| Connector | Description | Docs |
|-----------|-------------|------|
| **GitHub** | Repository metrics (stars, forks, issues, PRs) | [GITHUB.md](GITHUB.md) |
| **Shopify** | Store KPIs (orders, revenue, customers, inventory) | [SHOPIFY.md](SHOPIFY.md) |

## When to Use Connectors

Use connectors for data that:
- Lives in external systems (GitHub, Shopify, Stripe, etc.)
- Changes infrequently (stars, subscriber counts, daily revenue)
- You want to track over time as snapshots

## Installation

```bash
# All connectors (default)
cargo install --path crates/collector

# No connectors
cargo install --path crates/collector --no-default-features

# Specific connector(s)
cargo install --path crates/collector --no-default-features --features connector-github
cargo install --path crates/collector --no-default-features --features connector-shopify
cargo install --path crates/collector --no-default-features --features connector-github,connector-shopify
```

## Cron Schedule Format

Connectors use 6-field cron expressions (includes seconds):

```
┌─────────── second (0-59)
│ ┌───────── minute (0-59)
│ │ ┌─────── hour (0-23)
│ │ │ ┌───── day of month (1-31)
│ │ │ │ ┌─── month (1-12)
│ │ │ │ │ ┌─ day of week (0-6, Sun=0)
* * * * * *
```

Examples:
- `0 0 */6 * * *` - Every 6 hours
- `0 0 0 * * *` - Daily at midnight
- `0 */5 * * * *` - Every 5 minutes
- `*/30 * * * * *` - Every 30 seconds (testing only)

## Resilience

All connectors include:

- **Retry with backoff**: Transient failures (5xx, timeouts) retry with exponential backoff
- **Circuit breaker**: After 5 failures, requests blocked for 10 minutes
- **Task isolation**: Each connector runs independently, slow ones don't block others
- **Timeouts**: Per-request and overall timeout protection

## Building a New Connector

See existing connectors for patterns:
- `src/github.rs` - GitHub connector implementation
- `src/shopify.rs` - Shopify connector implementation

Basic steps:
1. Add config struct to `config.rs`
2. Create connector in `src/myconnector.rs`
3. Add feature flag to `Cargo.toml`
4. Wire up in `scheduler.rs` and collector's `serve.rs`
