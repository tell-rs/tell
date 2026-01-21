<p align="center">
  <strong>Analytics that tell the whole story.</strong><br> 
  <sub>Events, logs, business data — one platform. The full picture.</sub>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/status-alpha-orange" alt="Alpha">
</p>

Product analytics, logs, and business data — unified. Track events and funnels. Ingest structured logs. Connect sources like GitHub and Shopify. Query everything. Build dashboards.

- **Stupid fast.** 64M events/sec on batched TCP. High-performance Rust pipeline with sub-millisecond latency.
- **Connect everything.** Product events, structured logs, business data (GitHub, Shopify, more). Query them together — that's the point.
- **One binary.** No containers. No Docker Compose. Runs on a Raspberry Pi or your enterprise cluster.
- **Yours forever.** Self-hosted. Your data never leaves your servers. No cloud dependency.


## Solutions (vertical use-cases)

- **OT / Industrial** — Off-grid, secure-by-design log collection/forwarding for air-gapped + compliance-heavy networks. One Rust binary, deterministic performance.
- **Startups (apps + SaaS)** — One SDK for product events + structured logs, plus 360 business signals (e.g., GitHub, Shopify). Funnels/retention with correlated logs.
- **DTC / Ecommerce** — Near-real-time 360 across checkout funnel + backend reliability + orders/refunds + campaign KPIs. Answer “what changed?” with driver drill-down.
- **Enterprise** — Fast self-hosted logs/observability + data pipeline + analytics layer. Replace heavy ELK-style ops and unify dashboards across teams.


**Alpha** — First release coming soon. Tell Cloud coming soon. License keys at [tell.rs](https://tell.rs) (coming soon).


## Quick Start

```bash
curl https://tell.rs | sh

# or build from source
cargo install --path crates/tell            # binary: 'tell'
cargo install --path crates/bench           # binary: 'tell-bench'

tell                                        # start server (port 50000)

# another terminal – benchmarking
tell-bench tcp load -e 1000000 -c 5         # send test data

# or send test data and live tail
tell tail                                   # watch live
tell test                                   # send few demo data packets

# query your data (ClickHouse or local Arrow files)
tell query "SELECT event_name, COUNT(*) FROM events GROUP BY event_name"
```

### ClickHouse Setup (Optional)

```bash
# install clickhouse: https://clickhouse.com/docs/install
tell clickhouse init acme              # creates database, tables, users, config
```

This creates configs/acme.toml with generated credentials. Tell also works with local disk sinks (Parquet, Arrow, binary)

## Platform

- **Sources** — TCP (high-throughput), Syslog (TCP/UDP).
- **Connectors** — GitHub (stars, forks, issues, PRs), Shopify (orders, revenue, customers). Scheduled pulls with automatic retry.
- **Routing** — Route any source to any sink. Fan-out to multiple destinations. Filter by source, type, workspace.
- **Transformers** — Pattern matching for log clustering, Filter, Redact, Reduce.
- **Sinks** — ClickHouse, Parquet (query with Spark, DuckDB, Athena), Disk (batch binary, plaintext, optional LZ4), Stdout, Forwarder, Arrow
- **Tap** — Live streaming with `tell tail`. Filter by workspace, source, type. Sampling and rate limiting built in.
- **Query** — SQL queries via `tell query`. ClickHouse for production, Polars for local Arrow IPC files.
- **Auth** — API keys with workspace isolation. Hot reload without restart.
- **Config** — TOML with sensible defaults. Validation catches mistakes before you run.

- **API & Web** — (currently golang repo, is being rewritten to rust) Go + Fiber server, Vite + React + Shadcn UI. ~95% feature-complete. REST API for DAU/WAU/MAU, funnels, retention, breakdowns, comparisons, raw drill-down. Dashboards with markdown and public sharing. Multi-tenancy active. RBAC, LDAP/SSO/SAML integration in progress. Auth providers: Local, Clerk, WorkOS.

- **SDKs** — Go, Swift, Flutter (95%), C++.

## Experimental

These features are functional but need further testing:

- Source: Syslog (TCP/UDP) + HTTP (JSONL/FBS) 
- Source connectors (GitHub, Shopify)
- Sinks: Binary, Parquet, Arrow
- Live tail

## Coming Soon

These features are in active development:

- **Metrics & Traces** — Draft protocol and pipeline, stdout decoder working
- **Transformers** — Filter, Redact, Reduce, Pattern (double decoding issue, Pattern is draft)
- **API (rust)** — Rewrite of Go API
- **SDKs** – Rust and Typescript

## Performance

Apple M4 Pro | 12 cores | 5 clients | batch 500 | null sink

| Source | b500 | b100 | b10 | b3 |
|--------|------|------|------|------|
| TCP Binary | **64M** | 55M | 6.9M | 1.4M |
| HTTP FBS | 24M | 8.3M | 1.0M | 321K |
| HTTP JSON | 2.1M | 2.3M | 848K | 307K |
| Syslog TCP | 8.7M | 8.5M | 8.3M | 2.0M |

See [benchmarks/RESULTS.md](benchmarks/RESULTS.md) for full results.


## Compared to

| | Tell | PostHog | Mixpanel | Vector |
|--|------|---------|----------|--------|
| Events | ✓ | ✓ | ✓ | ✗ |
| Logs | ✓ | ✗ | ✗ | ✓ |
| Business connectors | ✓ | ✗ | ✗ | ✗ |
| SDKs (track, identify) | ✓ | ✓ | ✓ | ✗ |
| Dashboards | ✓ | ✓ | ✓ | ✗ |
| Multi-source pipeline | ✓ | ✗ | ✗ | ✓ |
| Routing + transforms | ✓ | ✗ | ✗ | ✓ |
| Multiple sinks | ✓ (CH, Arrow, Parquet, disk) | ClickHouse | ✗ | ✓ |
| CLI (`tail`, `query`) | ✓ | ✗ | ✗ | ✗ |
| Self-host (one binary) | ✓ | Docker Compose | ✗ | ✓ |
| Throughput | 64M/s | ~100K/s | — | 86 MiB/s |
| Language | Rust | Python | — | Rust |
| Source available | ✓ | ✓ | ✗ | ✓ |


## License

Source-available. Full product, free for most. Revenue-based pricing.

```
Company < $1M/year             Free           No key required
Company $1M-$10M/year          $299/mo        Pro (key required)
Company > $10M/year            $2,000/mo+     Enterprise (key required)
Managed services               $99/mo min     Per client, by client revenue
```

Government, education, and non-profit: contact us.

**You can:** Use, modify, self-host, fork. All features included.

**You cannot:** Offer as a hosted service to others (unless licensed for Managed Services).

See [LICENSE](LICENSE) for full terms.

## Team

Built by the founder of Logpoint (European SIEM, acquired).
