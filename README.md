<p align="center">
  <img src="assets/hero.png" alt="Tell" width="600">
</p>

<p align="center">
  <strong>Analytics that tell the whole story.</strong><br> 
  <sub>Events, logs, business data — one platform. The full picture.</sub>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/status-alpha-orange" alt="Alpha">
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> &bull;
  <a href="configs/example.toml">Config</a> &bull;
  <a href="https://github.com/REPO/discussions">Community</a>
</p>

---

**Alpha** — Shipping daily. First release coming soon.

## What is this?

Product analytics, logs, and business data — unified. Track events and funnels. Ingest structured logs. Connect sources like GitHub and Shopify. Query everything. Build dashboards.

**Stupid fast.** 65M events/sec on batched events. High-performance Rust pipeline with sub-millisecond latency.

**Connect everything.** Product events, structured logs, GitHub (stars, PRs, commits), Shopify (orders, customers). More coming.

**One binary.** No containers. No Docker Compose. Runs on a Raspberry Pi or your enterprise cluster.

**Yours forever.** Self-hosted. Your data never leaves your servers. No cloud dependency.

## Quick Start

```bash
curl -fsSL https://tell.rs/install.sh | sh   # coming soon

# or build from source
cargo install --path crates/collector
cargo install --path crates/bench --bin loadtest

tell                         # start server (port 50000)

# another terminal
loadtest -e 10000            # send test data
tell tail                    # watch live
```

See [configs/example.toml](configs/example.toml) for options.

## Platform

 - **Sources** — TCP (high-throughput), Syslog (TCP/UDP).
 - **Connectors** — GitHub (stars, forks, issues, PRs), Shopify (orders, revenue, customers). Scheduled pulls with automatic retry.
 - **Routing** — Route any source to any sink. Fan-out to multiple destinations. Filter by source, type, workspace.
 - **Transformers** — Pattern matching for log clustering, Filter, Redact, Reduce.
 - **Sinks** — ClickHouse, Parquet (query with Spark, DuckDB, Athena), Disk (binary + plaintext), Stdout, Forwarder.
 - **Tap** — Live streaming with `tell tail`. Filter by workspace, source, type. Sampling and rate limiting built in.
 - **Auth** — API keys with workspace isolation. Hot reload without restart.
 - **Config** — TOML with sensible defaults. Validation catches mistakes before you run.

 - **API & Web** — (currently golang repo, is being rewritten) Go + Fiber server, Vite + React + Shadcn UI. ~95% feature-complete. REST API for DAU/WAU/MAU, funnels, retention, breakdowns, comparisons, raw drill-down. Dashboards with markdown and public sharing. Multi-tenancy active. RBAC, LDAP/SSO/SAML integration in progress. Auth providers: Local, Clerk, WorkOS.

 - **SDKs** — Go, Swift, Flutter (95%), C++. TypeScript coming.

## Experimental

These features are in development:

- Transformers (Filter, Redact, Reduce)
- Syslog source (TCP/UDP)
- Source connectors (GitHub, Shopify)
- Binary sink format
- Live tail
- Protocols: Metrics, Traces

## Performance

Benchmarked on Apple M4 Pro (TCP to null sink):

```
./target/release/loadtest --events 400000000 -c 5
───────────────────────────────────────────────────────────────────────
Load Test | 127.0.0.1:50000 | 5 clients | 400M events | batch 500
System    | Apple M4 Pro (aarch64) | 12 cores | 24.0 GB
───────────────────────────────────────────────────────────────────────
[   1.0s]    64.57M events/s |  12924.1 MB/s |   64639000 events
[   2.0s]    69.36M events/s |  13881.9 MB/s |  134070000 events
[   3.0s]    68.32M events/s |  13673.7 MB/s |  202454500 events
[   4.0s]    67.33M events/s |  13475.8 MB/s |  269853000 events
[   5.0s]    66.47M events/s |  13302.9 MB/s |  336391000 events
```

### Comparison

| | Events/sec | Throughput |
|---|---|---|
| **Tell** | **65M** | **13 GB/s** |
| Vector | — | 86 MiB/s |
| PostHog | 100K | — |

<sub>TCP to blackhole. Same test methodology. See [benchmarks/RESULTS.md](benchmarks/RESULTS.md) for details.</sub>

Running in production since August 2025. ~4M events/sec sustained per node on Hetzner CAX11.

## License

Source-available. Full product, free for most.

```
Individual                     Free           No key required
Company < $1M/year revenue     Free           Key required
Company $1M-$10M/year          $299/mo        Pro
Company > $10M/year            $2,000/mo+     Enterprise
```

**You can:** Use, modify, self-host, fork. All features included.

**You cannot:** Offer as a hosted service to others.

See [LICENSE](../LICENSE) for full terms.

## Team

Built by the founder of Logpoint (European SIEM, acquired).

---

<p align="center">
  <a href="https://tell.rs">tell.rs</a>
</p>
