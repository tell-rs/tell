# DiskBinary Sink Benchmarks

Run: `cargo bench -p cdp-sinks --bench disk_binary`

Measures:
- Binary encoding: Batch to on-disk format (CPU-bound)
- Full write: Encoding + disk I/O (I/O-bound)

Results: See `/BENCHMARK_RESULTS.md`
Scenarios: See `/crates/bench/src/scenarios.rs`
