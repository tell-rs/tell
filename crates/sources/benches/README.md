# TCP Source Benchmarks

Run: `cargo bench -p tell-sources --bench tcp`

Measures wire message parsing (frame + FlatBuffer + auth + IP conversion).

Results: See `/BENCHMARK_RESULTS.md`
Scenarios: See `/crates/bench/src/scenarios.rs`
