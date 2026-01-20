# Benchmarks

## Load Testing (tell-bench)

See [crates/bench/README.md](../crates/bench/README.md) for the `tell-bench` CLI tool.

```bash
# Quick start
cargo build -p tell-bench --release
tell-bench tcp load -c 5 --events 10000000
tell-bench syslog-tcp load -c 5 --events 10000000
tell-bench syslog-udp load -c 5 --events 10000000
```

## Micro-benchmarks (Criterion)

```bash
cargo bench --all                              # All
cargo bench -p tell-sources --bench tcp        # TCP source
cargo bench -p tell-sources --bench syslog     # Syslog TCP/UDP
cargo bench -p tell-sinks                      # DiskBinary sink
cargo bench -p tell-pipeline                   # Router + tap overhead
cargo bench -p tell-protocol                   # Batch ops
cargo bench -p tell-bench --bench e2e          # Pipeline E2E
```

## Running Tell

```bash
# Start tell with null sink (for benchmarking)
./target/release/tell --config configs/bench.toml
```

## Results

See [RESULTS.md](RESULTS.md)
