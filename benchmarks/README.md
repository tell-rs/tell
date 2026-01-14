# Benchmarks

## Micro-benchmarks (Criterion)

```bash
cargo bench --all                              # All
cargo bench -p cdp-sources --bench tcp         # TCP source
cargo bench -p cdp-sources --bench syslog      # Syslog TCP/UDP
cargo bench -p cdp-sinks                       # DiskBinary sink
cargo bench -p cdp-pipeline                    # Router + tap overhead
cargo bench -p cdp-protocol                    # Batch ops
cargo bench -p cdp-bench --bench e2e           # Pipeline E2E
```

## Load Test

Multi-client TCP load test:

```bash
# Build
cargo build --release -p cdp-bench --bin loadtest

# Run (collector must be running on 127.0.0.1:50000)
./target/release/loadtest

# Custom options
./target/release/loadtest --clients 5 --events 400000000
./target/release/loadtest -c 100 -e 100000000 -b 500 -s 127.0.0.1:50000
```

### CLI Flags

```
-s, --server <ADDR>      Server address [default: 127.0.0.1:50000]
-c, --clients <N>        Concurrent clients [default: 100]
-e, --events <N>         Total events to send [default: 100000000]
-b, --batch-size <N>     Events per batch [default: 500]
-k, --api-key <HEX>      API key [default: 000102030405060708090a0b0c0d0e0f]
-r, --report-interval <S> Report interval secs [default: 1]
```

## Running the Collector

```bash
# Start collector with null sink (for benchmarking)
./target/release/collector --config configs/bench.toml
```

## Results

See [RESULTS.md](RESULTS.md)
