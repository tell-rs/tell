# tell-bench

Benchmark configuration and testing tools for Tell.

## tell-bench CLI

Unified benchmark and testing tool for all Tell source types.

### Installation

```bash
cargo install --path crates/bench --force
# or
cargo build -p tell-bench --release
```

### Usage

```bash
# TCP FlatBuffer source (binary protocol, highest throughput)
tell-bench tcp load -c 5 --events 10000000
tell-bench tcp benchmark -c 5
tell-bench tcp crash

# HTTP JSON source (JSONL format)
tell-bench http-json load -c 5 --events 1000000
tell-bench http-json benchmark -c 5

# HTTP FlatBuffer source (binary over HTTP)
tell-bench http-fbs load -c 5 --events 10000000
tell-bench http-fbs benchmark -c 5

# Syslog TCP source
tell-bench syslog-tcp load -c 5 --events 10000000 -s 127.0.0.1:50514
tell-bench syslog-tcp benchmark -c 5
tell-bench syslog-tcp crash
tell-bench syslog-tcp oversized -c 5 -d 30 -m 65536

# Syslog UDP source
tell-bench syslog-udp load -c 5 --events 10000000 -s 127.0.0.1:50515
tell-bench syslog-udp benchmark -c 5
tell-bench syslog-udp crash
tell-bench syslog-udp oversized -c 5 -d 30 -m 65536
```

### Modes

| Mode | Description |
|------|-------------|
| `load` | Throughput test with concurrent clients |
| `benchmark` | Message size scaling test |
| `crash` | Malformed/malicious data resilience test |
| `oversized` | Oversized message test - verify server handles large messages without DoS |

### TCP Options

```
-c, --clients       Number of concurrent clients (default: 5)
-e, --events        Total events to send (default: 10M)
-b, --batch-size    Messages per batch (default: 1000)
-s, --server        Server address (default: 127.0.0.1:50000)
-k, --api-key       API key in hex (default: 00010203...)
```

### HTTP Options

```
-c, --clients       Number of concurrent clients (default: 5)
-e, --events        Total events to send (default: 1M)
-b, --batch-size    Events per request (default: 100)
-u, --url           Server URL (default: http://127.0.0.1:8080)
-k, --api-key       API key in hex (default: 00010203...)
```

### Syslog Options

```
-c, --clients       Number of clients (default: 5)
-e, --events        Total messages (default: 10M)
-s, --server        Server address (default: 127.0.0.1:50514 for TCP, 127.0.0.1:50515 for UDP)
-f, --format        Syslog format: rfc3164, rfc5424 (default: rfc3164)
```

### Oversized Options (syslog-tcp/syslog-udp only)

```
-c, --clients         Number of clients (default: 5)
-d, --duration        Test duration in seconds (default: 30)
-m, --message-size    Message size in bytes (default: 65536)
--oversized-ratio     Ratio of oversized messages 0.0-1.0 (default: 1.0)
-s, --server          Server address
```

Verifies server handles oversized messages without DoS. Check `messages_malformed` counter after test.

## Benchmark Library

Shared benchmark scenarios for criterion tests.

```rust
use tell_bench::{SCENARIOS, BenchScenario};

for scenario in SCENARIOS {
    // scenario.name, scenario.events_per_batch, scenario.event_size
}
```

### Scenarios

| Name | Events/Batch | Event Size | Payload |
|------|--------------|------------|---------|
| realtime_small | 10 | 100 B | 1 KB |
| typical | 100 | 200 B | 20 KB |
| high_volume | 500 | 200 B | 100 KB |
| large_events | 100 | 1000 B | 100 KB |
