# Benchmark Results

## Benchmark Matrix

Apple M4 Pro (aarch64) | 12 cores | 25.8 GB | 5 clients | 50M events/test | null sink

| Source | b500 | b100 | b10 | b3 |
|--------|------|------|------|------|
| TCP Binary | 64.28M | 55.15M | 6.93M | 1.41M |
| HTTP FBS | 24.25M | 8.28M | 1.04M | 321.15K |
| HTTP JSON | 2.14M | 2.25M | 848.38K | 306.63K |
| Syslog TCP | 8.71M | 8.49M | 8.34M | 1.99M |
| Syslog UDP | 912.04K | 921.22K | 919.27K | 916.71K |

Generated with: `tell-bench all`

## Protocol Comparison

| Protocol | Pros | Cons |
|----------|------|------|
| TCP Binary | Highest throughput, lowest overhead | Requires binary client |
| HTTP FBS | REST-compatible, good throughput | HTTP overhead |
| HTTP JSON | Human-readable, easy debugging | Serialization overhead |
| Syslog TCP | Standard protocol, reliable | Lower throughput than binary |
| Syslog UDP | Fire-and-forget, low latency | Lossy, kernel buffer limited |

---

## Message Size Benchmark

Throughput at different payload sizes (TCP Binary, 5 clients, 500 events/batch):

| Msg Size | Events/sec | MB/s |
|----------|------------|------|
| 100 B | 28.7M | 10 |
| 500 B | 222M | 254 |
| 1 KB | 280M | 600 |
| 5 KB | 362M | 3,671 |
| 10 KB | 363M | 7,311 |
| 50 KB | 133M | 13,358 |
| 100 KB | 68M | 13,614 |
| 500 KB | 12.6M | 12,586 |
| 1 MB | 6.5M | 12,960 |

Peak events/sec at 10 KB payload. Peak throughput at 100 KB payload.

---

## Micro-benchmarks

### TCP Source - Wire Message Parsing

| Scenario | Events/batch | Events/sec |
|----------|--------------|------------|
| realtime_small | 10 | 156M |
| typical | 100 | 380M |
| high_volume | 500 | 357M |
| large_events | 100 | 71M |

### Pipeline Overhead (NullSink)

| Scenario | Events/batch | Events/sec |
|----------|--------------|------------|
| realtime_small | 10 | 21.4M |
| typical | 100 | 218M |
| high_volume | 500 | 1.05B |
| large_events | 100 | 202M |

### DiskBinary Sink (with I/O)

| Scenario | Events/batch | Events/sec |
|----------|--------------|------------|
| realtime_small | 10 | 86K |
| typical | 100 | 848K |
| high_volume | 500 | 4.0M |
| large_events | 100 | 825K |

### Syslog Processing

| Source | Messages | Msg/sec |
|--------|----------|---------|
| TCP | 1000 | 15M |
| UDP | 1000 | 150M |

### Tap System Overhead

| Mode | Time/batch | Overhead |
|------|------------|----------|
| No tap | 4.48 us | baseline |
| Tap idle | 4.55 us | +1.5% |
| Tap streaming | 4.99 us | +11.4% |
