# Benchmark Results

## System

```
Apple M4 Pro (aarch64) | 12 cores | 24 GB | Darwin 26.1
```

## Performance

| Test | Events/sec | Throughput |
|------|------------|------------|
| TCP to Blackhole (5 clients, 400M events) | 65M | 13 GB/s |

```
./target/release/loadtest --events 400000000 -c 5

[   1.0s]      64.57M events/s |  12.9 GB/s |   64639000 events ( 16.2%)
[   2.0s]      69.36M events/s |  13.9 GB/s |  134070000 events ( 33.5%)
[   3.0s]      68.32M events/s |  13.7 GB/s |  202454500 events ( 50.6%)
[   4.0s]      67.33M events/s |  13.5 GB/s |  269853000 events ( 67.5%)
[   5.0s]      66.47M events/s |  13.3 GB/s |  336391000 events ( 84.1%)
```

### Comparison

| Collector | Test | Throughput |
|-----------|------|------------|
| Collector | TCP to Blackhole | ~13 GB/s |
| Vector | TCP to Blackhole | ~86 MiB/s |

---

## Detailed Results

### Scenarios

| Name | Events/Batch | Event Size | Payload |
|------|--------------|------------|---------|
| realtime_small | 10 | 100 B | 1 KB |
| typical | 100 | 200 B | 20 KB |
| high_volume | 500 | 200 B | 100 KB |
| large_events | 100 | 1000 B | 100 KB |

### TCP Source - Wire Message Parsing

| Scenario | Time/msg | Wire msg/sec | Events/batch | Events/sec |
|----------|----------|--------------|--------------|------------|
| realtime_small | 64 ns | 15.6 M | 10 | 156 M |
| typical | 265 ns | 3.8 M | 100 | 380 M |
| high_volume | 1.4 µs | 715 K | 500 | 357 M |
| large_events | 1.4 µs | 706 K | 100 | 71 M |

### DiskBinary Sink - Binary Encoding

| Scenario | Time/batch | Events/batch | Events/sec |
|----------|------------|--------------|------------|
| realtime_small | 52 ns | 10 | 192 M |
| typical | 600 ns | 100 | 167 M |
| high_volume | 3.6 µs | 500 | 139 M |
| large_events | 2.3 µs | 100 | 44 M |

### DiskBinary Sink - Full Write (I/O)

| Scenario | Events/batch | Events/sec |
|----------|--------------|------------|
| realtime_small | 10 | 86 K |
| typical | 100 | 848 K |
| high_volume | 500 | 4.0 M |
| large_events | 100 | 825 K |

### Syslog TCP - Line Processing

| Messages | Msg/sec |
|----------|---------|
| 100 | 14 M |
| 500 | 15 M |
| 1000 | 15 M |

### Syslog UDP - Packet Processing

| Packets | Msg/sec |
|---------|---------|
| 100 | 96 M |
| 500 | 143 M |
| 1000 | 150 M |

### Syslog BatchBuilder - Message Add

| Msg Size | Msg/sec |
|----------|---------|
| 100 B | 256 M |
| 200 B | 179 M |
| 500 B | 96 M |
| 1000 B | 55 M |

### Pipeline → DiskBinarySink (Full Pipeline with I/O)

| Scenario | Events/batch | Events/sec |
|----------|--------------|------------|
| realtime_small | 10 | 86 K |
| typical | 100 | 826 K |
| high_volume | 500 | 4.0 M |
| large_events | 100 | 828 K |

### Pipeline → NullSink (Pipeline Overhead Only)

| Scenario | Events/batch | Events/sec |
|----------|--------------|------------|
| realtime_small | 10 | 21.4 M |
| typical | 100 | 218 M |
| high_volume | 500 | 1.05 B |
| large_events | 100 | 202 M |

### Network E2E: TcpSource → Router → NullSink

| Scenario | Events/batch | Events/sec |
|----------|--------------|------------|
| typical | 100 | 6.4 M |

### Network E2E: SyslogTcpSource → Router → NullSink

| Messages | Msg Size | Msg/sec |
|----------|----------|---------|
| 10,000 | 200 B | 205 K |
| 50,000 | 200 B | 211 K |
| 10,000 | 500 B | 195 K |

### Network E2E: SyslogUdpSource → Router → NullSink

| Messages | Msg Size | Msg/sec |
|----------|----------|---------|
| 10,000 | 200 B | 213 K |
| 50,000 | 200 B | 242 K |
| 10,000 | 500 B | 210 K |

### Tap System Overhead

| Scenario | Time/batch | Overhead |
|----------|------------|----------|
| No tap | 4.48 µs | baseline |
| Tap idle | 4.55 µs | +1.5% |
| Tap streaming | 4.99 µs | +11.4% |
