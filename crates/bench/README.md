# cdp-bench

Shared benchmark configuration for CDP Collector crates.

## TL;DR

```rust
use cdp_bench::{SCENARIOS, BenchScenario};

for scenario in SCENARIOS {
    // scenario.name, scenario.events_per_batch, scenario.event_size
}
```

## Scenarios

| Name | Events/Batch | Event Size | Payload |
|------|--------------|------------|---------|
| realtime_small | 10 | 100 B | 1 KB |
| typical | 100 | 200 B | 20 KB |
| high_volume | 500 | 200 B | 100 KB |
| large_events | 100 | 1000 B | 100 KB |
