# cdp-tap

Live streaming tap point for the CDP collector - enables `tail -f` style debugging.

## TL;DR

```
Collector Pipeline                         CLI Client
      │                                        │
      ▼                                        │
  TapPoint ──(Unix socket)──► TapServer ◄─────┘
      │                            │
      │  Arc<Batch> broadcast      │  TapMessage protocol
      │  O(1) metadata filter      │  Length-prefixed binary
      │  Client rate limiting      │
      ▼                            ▼
  ReplayBuffer              SubscriberManager
  (last N batches)          (per-client state)
```

## Key Design

- **Zero-cost when idle**: Single atomic load, immediate return if no subscribers
- **No work without clients**: Replay buffer only fills when subscribers exist
- **Metadata-only filtering**: O(1) HashSet lookups, no FlatBuffer decode
- **Client controls rate**: sample_rate + max_batches_per_sec in subscribe request
- **Auto-cleanup**: Disconnected clients removed automatically

## Performance

With no `collector tail` clients connected:
```
tap_point.tap(batch)  →  atomic load  →  return (< 1ns)
```

With clients connected:
- Atomic increment (tap_count)
- Replay buffer write (RwLock + Vec store)
- Broadcast to matching subscribers (filter + channel send)

## Usage

```rust
// In collector startup
let tap_point = Arc::new(TapPoint::new());
router.set_tap_point(Arc::clone(&tap_point));

// Start server (Unix socket)
let server = TapServer::with_defaults(tap_point);
tokio::spawn(server.run());
```

## Wire Protocol

All messages: `[4-byte BE length][payload]`

| Message | Direction | Purpose |
|---------|-----------|---------|
| Subscribe | C→S | Start streaming with filters |
| Batch | S→C | Batch envelope + raw FlatBuffer |
| Heartbeat | S→C | Keep-alive (30s) |
| Error | S→C | Error message |

## Filters

```rust
SubscribeRequest::new()
    .with_workspaces(vec![42])        // workspace_id filter
    .with_sources(vec!["tcp".into()]) // source_id filter
    .with_types(vec![0, 1])           // batch_type (Event=0, Log=1)
    .with_sample_rate(0.01)           // 1% sampling
    .with_rate_limit(1000)            // max 1000 batches/sec
    .with_last_n(100)                 // replay last 100 on connect
```

## Socket Path

Default: `/tmp/tell-tap.sock`
