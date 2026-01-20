# Disk Binary Sink

High-performance binary storage with metadata headers. Used for **archival** and **replay**.

## TL;DR

```toml
[sinks.disk_binary]
type = "disk_binary"
path = "archive/"
rotation = "hourly"
compression = "lz4"
```

## When to Use

- High-throughput archival (40M+ events/sec)
- Data replay and reprocessing
- Backup before transformations
- Maximum write performance

## Configuration

```toml
[sinks.disk_binary]
type = "disk_binary"
path = "archive/"                # Output directory (required)
rotation = "hourly"              # hourly, daily (default: daily)
compression = "lz4"              # none, lz4 (default: none)
buffer_size = 33554432           # 32MB (default)
metrics_enabled = true
metrics_interval = "10s"
```

## File Layout

```
{path}/
└── {workspace_id}/
    └── {date}/
        └── {hour}/              # Only with hourly rotation
            └── data.bin         # or data.bin.lz4
```

## File Format

Each message is stored with a 24-byte metadata header:

```
┌─────────────────────────────────────┐
│ Message 1                           │
│  - batch_timestamp: u64 (8 bytes)   │  Unix ms when sink processed
│  - source_ip: [u8; 16] (16 bytes)   │  IPv6 format (IPv4 mapped)
│  - length: u32 (4 bytes, big-end)   │
│  - data: [u8; length]               │  FlatBuffer message
├─────────────────────────────────────┤
│ Message 2                           │
│  - batch_timestamp: u64             │
│  - source_ip: [u8; 16]              │
│  - length: u32                      │
│  - data: [u8; length]               │
├─────────────────────────────────────┤
│ ...                                 │
└─────────────────────────────────────┘
```

## Reading Binary Files

### CLI Tool

```bash
# Read single file to stdout
tell read archive/42/2024-01-15/data.bin

# Redirect to file
tell read archive/42/2024-01-15/data.bin > output.log

# Convert entire directory
tell read archive/42/ output/42/
```

Output format:
```
HH:MM:SS.mmm 192.168.1.1 log info    my-service@host {"message":"started"}
HH:MM:SS.mmm 192.168.1.1 event track page_view {"url":"/home"}
```

### Rust API

```rust
use sinks::disk_binary::{BinaryReader, BinaryMessage};

// Open file (auto-detects .lz4 compression)
let reader = BinaryReader::open("archive/42/2024-01-15/data.bin")?;

// Iterate messages
for result in reader.messages() {
    let msg: BinaryMessage = result?;

    // Access metadata
    println!("Timestamp: {}", msg.metadata.batch_timestamp);
    println!("Source IP: {}", msg.metadata.source_ip_string());

    // Process FlatBuffer message
    let flat_batch = FlatBatch::parse(&msg.data)?;
    // ...
}
```

## Compression

LZ4 compression provides:
- Minimal CPU overhead
- Streaming decompression

```toml
compression = "lz4"
```

Files are named `data.bin.lz4` when compressed. `BinaryReader` auto-detects.

## Performance

| Metric | Value |
|--------|-------|
| Write throughput | 40M+ events/sec |
| Buffer size | 32MB (configurable) |
| Syscalls | Minimized via buffering |

## Metrics

| Metric | Description |
|--------|-------------|
| `batches_received` | Total batches processed |
| `bytes_written` | Bytes written to disk |
| `files_created` | Files created (rotations) |
| `write_errors` | Write failures |

## Notes

- Uses atomic file rotation (zero data loss during rotation)
- 32MB write buffers minimize syscalls
- For human-readable output, use `disk_plaintext`
- For analytics queries, use `parquet`
