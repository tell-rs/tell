# cdp-protocol

Zero-copy FlatBuffer parsing for CDP Collector.

## What it does

- Parses FlatBuffer wire format without code generation
- Provides `Batch`, `SchemaType`, `SourceId` types
- Zero-copy message access via `bytes::Bytes`

## Key Types

```rust
use cdp_protocol::{FlatBatch, Batch, SchemaType, SourceId};

// Parse incoming FlatBuffer
let flat = FlatBatch::parse(&wire_bytes)?;
let api_key = flat.api_key()?;
let schema = flat.schema_type();
let data = flat.data()?;

// Internal batch representation
let batch = Batch::new(source_id, schema_type, messages);
let msg = batch.get_message(0);  // Zero-copy slice
```

## Performance

- Clone is O(1): ~60ns regardless of batch size
- Message access: 1.6B elem/sec sequential
- Batch building: 252M elem/sec

## Tests

```bash
cargo test -p cdp-protocol
```

143 tests covering batch, schema, source, flatbuf, and error handling.