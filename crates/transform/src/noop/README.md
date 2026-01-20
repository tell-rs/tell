# noop

Pass-through transformer for testing and benchmarking.

## What it does

- Returns batches unchanged
- Zero overhead (just returns `Ok(batch)`)
- Useful for testing chain infrastructure
- Benchmarking transformer dispatch cost

## Usage

```rust
use tell_transform::{NoopTransformer, Chain};

let chain = Chain::new(vec![Box::new(NoopTransformer)]);
let result = chain.transform(batch).await?;
// result == batch (unchanged)
```

## Tests

```bash
cargo test -p tell-transform noop
```

6 tests verifying pass-through behavior.
