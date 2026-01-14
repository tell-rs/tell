# cdp-transform

Transformer chain for in-flight batch modification with zero-cost when disabled.

## What it does

- Chains multiple transformers that modify batches in sequence
- Zero overhead when chain is empty (single boolean check)
- Extensible via `TransformerRegistry` for custom transformers
- Built-in pattern matcher using Drain algorithm for log clustering

## Modules

| Module | Purpose |
|--------|---------|
| `chain` | Sequential transformer execution |
| `registry` | Dynamic transformer creation from config |
| `lazy_batch` | Decode-on-demand wrapper (shared across chain) |
| `noop` | Pass-through transformer for testing |
| `pattern` | Log pattern extraction (Drain algorithm) |

## Usage

```rust
use cdp_transform::{Chain, PatternTransformer, PatternConfig, create_default_registry};

// Option 1: Direct creation
let pattern = PatternTransformer::new(PatternConfig::default())?;
let chain = Chain::new(vec![Box::new(pattern)]);

// Option 2: Registry-based (config-driven)
let registry = create_default_registry();
let transformer = registry.create("pattern_matcher", &config)?;
let chain = Chain::new(vec![transformer]);

// Transform batches (async)
let transformed = chain.transform(batch).await?;

// Access pattern IDs (for log batches)
if let Some(pattern_ids) = transformed.pattern_ids() {
    // pattern_ids[i] corresponds to message i
}
```

## Extending with Custom Transformers

```rust
use cdp_transform::{Transformer, TransformResult, TransformerFactory, TransformerConfig};

struct MyTransformer;

impl Transformer for MyTransformer {
    fn transform<'a>(&'a self, batch: Batch)
        -> Pin<Box<dyn Future<Output = TransformResult<Batch>> + Send + 'a>>
    {
        Box::pin(async move {
            // Modify batch here
            Ok(batch)
        })
    }

    fn name(&self) -> &'static str { "my_transformer" }
}

struct MyFactory;

impl TransformerFactory for MyFactory {
    fn create(&self, config: &TransformerConfig) -> TransformResult<Box<dyn Transformer>> {
        Ok(Box::new(MyTransformer))
    }
    fn name(&self) -> &'static str { "my_transformer" }
}

// Register
registry.register("my_transformer", MyFactory);
```

## Zero-Cost Design

- Empty chain: single `if !enabled` check, returns batch unchanged
- Disabled transformers filtered at construction, not per-batch
- `LazyBatch` decodes messages only if a transformer needs them
- Pattern cache: 70-80% hit rate avoids Drain tree traversal

## Tests

```bash
cargo test -p cdp-transform
```

135 tests covering chain, registry, lazy_batch, noop, and pattern modules.
