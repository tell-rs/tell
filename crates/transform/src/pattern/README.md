# pattern

Log pattern extraction using the Drain algorithm.

## What it does

- Extracts templates from unstructured log messages
- Clusters similar messages (e.g., "User 123 logged in" → "User <*> logged in")
- 3-level caching minimizes pattern matching overhead
- File-based persistence for pattern storage

## Drain Algorithm

Drain uses a fixed-depth tree to cluster log messages:

1. Tokenize message by whitespace
2. Navigate tree: length → first token → clusters
3. Find matching cluster (similarity threshold) or create new one
4. Return pattern ID

**Variable detection** (automatically replaced with `<*>`):
- Numbers: `123`, `-45.67`, `0x1a2b`
- IPs: `192.168.1.1`
- UUIDs: `550e8400-e29b-41d4-a716-446655440000`
- Paths: `/var/log/syslog`
- URLs: `https://example.com`
- Emails: `user@example.com`
- Timestamps: `12:34:56`

## Caching Strategy

```
Level 1: Exact message hash → pattern ID (70-80% hit rate)
Level 2: Template hash → pattern ID (normalized tokens)
Level 3: Drain tree lookup (fallback)
```

## Usage

```rust
use tell_transform::{PatternTransformer, PatternConfig};

let config = PatternConfig::default()
    .with_similarity_threshold(0.5)  // 0.0-1.0, higher = stricter
    .with_cache_size(100_000);

let transformer = PatternTransformer::new(config)?;

// After transform, batch has pattern IDs
let batch = transformer.transform(batch).await?;
let pattern_ids = batch.pattern_ids().unwrap();

// Get pattern template
let pattern = transformer.get_pattern(pattern_ids[0]);
println!("Template: {}", pattern.template);  // "User <*> logged in"
```

## Persistence

Patterns can be saved to JSON files:

```rust
let config = PatternConfig::default();
config.persistence = PersistenceConfig::default()
    .with_file("/var/lib/tell/patterns.json".into());

// Patterns auto-flush when batch threshold reached
// Or manually save all
transformer.save_patterns()?;
```

File format:
```json
{
  "version": 1,
  "patterns": [
    {"id": 1, "template": "User <*> logged in", "count": 42},
    {"id": 2, "template": "Error in <*>", "count": 15}
  ]
}
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `similarity_threshold` | 0.5 | Pattern matching strictness (0.0-1.0) |
| `max_child_nodes` | 100 | Drain tree branching factor |
| `cache_size` | 100,000 | L1 cache capacity |
| `persistence.enabled` | false | Enable file persistence |
| `persistence.file_path` | None | JSON file path |
| `persistence.flush_interval` | 5s | Auto-flush interval |
| `persistence.batch_size` | 100 | Patterns before auto-flush |

## Tests

```bash
cargo test -p tell-transform pattern
```

96 tests covering config, drain, cache, persistence, and transformer.
