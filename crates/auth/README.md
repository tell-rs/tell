# cdp-auth

API key management with zero-allocation validation for CDP Collector.

## What it does

- Validates API keys in O(1) with no allocations in hot path
- Maps API keys (16 bytes) to workspace IDs
- Thread-safe with `RwLock` (multiple readers, single writer)
- Supports hot reload without restart

## File Format

API keys are stored in a simple text file:

```text
# comments start with #
000102030405060708090a0b0c0d0e0f:workspace_1
deadbeefdeadbeefdeadbeefdeadbeef:workspace_2
cafebabecafebabecafebabecafebabe:workspace_3
```

Each line contains:
- 32 hex characters (16 bytes) for the API key
- Colon separator
- Workspace ID (string)

## Usage

```rust
use cdp_auth::{ApiKeyStore, WorkspaceId};
use std::str::FromStr;

// Load from file
let store = ApiKeyStore::from_file("configs/apikeys.conf")?;

// Or parse from string
let store = ApiKeyStore::from_str("deadbeef...:workspace_1")?;

// Validate key (hot path - zero allocation)
let key: [u8; 16] = received_key;
if let Some(workspace) = store.validate(&key) {
    println!("Valid key for workspace: {}", workspace);
}

// Hot reload (atomic - preserves old keys on error)
store.reload("configs/apikeys.conf")?;
```

## Zero-Copy Design

- API keys stored as `[u8; 16]` arrays (stack allocated)
- `WorkspaceId` uses `Arc<str>` (cheap clone)
- Validation is O(1) HashMap lookup
- No allocations in hot path

## Thread Safety

```rust
use std::sync::Arc;
use cdp_auth::SharedApiKeyStore;

// Share across threads
let store: SharedApiKeyStore = Arc::new(ApiKeyStore::from_file("keys.conf")?);

// Multiple readers can validate concurrently
let workspace = store.validate(&key);

// Single writer for reload
store.reload("keys.conf")?;
```

## Tests

```bash
cargo test -p cdp-auth
```

53 tests covering parsing, validation, reload, concurrency, and edge cases.