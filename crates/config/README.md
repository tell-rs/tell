# tell-config

TOML configuration loading and validation for Tell.

## What it does

- Loads configuration from TOML files
- Validates consistency (port conflicts, missing fields, routing references)
- Provides typed config structs with sensible defaults

## Minimal Config

```toml
[[sources.tcp]]
port = 50000

[sinks.stdout]
type = "stdout"
```

## Full Example

See `configs/example.toml` for all available options.

## Usage

```rust
use tell_config::Config;
use std::str::FromStr;

// From file
let config = Config::from_file("configs/config.toml")?;

// From string
let config = Config::from_str(toml_string)?;

// Access settings
for tcp in &config.sources.tcp {
    println!("TCP port: {}", tcp.port);
}

for (name, sink) in config.sinks.iter() {
    println!("Sink: {} ({})", name, sink.type_name());
}
```

## Config Sections

| Section | Description |
|---------|-------------|
| `global` | Buffer sizes, queue sizes, API keys file |
| `log` | Log level, format, output |
| `metrics` | Reporting interval, format, include flags |
| `sources` | TCP, TCP Debug, Syslog TCP/UDP |
| `sinks` | Null, Stdout, DiskBinary, DiskPlaintext, ClickHouse, Parquet, Forwarder |
| `routing` | Source → Sink mapping rules |

## Tests

```bash
cargo test -p tell-config
```

78 tests covering parsing, defaults, validation, and edge cases.