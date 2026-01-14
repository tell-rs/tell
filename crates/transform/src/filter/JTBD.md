# Filter Transformer

> **Job:** Drop unwanted events at the edge to reduce noise and cost.

## When To Use

| Scenario | Condition | Action | Verdict |
|----------|-----------|--------|---------|
| Health check spam | `path == "/health"` | Drop | Use filter |
| Debug logs in prod | `level == "debug"` | Drop | Use filter |
| Internal traffic | `source_ip starts_with "10."` | Drop | Use filter |
| Bot traffic | `user_agent contains "bot"` | Drop | Use filter |
| Keep only errors | `level != "error"` | Drop non-matches | Use filter |
| Exclude test events | `env == "test"` | Drop | Use filter |
| Normal traffic | All events valuable | Keep all | Skip - zero-copy |

**Rule:** If not dropping >10% of events, skip filter for zero-copy.

## Condition Syntax

Simple JSON field matching (no VRL/DSL complexity):

```toml
[[routing.rules.transformers]]
type = "filter"
action = "drop"  # or "keep"

# Match conditions (AND logic within a rule)
[routing.rules.transformers.condition]
field = "level"
operator = "eq"        # eq, ne, contains, starts_with, ends_with, regex, exists
value = "debug"
```

### Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `eq` | Equals | `level eq "debug"` |
| `ne` | Not equals | `level ne "error"` |
| `contains` | String contains | `path contains "/api"` |
| `starts_with` | String prefix | `ip starts_with "10."` |
| `ends_with` | String suffix | `email ends_with "@test.com"` |
| `regex` | Regex match | `path regex "^/v[0-9]+/"` |
| `exists` | Field exists | `user_id exists` |
| `gt`, `lt`, `gte`, `lte` | Numeric comparison | `status_code gte 400` |

### Multiple Conditions

```toml
# Drop if level=debug AND env=production
[[routing.rules.transformers]]
type = "filter"
action = "drop"
match = "all"  # all (AND) or any (OR)

[[routing.rules.transformers.conditions]]
field = "level"
operator = "eq"
value = "debug"

[[routing.rules.transformers.conditions]]
field = "env"
operator = "eq"
value = "production"
```

## Not This Job

| Need | Use Instead |
|------|-------------|
| Mask sensitive fields | `redact` transformer |
| Aggregate duplicates | `reduce` transformer |
| Sample percentage | `sample` transformer |
| Transform field values | `remap` transformer (future) |

## Metrics

- `filter_events_dropped` - Events dropped by filter
- `filter_events_passed` - Events that passed through
- `filter_drop_rate` - Percentage of events dropped
