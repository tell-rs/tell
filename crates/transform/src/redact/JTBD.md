# Redact Transformer

> **Job:** Remove or pseudonymize PII from events to enable compliant analytics.

## When To Use

| Scenario | Data | Strategy | Verdict |
|----------|------|----------|---------|
| GDPR compliance | Email in logs | Hash (pseudonymize) | Use redact |
| HIPAA logs | SSN, patient ID | Full redact | Use redact |
| Analytics tracking | User email | Hash (keep correlation) | Use redact |
| Payment logs | Credit card | Full redact | Use redact |
| Internal tools | No PII | None | Skip - zero-copy |

**Rule:** If you need to correlate events by user without storing PII, use hash. If PII must be fully removed, use redact.

## Masking Strategies

### 1. Full Redact (default)
Replaces value with `[REDACTED]`:
```
"email": "john@example.com" → "email": "[REDACTED]"
```

### 2. Pseudonymize (deterministic hash)
Replaces with keyed HMAC-SHA256 hash (truncated):
```
"email": "john@example.com" → "email": "usr_a1b2c3d4e5f6"
```

Benefits:
- Same input → same hash (enables correlation)
- Cannot reverse without key
- Key is per-workspace (configurable)
- Prefix indicates type (`usr_`, `phn_`, `ssn_`)

## Built-in Patterns

| Pattern | Regex | Countries | Example |
|---------|-------|-----------|---------|
| `email` | RFC 5322 simplified | Universal | `user@domain.com` |
| `phone` | E.164 + common formats | Universal | `+1-555-123-4567` |
| `credit_card` | Luhn-valid 13-19 digits | Universal | `4111-1111-1111-1111` |
| `ssn_us` | `\d{3}-\d{2}-\d{4}` | US | `123-45-6789` |
| `cpr_dk` | `\d{6}-\d{4}` | Denmark | `010190-1234` |
| `nino_uk` | `[A-Z]{2}\d{6}[A-Z]` | UK | `AB123456C` |
| `bsn_nl` | `\d{9}` (with check) | Netherlands | `123456789` |
| `ipv4` | IPv4 address | Universal | `192.168.1.1` |
| `ipv6` | IPv6 address | Universal | `2001:db8::1` |
| `passport` | Alphanumeric 6-9 | Universal | `AB1234567` |
| `iban` | ISO 13616 | EU | `DE89370400440532013000` |

## Configuration

### Simple: Scan all fields for known patterns
```toml
[[routing.rules.transformers]]
type = "redact"
patterns = ["email", "phone", "ssn_us", "credit_card"]
strategy = "hash"      # "hash" or "redact"
hash_key = "workspace-secret-key"  # Required for "hash" strategy
```

### Targeted: Specific fields only (faster)
```toml
[[routing.rules.transformers]]
type = "redact"
strategy = "hash"
hash_key = "workspace-secret-key"

[[routing.rules.transformers.fields]]
path = "user.email"
pattern = "email"

[[routing.rules.transformers.fields]]
path = "metadata.ip"
pattern = "ipv4"
strategy = "redact"  # Override: full redact for IP
```

### Custom patterns
```toml
[[routing.rules.transformers]]
type = "redact"
strategy = "redact"

[[routing.rules.transformers.custom_patterns]]
name = "employee_id"
regex = "EMP-\\d{6}"
prefix = "emp_"  # For hash strategy
```

## Hash Output Format

```
<prefix>_<base62(hmac_sha256(value, key)[:16])>
```

| Pattern | Prefix | Example Output |
|---------|--------|----------------|
| email | `usr_` | `usr_7Hx9KmPqR2sT4v` |
| phone | `phn_` | `phn_3Wd8FgHj5kLm` |
| ssn_us | `ssn_` | `ssn_9Qr2StUv6wXy` |
| credit_card | `cc_` | `cc_1Az3BcDe5fGh` |
| ipv4 | `ip4_` | `ip4_4Jk6LmNp8qRs` |
| custom | configurable | `emp_2Tv4UwXy7zAb` |

## Performance

| Mode | Overhead | Use When |
|------|----------|----------|
| Targeted fields | ~1μs/event | You know which fields contain PII |
| Full scan | ~50-100μs/event | Unknown structure, must scan all |

**Rule:** Use targeted fields when possible. Full scan is a safety net.

## Not This Job

| Need | Use Instead |
|------|-------------|
| Drop entire events | `filter` transformer |
| Aggregate events | `reduce` transformer |
| Complex field transforms | `remap` transformer (future) |

## Compliance Notes

- **GDPR**: Hash = pseudonymization (still personal data, but safer)
- **HIPAA**: Full redact recommended for PHI
- **PCI-DSS**: Full redact for credit cards, or tokenization
- **Key management**: Hash key should rotate; old hashes become non-correlatable
