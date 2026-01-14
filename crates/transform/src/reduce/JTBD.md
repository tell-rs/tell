# Reduce Transformer

> **Job:** Consolidate redundant events at the edge to reduce storage costs.

## When To Use

| Scenario | Before | After | Savings | Verdict |
|----------|--------|-------|---------|---------|
| Error storm | 100,000 "timeout" errors/sec | 1 event with `_count: 100000` | 99.999% | Use reduce |
| Rapid clicks | 50 clicks in 1 second | 1 event with `_count: 50` | 98% | Use reduce |
| Health checks | 60 pings/minute | 1 summary/minute | 98% | Use reduce |
| Normal traffic | 10,000 unique events | 10,000 events | 0% | Skip - zero-copy |

**Rule:** If not seeing >90% reduction, keep zero-copy.

## Not This Job

| Need | Use Instead |
|------|-------------|
| Multi-line logs | Source-level framing |
| Exact deduplication | `dedupe` transformer |
| Statistical sampling | `sample` transformer |
| Field transformation | `remap` transformer |
