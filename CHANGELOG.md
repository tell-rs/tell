# v2.0 RUST REWRITE --- IN-PROGRES

Rust rewrite 

New features
 - tail in live stream with filters?
 - new multiplex routing?
 - cleaner/simpler config, and better defuaults
 - create new unique apikeys on start (if doesnt exist)
 - performance is faster and slower than the golang depending on amount of concurrent connection, the more the lesser benefits on max througput.
 - transformers (experimental) filter, reduce, redact, 
 - new source connectors (experimental) github, shopify
 - clickhouse arrow as new primary clickhouse sink, better performance
 - sink disk_plaintext now writes protocols to separate .log files (eg. events, logs, snapshots)
 - arrow sink

--- 

# v1.8.0 (December 1, 2025)
EXPERIMENTAL: Metrics & Distributed Tracing Protocols

- feat: **Experimental** - New `metric.fbs` schema for performance metrics (gauges, counters, histograms)
- feat: **Experimental** - New `trace.fbs` schema for distributed tracing with span tree reconstruction
- feat: Metrics support for GAUGE (point-in-time), COUNTER (cumulative/delta), and HISTOGRAM (with min/max/buckets)
- feat: Metrics temporality support - CUMULATIVE (Prometheus-style) and DELTA (StatsD-style)
- feat: Histogram support with count, sum, min, max, and explicit bucket boundaries
- feat: Typed metric labels - `Label` (string) and `IntLabel` (int64) for efficient range queries on numeric dimensions (status_code, port)
- feat: Traces support span tree reconstruction via trace_id, span_id, parent_span_id relationships
- feat: Span kinds: INTERNAL, SERVER, CLIENT, PRODUCER, CONSUMER for service topology mapping
- feat: Client-side telemetry support - optional session_id on metrics for app/game performance correlation
- feat: Full session_id correlation across all protocols (events, logs, metrics, traces)
- feat: Nanosecond timestamp precision for metrics and traces (OTLP-compatible)
- feat: Added TRACE schema type to common.fbs routing enum
- feat: Added correlation model documentation to common.fbs header
- docs: Comprehensive usage examples in metric.fbs and trace.fbs
- note: Protocols follow OpenTelemetry semantics but use FlatBuffers wire format (not wire-compatible, converter possible)
- note: Collector handlers and sinks not yet implemented - schema only release

# v1.7.1 (November 9, 2025)
Dependency Updates

- chore: Updated Go version from 1.24.6 to 1.25.3

# v1.7.0 (November 1, 2025)
EXPERIMENTAL: New Transformer Pipeline & Pattern Matching

- feat: **Experimental** - New transformer pipeline architecture for in-flight data enrichment between sources and sinks
- feat: Pattern matching transformer using Drain algorithm - automatically extracts log patterns and assigns pattern_id for semantic search
- feat: Pattern-based querying - reduces millions of unique logs to hundreds of reusable patterns (e.g., "User <*> logged in from <*>")
- feat: Hot reload patterns from ClickHouse every 60 seconds with zero downtime - distributed collectors stay in sync
- feat: Zero-copy fast path when transformers disabled - maintains ~40M events/sec raw throughput
- feat: Configurable pattern matching with sensible defaults (just set `enabled: true`)
- perf: Single decode cache eliminates double-decode between transformer and sink
- perf: Message-to-pattern cache with 70-80% hit rate for fast pattern lookups
- perf: Comprehensive metrics - transformer duration, error rate, backpressure monitoring with critical alerts
- docs: New user-facing TRANSFORMERS.md guide and technical PATTERN_MATCHING.md deep-dive
- note: Vector embeddings for semantic similarity search planned but not yet implemented (stored with empty embedding field for future post-processing)
- refactor: Created pkg/batch package to eliminate circular dependencies
- refactor: Moved transformers from per-source to centralized pipeline (transform once, fan out to all sinks)
- breaking: Batch interface changes - added PatternIDs field for pattern storage

# v1.6.0 (October 14, 2025)
SOC Forwarding & Source IP Preservation

- feat: Ultra-fast SOC forwarding sink (much faster than traditional syslog forwarders)
- feat: Source IP preservation through collector forwarding chain via optional batch field
- feat: Multi-TCP source support for dual-port SOC deployments (public + forwarder ports)
- feat: TCP source forwarding_mode configuration for secure source IP handling
- perf: Near zero-copy forwarding - only batch header rebuilt (~100 bytes), event data untouched
- perf: Batch forwarding with 500 events per TCP write (vs per-event in syslog forwarders)
- security: forwarding_mode defaults to false (secure against IP spoofing attacks)

# v1.5.0 (September ??, 2025)
Sink Architecture Overhaul & New Storage Options

- feat: New disk_binary_batch sink with 24-byte enriched metadata headers and 40M events/sec performance
- feat: New parquet sink for analytics with columnar storage, schema separation (events.parquet/logs.parquet), and cross-platform compatibility
- feat: Optional LZ4 compression support for disk_binary_batch and disk_plaintext sinks
- feat: Complete binreader tool rewrite with binary FlatBuffer parsing, LZ4 auto-detection, time-range filtering, text search, and statistics
- feat: TCP debug source enhanced logging with structured output and complete binary dumps (removed 64-byte limit)
- breaking: Removed disk_binary and disk_lz4 sinks, replaced with unified disk_binary_batch with optional compression
- feat: Consistent metadata enrichment across all disk sinks with source IP and batch timestamps
- perf: Removed JSON marshaling from disk_plaintext sink, replaced with direct fmt.Fprintf() formatting
- perf: Zero-copy optimizations in parquet sink with direct FlatBuffer storage

# v1.4.4 (September 19, 2025)
- feat: TCP debug source now shows complete hex dump of messages instead of limiting to 64 bytes for full binary analysis

## v1.4.3 (September 5, 2025)
- fixed: Fixed email normalization in `generateUserIDFromEmail()` causing duplicate users from case/whitespace variations

## v1.4.0 (August 31, 2025)
3-Table User Identity Architecture Implementation

- feat: Implemented streaming 3-table user identity architecture with parallel INSERT operations to `users_v1`, `user_devices`, and `user_traits` tables
- perf: Eliminated SELECT-then-INSERT bottlenecks with pure streaming architecture - zero coordination between tables during IDENTIFY event processing
- feat: User traits stored as flexible key-value pairs solving partial update data loss, device relationships preserved for cross-device analytics
- feat: ClickHouse sink unit tests covering data extraction, concurrent processing, and edge cases

## v1.3.0 / v1.2.1 (August 31, 2025)
Syslog Performance Optimization & Testing Framework

- perf: Complete syslog parsing rewrite with dramatic performance improvements:
  - RFC 3164: 304.5 → 121.2 ns/op (60% faster)
  - RFC 5424: 273.3 → 122.5 ns/op (55% faster)
  - Structured Data: 718.7 → 122.4 ns/op (83% faster)
  - Large Messages: 10,583 → 117.2 ns/op (99% faster)
- perf: Eliminated interface{} boxing overhead - replaced `Metadata map[string]interface{}` with direct `WorkspaceID string` and `SourceIP net.IP` fields, updated all sinks/sources for direct field access eliminating type assertions and map allocations
- perf: Memory optimization - Large messages reduced from 22,032 B/op to 420 B/op, consistent 5 allocs/op across all message types
- feat: syslog parsing with zero-copy payload preservation, fast-path hostname extraction, and optimized structured data handling
- feat: Comprehensive syslog testing framework with unit/integration tests, benchmarks, and complete coverage for RFC 3164/5424, UTF-8, BOM, structured data, and edge cases

## v1.2.0 (August 21, 2025)
New experimental TCP and UDP Syslog Source Collection Support

- Syslog Sources: Added high-performance TCP and UDP syslog ingestion with auto-format detection (RFC 3164/5424)
- Sinks: Added syslog decoder support for RFC 3164/5424 format rendering in all output sinks
- Configuration: New syslog-specific configuration profiles with non-privileged port options for development
- Testing: Syslog test clients for TCP/UDP with mixed format demonstrations
- Pipeline: Multi-protocol support expanded to include syslog alongside analytics events and logs in the pipeline

## August 21, 2025
Source IP Collection & Storage

- TCP Sources: Extract `tcpAddr.IP` once per connection, pass through pipeline
- ClickHouse Sink: Store `source_ip` in all tables with IPv4-mapped IPv6 format
- Display Sinks: Show `real_source_ip` in JSON output to distinguish from client `source`

## v1.1.5 (August 17, 2025)
Tell Schema & ClickHouse Sink Complete Rewrite

- feat: **MAJOR** - Complete ClickHouse sink rewrite for Tell v1.1 schema
- feat: Event type routing - TRACK→events_v1, IDENTIFY→users_v1, CONTEXT→context_v1, LOGS→logs_v1
- feat: Zero-copy processing with concurrent per-table batching for optimal performance
- feat: Single-customer deployment optimization with simplified architecture
- breaking: Log schema migration - `context_id` (uint64) → `session_id` ([16]byte UUID) for consistency
- feat: Updated all sinks (ClickHouse, plaintext, stdout, binreader) for new session_id field
- feat: Session correlation - events and logs now both use session_id UUID for perfect correlation
- perf: Non-blocking flush coordination with async batch processing

## v1.1.4 (August 14, 2025)
- refactor: Reduced atomic file rotation logging from 4 lines to 2 lines

## v1.1.3 (August 12, 2025)
Event Schema & Field Access Optimization

- feat: Added event_name field to Event FlatBuffers schema (id: 4) for direct field access
- feat: Updated all sinks to support event_name and session_id fields in output
- refactor: Renamed user_id to device_id throughout collector for schema consistency
- refactor: Improved field display to show "nil" instead of "0 bytes" for empty optional fields
- refactor: Updated ClickHouse sink to use direct event_name field access for better performance
- perf: Optimized SafeGetRootAsBatch with 5-stage fast-fail validation for garbage data protection
- feat: New crash tests
- **BREAKING**: Field names updated from user_id to device_id, requires SDK coordination

## v1.1.1 / v1.1 (August 11, 2025)
Protocol Versioning & Field Ordering Optimization

- feat: Added protocol version field (uint8) to Batch schema for backward compatibility and evolution support
- feat: Added CONTEXT event type (6) for session/device context updates routing to context table to keep enrich type separated
- refactor: Optimized Batch field ordering for processing pipeline (api_key → version → schema_type → batch_id → data)
- refactor: Optimized Event field ordering with event_type first for fast routing decisions
- refactor: Renamed user_id to device_id in Event schema for accurate field naming
- refactor: Enhanced event type routing documentation (TRACK→events, IDENTIFY→users, CONTEXT→context)
- docs: Added comprehensive schema documentation explaining processing pipeline field ordering rationale
- **BREAKING**: Protocol structure updated, requires SDK regeneration and collector handler updates

## v1.0.10 (August 9, 2025)

FlatBuffer Serialization & Debug Improvements
- feat: Added comprehensive TCP debug source with binary message analysis and field-by-field deserialization
- fix: Corrected LogEventType enum values (COLLECT→LOG) and optimized from uint32 to uint8
- feat: Enhanced FlatBuffer validation with SafeGetRootAsBatch error handling
- feat: Added detailed binary dump logging for SDK debugging and troubleshooting
- feat: Added TCP debug source configuration with production safety warnings
- refactor: Improved schema routing and message parsing reliability
- docs: Updated ClickHouse setup documentation

## v1.0.9 (August 1, 2025)

ClickHouse & Storage Improvements (v1.0.9)
- fix: Properly convert [16]byte to UUID for ClickHouse insertion
- fix: Remove debug logging from production ClickHouse sink
- fix: Finalize UUID binary optimization and disk_binary improvements
- fix: Improved binary disk writer and fixed UUID in ClickHouse sink
- feat: New configs for ClickHouse setup

## v1.0.2-v1.0.4 (July 30, 2025)

Storage & Reliability Improvements (v1.0.2-v1.0.4)
- fix: Atomic disk file-rotations for data integrity
- feat: New binary disk storage sink implementation
- fix: User ID binary decoding in plaintext sink
- fix: Event type display formatting for stdout sink
- fix: Resolved stdout sink user ID display issues
- fix: Collector crash prevention and TCP source debug improvements

## v1.0.1 (July 25, 2025)

Domain-Specific Logging & Architecture (v1.0.1)
- feat: Domain-specific logging implementation with structured output
- refactor: Single binary collector architecture (moved to tell repo)
- feat: Configurable sink metrics with profile-based defaults
- chore: Configuration profile renaming for clarity
- refactor: All sinks now use structured logging consistently

## July 21, 2025

Code Quality & Metrics Improvements
- feat: Improved metrics log message consistency and clarity
- feat: Enhanced sink initialization logging with domain context

## July 16, 2025

Per-Sink Metrics Implementation
- feat: Added configurable per-sink metrics with profile-based defaults and individual sink control

## July 15, 2025

API Key Security & Logging Improvements
- feat: Added file-based API key management with hot reload and zero performance impact
- feat: Replaced captain logging with direct zerolog implementation
- feat: Removed Axiom dependency and external logging integrations
- feat: Maintained structured logging format with improved performance

## July 14, 2025

Metrics & Configuration Improvements
- feat: Added deployment profiles (Cloud, OT, Self-hosted) with configurable metrics
- feat: Reorganized configuration files into clean profile-based structure

## Earlier Versions

Schema & Protocol Changes
- **BREAKING**: Unified TCP sources into single endpoint with schema-type routing
- feat: Added dual-database ClickHouse support (events→`tell`, logs→`log`)


## v0.1.x-beta (May 24, 2024)
- feat: Added new log schema with context IDs, event routing, and structured payloads, support for log severity levels, correlation tracking, and batch sequence IDs
- feat: Implemented binary FlatBuffers format for high-performance zero-copy log processing
- feat: Introduced dedicated TCP ports for events (50000) and logs (50001)
- feat: Added specialized event and log batch processing in pipeline
- refactor: Reorganized schema files into pkg/schema directory
- refactor: Separated TCP source implementations for optimized event and log handling to reduce context switching (ops)
- refactor: Updated pipeline to handle multiple data types efficiently
- docs: Added detailed log protocol documentation

## May 23, 2025
- feat: Added new `tell` command-line utility
- refactor: Updated module path to support remote installation via Go tools
