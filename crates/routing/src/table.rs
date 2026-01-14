//! Routing table for O(1) source→sinks lookup
//!
//! The routing table is compiled once at startup from configuration.
//! All allocations happen during compilation - the hot path is zero-copy.

use std::collections::HashMap;

use cdp_protocol::SourceId;

use crate::SinkId;

/// Pre-compiled routing table for O(1) lookups
///
/// The table maps source IDs to lists of sink IDs. All data is pre-allocated
/// during compilation, so the `route()` method performs no allocations.
///
/// # Zero-Copy Design
///
/// - Routes stored as `Vec<SinkId>` - allocated once at compile time
/// - `route()` returns `&[SinkId]` - slice reference, no copy
/// - `SinkId` is `Copy` - consumers can copy individual IDs cheaply
///
/// # Example
///
/// ```
/// use cdp_routing::{RoutingTable, SinkId, SourceId};
///
/// let mut table = RoutingTable::new();
///
/// // Set default sinks (used when source has no explicit route)
/// table.set_default(vec![SinkId::new(0)]);
///
/// // Add explicit routes
/// table.add_route(SourceId::new("tcp"), vec![SinkId::new(1), SinkId::new(2)]);
/// table.add_route(SourceId::new("syslog"), vec![SinkId::new(2)]);
///
/// // O(1) lookup in hot path
/// let sinks = table.route(&SourceId::new("tcp"));
/// assert_eq!(sinks, &[SinkId::new(1), SinkId::new(2)]);
///
/// // Unknown source gets default
/// let default = table.route(&SourceId::new("unknown"));
/// assert_eq!(default, &[SinkId::new(0)]);
/// ```
#[derive(Debug, Clone)]
pub struct RoutingTable {
    /// Pre-compiled routes: source → sinks
    routes: HashMap<SourceId, Vec<SinkId>>,

    /// Default sinks for sources without explicit routes
    default_sinks: Vec<SinkId>,

    /// Sink names for debugging/metrics (indexed by SinkId)
    sink_names: Vec<String>,
}

impl Default for RoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

impl RoutingTable {
    /// Create a new empty routing table
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
            default_sinks: Vec::new(),
            sink_names: Vec::new(),
        }
    }

    /// Create a routing table with pre-allocated capacity
    ///
    /// Use this when you know the number of routes ahead of time
    /// to avoid reallocations during compilation.
    #[inline]
    #[must_use]
    pub fn with_capacity(routes: usize, sinks: usize) -> Self {
        Self {
            routes: HashMap::with_capacity(routes),
            default_sinks: Vec::new(),
            sink_names: Vec::with_capacity(sinks),
        }
    }

    /// Set the default sinks for sources without explicit routes
    ///
    /// These sinks receive batches from any source that doesn't have
    /// a specific routing rule.
    #[inline]
    pub fn set_default(&mut self, sinks: Vec<SinkId>) {
        self.default_sinks = sinks;
    }

    /// Add a route from a source to multiple sinks
    ///
    /// If a route already exists for this source, it is replaced.
    #[inline]
    pub fn add_route(&mut self, source: SourceId, sinks: Vec<SinkId>) {
        self.routes.insert(source, sinks);
    }

    /// Register a sink name (for debugging/metrics)
    ///
    /// Returns the assigned SinkId. Sinks should be registered in order.
    #[inline]
    pub fn register_sink(&mut self, name: impl Into<String>) -> SinkId {
        let id = SinkId::new(self.sink_names.len() as u16);
        self.sink_names.push(name.into());
        id
    }

    /// Route a batch to its destination sinks
    ///
    /// This is the hot path - O(1) HashMap lookup, returns a slice reference.
    ///
    /// # Zero-Copy
    ///
    /// Returns `&[SinkId]` - a slice into pre-allocated storage.
    /// No allocations, no copies. Callers can iterate or copy individual
    /// `SinkId` values (which are `Copy` and only 2 bytes).
    ///
    /// # Fallback
    ///
    /// If the source has no explicit route, returns the default sinks.
    /// If no defaults are configured, returns an empty slice.
    #[inline]
    pub fn route(&self, source: &SourceId) -> &[SinkId] {
        self.routes
            .get(source)
            .map(Vec::as_slice)
            .unwrap_or(&self.default_sinks)
    }

    /// Check if a source has an explicit route
    #[inline]
    pub fn has_route(&self, source: &SourceId) -> bool {
        self.routes.contains_key(source)
    }

    /// Get the default sinks
    #[inline]
    pub fn default_sinks(&self) -> &[SinkId] {
        &self.default_sinks
    }

    /// Get the name of a sink by ID (for debugging/metrics)
    #[inline]
    pub fn sink_name(&self, id: SinkId) -> Option<&str> {
        self.sink_names.get(id.as_usize()).map(String::as_str)
    }

    /// Get the number of explicit routes
    #[inline]
    pub fn route_count(&self) -> usize {
        self.routes.len()
    }

    /// Get the number of registered sinks
    #[inline]
    pub fn sink_count(&self) -> usize {
        self.sink_names.len()
    }

    /// Check if the routing table is empty (no routes, no defaults)
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty() && self.default_sinks.is_empty()
    }

    /// Iterate over all explicit routes
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&SourceId, &[SinkId])> {
        self.routes.iter().map(|(k, v)| (k, v.as_slice()))
    }

    /// Get all registered sink names
    #[inline]
    pub fn sink_names(&self) -> &[String] {
        &self.sink_names
    }
}

/// Builder for constructing routing tables from configuration
///
/// Provides a convenient way to build a routing table with validation.
#[derive(Debug, Default)]
pub struct RoutingTableBuilder {
    /// Registered sinks: name → id
    sink_ids: HashMap<String, SinkId>,

    /// Sink names in order
    sink_names: Vec<String>,

    /// Routes being built
    routes: Vec<(SourceId, Vec<SinkId>)>,

    /// Default sink names
    default_sink_names: Vec<String>,
}

impl RoutingTableBuilder {
    /// Create a new builder
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a sink and get its ID
    ///
    /// If the sink is already registered, returns the existing ID.
    pub fn register_sink(&mut self, name: impl Into<String>) -> SinkId {
        let name = name.into();
        if let Some(&id) = self.sink_ids.get(&name) {
            return id;
        }

        let id = SinkId::new(self.sink_names.len() as u16);
        self.sink_ids.insert(name.clone(), id);
        self.sink_names.push(name);
        id
    }

    /// Get the ID of a registered sink
    #[inline]
    pub fn get_sink_id(&self, name: &str) -> Option<SinkId> {
        self.sink_ids.get(name).copied()
    }

    /// Add a route from source to sinks (by name)
    ///
    /// Sinks must be registered first.
    ///
    /// # Errors
    ///
    /// Returns `None` if any sink name is not registered.
    pub fn add_route_by_name(
        &mut self,
        source: impl Into<SourceId>,
        sink_names: &[impl AsRef<str>],
    ) -> Option<()> {
        let sink_ids: Vec<SinkId> = sink_names
            .iter()
            .map(|name| self.sink_ids.get(name.as_ref()).copied())
            .collect::<Option<Vec<_>>>()?;

        self.routes.push((source.into(), sink_ids));
        Some(())
    }

    /// Set default sinks (by name)
    ///
    /// Sinks must be registered first.
    pub fn set_default_by_name(&mut self, sink_names: Vec<String>) {
        self.default_sink_names = sink_names;
    }

    /// Build the routing table
    ///
    /// # Errors
    ///
    /// Returns `None` if any default sink name is not registered.
    pub fn build(self) -> Option<RoutingTable> {
        let default_sinks: Vec<SinkId> = self
            .default_sink_names
            .iter()
            .map(|name| self.sink_ids.get(name).copied())
            .collect::<Option<Vec<_>>>()?;

        let mut table = RoutingTable::with_capacity(self.routes.len(), self.sink_names.len());
        table.sink_names = self.sink_names;
        table.default_sinks = default_sinks;

        for (source, sinks) in self.routes {
            table.routes.insert(source, sinks);
        }

        Some(table)
    }
}
