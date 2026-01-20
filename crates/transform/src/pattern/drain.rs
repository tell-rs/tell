//! Drain Algorithm - Log pattern extraction
//!
//! Implements the Drain algorithm for extracting log patterns from unstructured
//! log messages. Drain uses a fixed-depth tree structure to cluster similar
//! log messages and extract common patterns.
//!
//! # Algorithm Overview
//!
//! 1. Parse log message into tokens
//! 2. Navigate tree by: length → first token → last token
//! 3. Find best matching pattern cluster or create new one
//! 4. Return pattern template with variables as `<*>`
//!
//! # Reference
//!
//! "Drain: An Online Log Parsing Approach with Fixed Depth Tree"
//! by Pinjia He et al.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
#[path = "drain_test.rs"]
mod tests;

/// Pattern ID type - unique identifier for each discovered pattern
pub type PatternId = u64;

/// A discovered log pattern template
#[derive(Debug, Clone)]
pub struct Pattern {
    /// Unique pattern identifier
    pub id: PatternId,

    /// Pattern template with variables as `<*>`
    pub template: String,

    /// Human-readable name (first 5 non-wildcard tokens)
    pub canonical_name: String,

    /// Token sequence with None for variables
    pub tokens: Vec<Option<String>>,

    /// Number of messages matched to this pattern
    pub count: u64,

    /// Unix timestamp when pattern was first seen (seconds since epoch)
    pub first_seen: u64,

    /// Unix timestamp when pattern was last seen (seconds since epoch)
    pub last_seen: u64,
}

impl Pattern {
    /// Create a new pattern from tokens
    fn new(id: PatternId, tokens: Vec<Option<String>>) -> Self {
        let template = tokens
            .iter()
            .map(|t| t.as_deref().unwrap_or("<*>"))
            .collect::<Vec<_>>()
            .join(" ");

        let canonical_name = generate_canonical_name(&tokens);
        let now = current_timestamp();

        Self {
            id,
            template,
            canonical_name,
            tokens,
            count: 1,
            first_seen: now,
            last_seen: now,
        }
    }
}

/// Get current Unix timestamp in seconds
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Generate a human-readable canonical name from pattern tokens
///
/// Takes the first 5 non-wildcard tokens and joins them with spaces.
/// Returns "Unknown Pattern" if all tokens are wildcards.
pub fn generate_canonical_name(tokens: &[Option<String>]) -> String {
    let name_tokens: Vec<&str> = tokens.iter().filter_map(|t| t.as_deref()).take(5).collect();

    if name_tokens.is_empty() {
        "Unknown Pattern".to_string()
    } else {
        name_tokens.join(" ")
    }
}

/// Drain tree for pattern extraction
///
/// Thread-safe implementation using interior mutability.
pub struct DrainTree {
    /// Root of the tree (length → first token → clusters)
    root: RwLock<DrainNode>,

    /// Similarity threshold for pattern matching (0.0 - 1.0)
    similarity_threshold: f64,

    /// Maximum children per node (limits memory)
    max_children: usize,

    /// Next pattern ID counter
    next_id: AtomicU64,

    /// All discovered patterns (id → pattern)
    patterns: RwLock<HashMap<PatternId, Pattern>>,
}

/// Node in the Drain tree
#[derive(Debug, Default)]
struct DrainNode {
    /// Child nodes (token → node)
    children: HashMap<String, DrainNode>,

    /// Pattern clusters at this node
    clusters: Vec<PatternCluster>,
}

/// A cluster of log patterns
#[derive(Debug, Clone)]
struct PatternCluster {
    /// Pattern ID
    id: PatternId,

    /// Token template (None = variable)
    tokens: Vec<Option<String>>,
}

impl DrainTree {
    /// Create a new Drain tree
    pub fn new(similarity_threshold: f64, max_children: usize) -> Self {
        Self {
            root: RwLock::new(DrainNode::default()),
            similarity_threshold: similarity_threshold.clamp(0.0, 1.0),
            max_children: max_children.max(1),
            next_id: AtomicU64::new(1),
            patterns: RwLock::new(HashMap::new()),
        }
    }

    /// Parse a log message and return its pattern ID
    ///
    /// If the message matches an existing pattern, returns that pattern's ID.
    /// If no match, creates a new pattern and returns its ID.
    pub fn parse(&self, message: &str) -> PatternId {
        let tokens = self.tokenize(message);

        if tokens.is_empty() {
            return 0; // Empty message
        }

        // Try to find existing pattern
        if let Some(id) = self.find_pattern(&tokens) {
            return id;
        }

        // Create new pattern
        self.create_pattern(tokens)
    }

    /// Get a pattern by ID
    pub fn get_pattern(&self, id: PatternId) -> Option<Pattern> {
        self.patterns.read().get(&id).cloned()
    }

    /// Get all discovered patterns
    pub fn all_patterns(&self) -> Vec<Pattern> {
        self.patterns.read().values().cloned().collect()
    }

    /// Get pattern count
    pub fn pattern_count(&self) -> usize {
        self.patterns.read().len()
    }

    /// Tokenize a log message
    fn tokenize(&self, message: &str) -> Vec<String> {
        message.split_whitespace().map(|s| s.to_string()).collect()
    }

    /// Check if a token is likely a variable
    fn is_variable(&self, token: &str) -> bool {
        // Numbers (including decimals, negatives)
        if token.parse::<f64>().is_ok() {
            return true;
        }

        // Hex numbers (0x prefix or all hex chars)
        if token.starts_with("0x")
            || (token.len() > 8 && token.chars().all(|c| c.is_ascii_hexdigit()))
        {
            return true;
        }

        // IP addresses (simple check)
        if token.matches('.').count() == 3 && token.split('.').all(|p| p.parse::<u8>().is_ok()) {
            return true;
        }

        // UUIDs
        if token.len() == 36
            && token.matches('-').count() == 4
            && token
                .replace('-', "")
                .chars()
                .all(|c| c.is_ascii_hexdigit())
        {
            return true;
        }

        // Paths (contain / or \)
        if token.contains('/') || token.contains('\\') {
            return true;
        }

        // URLs
        if token.starts_with("http://") || token.starts_with("https://") {
            return true;
        }

        // Email-like (contains @)
        if token.contains('@') && token.contains('.') {
            return true;
        }

        // Timestamps (common formats)
        if token.contains(':') && token.len() >= 5 {
            let parts: Vec<_> = token.split(':').collect();
            if parts.len() >= 2 && parts.iter().all(|p| p.parse::<u32>().is_ok()) {
                return true;
            }
        }

        false
    }

    /// Find an existing pattern that matches the tokens
    fn find_pattern(&self, tokens: &[String]) -> Option<PatternId> {
        let root = self.root.read();
        let len_key = tokens.len().to_string();

        // Navigate: length → first token → last token
        let len_node = root.children.get(&len_key)?;

        let first_token = if self.is_variable(&tokens[0]) {
            "<*>"
        } else {
            &tokens[0]
        };

        let first_node = len_node.children.get(first_token)?;

        // Search clusters for best match
        for cluster in &first_node.clusters {
            if self.matches_cluster(tokens, cluster) {
                // Update count
                self.increment_pattern_count(cluster.id);
                return Some(cluster.id);
            }
        }

        None
    }

    /// Check if tokens match a cluster's pattern
    fn matches_cluster(&self, tokens: &[String], cluster: &PatternCluster) -> bool {
        if tokens.len() != cluster.tokens.len() {
            return false;
        }

        let mut matches = 0;
        let total = tokens.len();

        for (token, pattern_token) in tokens.iter().zip(cluster.tokens.iter()) {
            match pattern_token {
                Some(pt) if pt == token => matches += 1,
                Some(_) if self.is_variable(token) => matches += 1,
                None => matches += 1, // Variable in pattern
                _ => {}
            }
        }

        let similarity = matches as f64 / total as f64;
        similarity >= self.similarity_threshold
    }

    /// Increment pattern match count and update last_seen
    fn increment_pattern_count(&self, id: PatternId) {
        if let Some(pattern) = self.patterns.write().get_mut(&id) {
            pattern.count += 1;
            pattern.last_seen = current_timestamp();
        }
    }

    /// Create a new pattern from tokens
    fn create_pattern(&self, tokens: Vec<String>) -> PatternId {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        // Convert tokens to pattern (marking variables)
        let pattern_tokens: Vec<Option<String>> = tokens
            .iter()
            .map(|t| {
                if self.is_variable(t) {
                    None
                } else {
                    Some(t.clone())
                }
            })
            .collect();

        // Create pattern
        let pattern = Pattern::new(id, pattern_tokens.clone());

        // Store pattern
        self.patterns.write().insert(id, pattern);

        // Add to tree
        self.add_to_tree(&tokens, id, pattern_tokens);

        id
    }

    /// Add a pattern to the tree
    fn add_to_tree(&self, tokens: &[String], id: PatternId, pattern_tokens: Vec<Option<String>>) {
        let mut root = self.root.write();
        let len_key = tokens.len().to_string();

        // Get or create length node
        let len_node = root.children.entry(len_key).or_default();

        // Limit children
        if len_node.children.len() >= self.max_children {
            return; // Drop pattern if tree is full
        }

        let first_token = if self.is_variable(&tokens[0]) {
            "<*>".to_string()
        } else {
            tokens[0].clone()
        };

        // Get or create first token node
        let first_node = len_node.children.entry(first_token).or_default();

        // Add cluster
        first_node.clusters.push(PatternCluster {
            id,
            tokens: pattern_tokens,
        });
    }

    /// Clear all patterns (for testing)
    #[cfg(test)]
    pub fn clear(&self) {
        *self.root.write() = DrainNode::default();
        self.patterns.write().clear();
        self.next_id.store(1, Ordering::SeqCst);
    }
}

impl Default for DrainTree {
    fn default() -> Self {
        Self::new(0.5, 100)
    }
}
