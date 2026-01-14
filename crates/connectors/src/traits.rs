//! Connector trait definition

use crate::error::ConnectorError;
use cdp_protocol::Batch;

/// Trait for pull-based connectors that fetch data from external sources
///
/// Connectors pull snapshots of data from external services (GitHub, Stripe, etc.)
/// and convert them into `Batch` instances that flow through the pipeline.
pub trait Connector: Send + Sync {
    /// Returns the connector name (e.g., "github", "stripe")
    fn name(&self) -> &'static str;

    /// Pull snapshot data for a specific entity
    ///
    /// # Arguments
    /// * `entity` - The entity identifier (e.g., "owner/repo" for GitHub)
    ///
    /// # Returns
    /// A `Batch` containing the snapshot data ready for the pipeline
    fn pull(
        &self,
        entity: &str,
    ) -> impl std::future::Future<Output = Result<Batch, ConnectorError>> + Send;
}
