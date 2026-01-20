//! Connector scheduler with cron support
//!
//! Manages scheduled execution of connectors based on cron expressions.
//! Each connector runs in its own tokio task for isolation.

use crate::error::ConnectorError;
use crate::traits::Connector;
use chrono::{DateTime, Utc};
use cron::Schedule;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tell_protocol::Batch;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

#[cfg(feature = "github")]
use crate::config::GitHubConnectorConfig;
#[cfg(feature = "github")]
use crate::github::GitHub;

#[cfg(feature = "shopify")]
use crate::config::ShopifyConnectorConfig;
#[cfg(feature = "shopify")]
use crate::shopify::Shopify;

/// Default overall timeout for a connector pull operation (all entities)
const DEFAULT_OVERALL_TIMEOUT_SECS: u64 = 300; // 5 minutes

/// Connector type enum for runtime polymorphism without dyn
enum ConnectorImpl {
    #[cfg(feature = "github")]
    GitHub(GitHub),
    #[cfg(feature = "shopify")]
    Shopify(Shopify),
    // Placeholder to prevent empty enum when no features enabled
    #[cfg(not(any(feature = "github", feature = "shopify")))]
    _None,
}

impl ConnectorImpl {
    async fn pull(&self, entity: &str) -> Result<Batch, ConnectorError> {
        match self {
            #[cfg(feature = "github")]
            ConnectorImpl::GitHub(g) => g.pull(entity).await,
            #[cfg(feature = "shopify")]
            ConnectorImpl::Shopify(s) => s.pull(entity).await,
            #[cfg(not(any(feature = "github", feature = "shopify")))]
            ConnectorImpl::_None => unreachable!(),
        }
    }
}

/// A scheduled connector instance
pub struct ScheduledConnector {
    /// Unique name for this connector instance
    pub name: String,
    /// The connector implementation
    connector: Arc<ConnectorImpl>,
    /// Entities to pull
    entities: Vec<String>,
    /// Cron schedule (None = manual only)
    schedule: Option<Schedule>,
    /// Next scheduled run time
    next_run: Option<DateTime<Utc>>,
    /// Whether this connector is currently running
    running: Arc<AtomicBool>,
    /// Overall timeout for pulling all entities
    overall_timeout: Duration,
}

impl ScheduledConnector {
    /// Create a new scheduled GitHub connector
    #[cfg(feature = "github")]
    pub fn github(
        name: String,
        config: &GitHubConnectorConfig,
        cron_expr: Option<&str>,
    ) -> Result<Self, ConnectorError> {
        let schedule = cron_expr
            .map(|expr| {
                Schedule::from_str(expr)
                    .map_err(|e| ConnectorError::InvalidSchedule(format!("{}: {}", expr, e)))
            })
            .transpose()?;

        let connector = GitHub::from_config(config)?;
        let next_run = schedule.as_ref().and_then(|s| s.upcoming(Utc).next());

        Ok(Self {
            name,
            connector: Arc::new(ConnectorImpl::GitHub(connector)),
            entities: config.entities.clone(),
            schedule,
            next_run,
            running: Arc::new(AtomicBool::new(false)),
            overall_timeout: Duration::from_secs(DEFAULT_OVERALL_TIMEOUT_SECS),
        })
    }

    /// Create a new scheduled Shopify connector
    #[cfg(feature = "shopify")]
    pub fn shopify(
        name: String,
        config: &ShopifyConnectorConfig,
        cron_expr: Option<&str>,
    ) -> Result<Self, ConnectorError> {
        let schedule = cron_expr
            .map(|expr| {
                Schedule::from_str(expr)
                    .map_err(|e| ConnectorError::InvalidSchedule(format!("{}: {}", expr, e)))
            })
            .transpose()?;

        let connector = Shopify::from_config(config)?;
        let next_run = schedule.as_ref().and_then(|s| s.upcoming(Utc).next());

        Ok(Self {
            name,
            connector: Arc::new(ConnectorImpl::Shopify(connector)),
            entities: config.entities.clone(),
            schedule,
            next_run,
            running: Arc::new(AtomicBool::new(false)),
            overall_timeout: Duration::from_secs(DEFAULT_OVERALL_TIMEOUT_SECS),
        })
    }

    /// Get the next scheduled run time
    pub fn next_run(&self) -> Option<DateTime<Utc>> {
        self.next_run
    }

    /// Update next run time after execution
    fn advance_schedule(&mut self) {
        self.next_run = self.schedule.as_ref().and_then(|s| s.upcoming(Utc).next());
    }

    /// Check if this connector should run now
    fn should_run(&self, now: DateTime<Utc>) -> bool {
        match self.next_run {
            Some(next) => now >= next,
            None => false,
        }
    }

    /// Check if this connector is currently running
    fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Try to start running (returns false if already running)
    fn try_start(&self) -> bool {
        self.running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }

    /// Pull all entities and return batches
    pub async fn pull_all(&self) -> Vec<Result<Batch, ConnectorError>> {
        let mut results = Vec::with_capacity(self.entities.len());
        for entity in &self.entities {
            results.push(self.connector.pull(entity).await);
        }
        results
    }
}

/// Scheduler that manages multiple connectors
pub struct ConnectorScheduler {
    connectors: Vec<ScheduledConnector>,
    /// Channel to send batches to the pipeline
    batch_sender: mpsc::Sender<Arc<Batch>>,
    /// Check interval for scheduled runs
    check_interval: Duration,
}

impl ConnectorScheduler {
    /// Create a new scheduler
    pub fn new(batch_sender: mpsc::Sender<Arc<Batch>>) -> Self {
        Self {
            connectors: Vec::new(),
            batch_sender,
            check_interval: Duration::from_secs(60),
        }
    }

    /// Set the check interval for scheduled runs
    pub fn with_check_interval(mut self, interval: Duration) -> Self {
        self.check_interval = interval;
        self
    }

    /// Add a scheduled connector
    pub fn add(&mut self, connector: ScheduledConnector) {
        info!(
            connector = %connector.name,
            entities = ?connector.entities,
            next_run = ?connector.next_run,
            "registered connector"
        );
        self.connectors.push(connector);
    }

    /// Run the scheduler loop
    ///
    /// Connectors execute in isolated tasks - a slow connector doesn't block others.
    pub async fn run(mut self) -> Result<(), ConnectorError> {
        info!(
            connectors = self.connectors.len(),
            check_interval = ?self.check_interval,
            "starting connector scheduler"
        );

        loop {
            let now = Utc::now();

            // Check each connector and spawn tasks for due ones
            for connector in &mut self.connectors {
                if connector.should_run(now) {
                    // Skip if already running (prevents overlap)
                    if connector.is_running() {
                        warn!(
                            connector = %connector.name,
                            "skipping scheduled run - previous execution still in progress"
                        );
                        continue;
                    }

                    // Try to acquire the run lock
                    if !connector.try_start() {
                        continue;
                    }

                    info!(connector = %connector.name, "spawning scheduled pull task");

                    // Clone what we need for the spawned task
                    let name = connector.name.clone();
                    let connector_impl = Arc::clone(&connector.connector);
                    let entities = connector.entities.clone();
                    let batch_sender = self.batch_sender.clone();
                    let running_flag = Arc::clone(&connector.running);
                    let overall_timeout = connector.overall_timeout;

                    // Spawn isolated task for this connector
                    tokio::spawn(async move {
                        let result = tokio::time::timeout(
                            overall_timeout,
                            execute_connector_pull(
                                &name,
                                &connector_impl,
                                &entities,
                                &batch_sender,
                            ),
                        )
                        .await;

                        match result {
                            Ok((success, failed)) => {
                                debug!(
                                    connector = %name,
                                    success,
                                    failed,
                                    "pull complete"
                                );
                            }
                            Err(_) => {
                                error!(
                                    connector = %name,
                                    timeout_secs = overall_timeout.as_secs(),
                                    "connector timed out"
                                );
                            }
                        }

                        // Mark as done so next scheduled run can proceed
                        running_flag.store(false, Ordering::Relaxed);
                    });

                    // Advance to next scheduled time immediately (don't wait for task)
                    connector.advance_schedule();
                    debug!(
                        connector = %connector.name,
                        next_run = ?connector.next_run,
                        "next scheduled run"
                    );
                }
            }

            tokio::time::sleep(self.check_interval).await;
        }
    }

    /// Run all connectors once immediately (for testing/manual triggers)
    pub async fn run_once(&self) -> Vec<(&str, Vec<Result<Batch, ConnectorError>>)> {
        let mut all_results = Vec::new();
        for connector in &self.connectors {
            let results = connector.pull_all().await;
            all_results.push((connector.name.as_str(), results));
        }
        all_results
    }
}

/// Execute a connector pull operation (runs inside spawned task)
async fn execute_connector_pull(
    name: &str,
    connector: &ConnectorImpl,
    entities: &[String],
    batch_sender: &mpsc::Sender<Arc<Batch>>,
) -> (u32, u32) {
    let mut success = 0u32;
    let mut failed = 0u32;

    for entity in entities {
        match connector.pull(entity).await {
            Ok(batch) => {
                if let Err(e) = batch_sender.send(Arc::new(batch)).await {
                    error!(
                        connector = %name,
                        entity = %entity,
                        error = %e,
                        "failed to send batch to pipeline"
                    );
                    failed += 1;
                } else {
                    debug!(
                        connector = %name,
                        entity = %entity,
                        "pulled entity successfully"
                    );
                    success += 1;
                }
            }
            Err(e) => {
                warn!(
                    connector = %name,
                    entity = %entity,
                    error = %e,
                    "pull failed"
                );
                failed += 1;
            }
        }
    }

    (success, failed)
}

/// Builder for creating a scheduler from config
#[allow(dead_code)]
pub struct SchedulerBuilder {
    batch_sender: mpsc::Sender<Arc<Batch>>,
    connectors: Vec<ScheduledConnector>,
}

#[allow(dead_code)]
impl SchedulerBuilder {
    /// Create a new builder
    pub fn new(batch_sender: mpsc::Sender<Arc<Batch>>) -> Self {
        Self {
            batch_sender,
            connectors: Vec::new(),
        }
    }

    /// Add a GitHub connector from config
    #[cfg(feature = "github")]
    pub fn add_github(
        mut self,
        name: &str,
        config: &GitHubConnectorConfig,
        schedule: Option<&str>,
    ) -> Result<Self, ConnectorError> {
        let connector = ScheduledConnector::github(name.to_string(), config, schedule)?;
        self.connectors.push(connector);
        Ok(self)
    }

    /// Add a Shopify connector from config
    #[cfg(feature = "shopify")]
    pub fn add_shopify(
        mut self,
        name: &str,
        config: &ShopifyConnectorConfig,
        schedule: Option<&str>,
    ) -> Result<Self, ConnectorError> {
        let connector = ScheduledConnector::shopify(name.to_string(), config, schedule)?;
        self.connectors.push(connector);
        Ok(self)
    }

    /// Build the scheduler
    pub fn build(self) -> ConnectorScheduler {
        let mut scheduler = ConnectorScheduler::new(self.batch_sender);
        for connector in self.connectors {
            scheduler.add(connector);
        }
        scheduler
    }
}
