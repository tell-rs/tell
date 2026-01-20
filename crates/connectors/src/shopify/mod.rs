//! Shopify connector for pulling store metrics
//!
//! Fetches store statistics (orders, revenue, customers, etc.) from the Shopify Admin API
//! and converts them into snapshots for the pipeline.

use crate::config::ShopifyConnectorConfig;
use crate::error::ConnectorError;
use crate::resilience::{CircuitBreaker, ResilienceConfig, RetryError, execute_with_retry};
use crate::traits::Connector;
use chrono::{Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tell_protocol::{Batch, BatchBuilder, BatchType, SourceId};
use tracing::{debug, warn};

/// Shopify connector configuration (simple version for CLI)
#[derive(Debug, Clone)]
pub struct ShopifyConfig {
    /// Store domain (e.g., mystore.myshopify.com)
    pub store: String,
    /// Shopify Admin API access token
    pub access_token: Option<String>,
    /// API version (default: 2024-01)
    pub api_version: String,
}

impl Default for ShopifyConfig {
    fn default() -> Self {
        Self {
            store: String::new(),
            access_token: None,
            api_version: "2024-01".to_string(),
        }
    }
}

/// Shopify connector for fetching store metrics
pub struct Shopify {
    store: String,
    access_token: Option<String>,
    api_version: String,
    client: reqwest::Client,
    source_id: SourceId,
    /// Which metrics to include (None = all)
    metrics_filter: Option<Vec<String>>,
    /// Workspace ID for batch routing
    workspace_id: u32,
    /// Resilience configuration
    resilience: ResilienceConfig,
    /// Circuit breaker for this connector
    circuit_breaker: Arc<CircuitBreaker>,
}

impl Shopify {
    /// Create a new Shopify connector with the given configuration
    ///
    /// # Errors
    ///
    /// Returns error if HTTP client creation fails (e.g., TLS or proxy misconfiguration)
    pub fn new(config: ShopifyConfig) -> Result<Self, ConnectorError> {
        let resilience = ResilienceConfig::default();
        let client = reqwest::Client::builder()
            .user_agent("tell/0.1")
            .timeout(Duration::from_secs(resilience.timeout_secs))
            .build()
            .map_err(|e| ConnectorError::Init(format!("Shopify HTTP client: {}", e)))?;

        let circuit_breaker = Arc::new(CircuitBreaker::new("shopify", resilience.clone()));

        Ok(Self {
            store: config.store,
            access_token: config.access_token,
            api_version: config.api_version,
            client,
            source_id: SourceId::new("connector:shopify"),
            metrics_filter: None,
            workspace_id: 1,
            resilience,
            circuit_breaker,
        })
    }

    /// Create a Shopify connector from connector config (from TOML)
    ///
    /// # Errors
    ///
    /// Returns error if HTTP client creation fails
    pub fn from_config(config: &ShopifyConnectorConfig) -> Result<Self, ConnectorError> {
        let resilience = config.resilience_config();
        let client = reqwest::Client::builder()
            .user_agent("tell/0.1")
            .timeout(Duration::from_secs(resilience.timeout_secs))
            .build()
            .map_err(|e| ConnectorError::Init(format!("Shopify HTTP client: {}", e)))?;

        let circuit_breaker = Arc::new(CircuitBreaker::new("shopify", resilience.clone()));

        Ok(Self {
            store: config.store.clone(),
            access_token: config.access_token.clone(),
            api_version: config.api_version.clone(),
            client,
            source_id: SourceId::new("connector:shopify"),
            metrics_filter: config.metrics.clone(),
            workspace_id: config.workspace_id,
            resilience,
            circuit_breaker,
        })
    }

    /// Build API URL for an endpoint
    fn api_url(&self, endpoint: &str) -> String {
        format!(
            "https://{}/admin/api/{}/{}",
            self.store, self.api_version, endpoint
        )
    }

    /// Build a request with auth header
    fn build_request(&self, url: &str) -> reqwest::RequestBuilder {
        let mut request = self.client.get(url);
        if let Some(ref token) = self.access_token {
            request = request.header("X-Shopify-Access-Token", token);
        }
        request
    }

    /// Handle common HTTP response errors
    fn handle_error_status(&self, response: reqwest::Response, context: &str) -> ConnectorError {
        match response.status() {
            reqwest::StatusCode::NOT_FOUND => ConnectorError::NotFound(context.to_string()),
            reqwest::StatusCode::UNAUTHORIZED => {
                ConnectorError::AuthFailed("Invalid or missing access token".into())
            }
            reqwest::StatusCode::FORBIDDEN => {
                ConnectorError::AuthFailed("Access denied - check API permissions".into())
            }
            reqwest::StatusCode::TOO_MANY_REQUESTS => ConnectorError::RateLimited {
                retry_after_secs: 60,
            },
            _ => ConnectorError::Http(response.error_for_status().unwrap_err()),
        }
    }

    /// Execute an operation with retry and circuit breaker
    async fn execute_with_resilience<F, Fut, T>(
        &self,
        entity: &str,
        operation: F,
    ) -> Result<T, ConnectorError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, ConnectorError>>,
    {
        if self.circuit_breaker.is_open() {
            warn!(
                connector = "shopify",
                entity = %entity,
                "circuit breaker open, skipping request"
            );
            return Err(ConnectorError::ConfigError(
                "circuit breaker open".to_string(),
            ));
        }

        let result = execute_with_retry(
            &self.resilience,
            Some(self.circuit_breaker.as_ref()),
            entity,
            operation,
        )
        .await;

        match result {
            Ok(value) => Ok(value),
            Err(RetryError::CircuitOpen) => Err(ConnectorError::ConfigError(
                "circuit breaker open".to_string(),
            )),
            Err(RetryError::Exhausted {
                attempts,
                last_error,
            }) => {
                warn!(
                    connector = "shopify",
                    entity = %entity,
                    attempts,
                    error = %last_error,
                    "request failed after retries"
                );
                Err(ConnectorError::ConfigError(format!(
                    "failed after {} attempts: {}",
                    attempts, last_error
                )))
            }
            Err(RetryError::Permanent(e)) => Err(e),
        }
    }

    /// Check if a metric should be included
    fn include_metric(&self, metric: &str) -> bool {
        match &self.metrics_filter {
            None => true,
            Some(filter) => filter.iter().any(|m| m == metric),
        }
    }

    // --- Order Metrics ---

    /// Fetch order count
    async fn fetch_order_count_once(&self) -> Result<u64, ConnectorError> {
        let url = self.api_url("orders/count.json?status=any");
        let response = self.build_request(&url).send().await?;

        if !response.status().is_success() {
            return Err(self.handle_error_status(response, "orders/count"));
        }

        let data: CountResponse = response.json().await?;
        Ok(data.count)
    }

    /// Fetch orders from last N days and calculate metrics
    async fn fetch_order_stats_once(&self, days: i64) -> Result<OrderStats, ConnectorError> {
        let since = (Utc::now() - ChronoDuration::days(days)).to_rfc3339();
        let url = self.api_url(&format!(
            "orders.json?status=any&created_at_min={}&fields=id,total_price,created_at",
            urlencoding::encode(&since)
        ));
        let response = self.build_request(&url).send().await?;

        if !response.status().is_success() {
            return Err(self.handle_error_status(response, "orders"));
        }

        let data: OrdersResponse = response.json().await?;

        let count = data.orders.len() as u64;
        let revenue: f64 = data
            .orders
            .iter()
            .filter_map(|o| o.total_price.parse::<f64>().ok())
            .sum();
        let avg_order_value = if count > 0 {
            revenue / count as f64
        } else {
            0.0
        };

        Ok(OrderStats {
            count,
            revenue,
            avg_order_value,
        })
    }

    /// Fetch today's order stats
    async fn fetch_today_order_stats_once(&self) -> Result<OrderStats, ConnectorError> {
        let today = Utc::now().format("%Y-%m-%dT00:00:00Z").to_string();
        let url = self.api_url(&format!(
            "orders.json?status=any&created_at_min={}&fields=id,total_price",
            urlencoding::encode(&today)
        ));
        let response = self.build_request(&url).send().await?;

        if !response.status().is_success() {
            return Err(self.handle_error_status(response, "orders/today"));
        }

        let data: OrdersResponse = response.json().await?;

        let count = data.orders.len() as u64;
        let revenue: f64 = data
            .orders
            .iter()
            .filter_map(|o| o.total_price.parse::<f64>().ok())
            .sum();

        Ok(OrderStats {
            count,
            revenue,
            avg_order_value: if count > 0 {
                revenue / count as f64
            } else {
                0.0
            },
        })
    }

    // --- Product Metrics ---

    /// Fetch product counts
    async fn fetch_product_counts_once(&self) -> Result<ProductStats, ConnectorError> {
        // Total products
        let url = self.api_url("products/count.json");
        let response = self.build_request(&url).send().await?;

        if !response.status().is_success() {
            return Err(self.handle_error_status(response, "products/count"));
        }

        let total: CountResponse = response.json().await?;

        // Active products
        let url = self.api_url("products/count.json?status=active");
        let response = self.build_request(&url).send().await?;

        if !response.status().is_success() {
            return Err(self.handle_error_status(response, "products/count/active"));
        }

        let active: CountResponse = response.json().await?;

        Ok(ProductStats {
            total: total.count,
            active: active.count,
        })
    }

    // --- Fulfillment Metrics ---

    /// Fetch unfulfilled order count
    async fn fetch_unfulfilled_orders_once(&self) -> Result<u64, ConnectorError> {
        let url = self.api_url("orders/count.json?fulfillment_status=unfulfilled&status=open");
        let response = self.build_request(&url).send().await?;

        if !response.status().is_success() {
            return Err(self.handle_error_status(response, "orders/count/unfulfilled"));
        }

        let data: CountResponse = response.json().await?;
        Ok(data.count)
    }

    // --- Refund Metrics ---

    /// Fetch refund count in last N days (via orders with refunds)
    async fn fetch_refunds_once(&self, days: i64) -> Result<RefundStats, ConnectorError> {
        let since = (Utc::now() - ChronoDuration::days(days)).to_rfc3339();
        let url = self.api_url(&format!(
            "orders.json?status=any&created_at_min={}&fields=id,refunds",
            urlencoding::encode(&since)
        ));
        let response = self.build_request(&url).send().await?;

        if !response.status().is_success() {
            return Err(self.handle_error_status(response, "orders/refunds"));
        }

        let data: OrdersWithRefundsResponse = response.json().await?;

        let mut refund_count = 0u64;
        let mut refund_amount = 0.0f64;

        for order in &data.orders {
            for refund in &order.refunds {
                refund_count += 1;
                for txn in &refund.transactions {
                    if let Ok(amount) = txn.amount.parse::<f64>() {
                        refund_amount += amount;
                    }
                }
            }
        }

        Ok(RefundStats {
            count: refund_count,
            amount: refund_amount,
        })
    }

    // --- Inventory Metrics ---

    /// Fetch inventory levels (requires inventory locations)
    async fn fetch_inventory_stats_once(&self) -> Result<InventoryStats, ConnectorError> {
        // First get locations
        let url = self.api_url("locations.json");
        let response = self.build_request(&url).send().await?;

        if !response.status().is_success() {
            return Err(self.handle_error_status(response, "locations"));
        }

        let locations: LocationsResponse = response.json().await?;

        let mut total_items = 0u64;
        let mut low_stock_count = 0u64;

        // For each location, get inventory levels
        for location in &locations.locations {
            let url = self.api_url(&format!(
                "inventory_levels.json?location_ids={}",
                location.id
            ));
            let response = self.build_request(&url).send().await?;

            if response.status().is_success() {
                let levels: InventoryLevelsResponse = response.json().await?;
                for level in &levels.inventory_levels {
                    if let Some(qty) = level.available {
                        total_items += qty.max(0) as u64;
                        // Consider low stock if < 10 items
                        if qty > 0 && qty < 10 {
                            low_stock_count += 1;
                        }
                    }
                }
            }
        }

        Ok(InventoryStats {
            total_items,
            low_stock_count,
        })
    }

    // --- Abandoned Checkout Metrics ---

    /// Fetch abandoned checkouts count
    async fn fetch_abandoned_checkouts_once(&self) -> Result<u64, ConnectorError> {
        let url = self.api_url("checkouts/count.json");
        let response = self.build_request(&url).send().await?;

        if !response.status().is_success() {
            // Checkouts API might not be available on all plans
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(0);
            }
            return Err(self.handle_error_status(response, "checkouts/count"));
        }

        let data: CountResponse = response.json().await?;
        Ok(data.count)
    }

    // --- Customer Metrics ---

    /// Fetch customer count
    async fn fetch_customer_count_once(&self) -> Result<u64, ConnectorError> {
        let url = self.api_url("customers/count.json");
        let response = self.build_request(&url).send().await?;

        if !response.status().is_success() {
            return Err(self.handle_error_status(response, "customers/count"));
        }

        let data: CountResponse = response.json().await?;
        Ok(data.count)
    }

    /// Fetch new customers in last N days
    async fn fetch_new_customers_once(&self, days: i64) -> Result<u64, ConnectorError> {
        let since = (Utc::now() - ChronoDuration::days(days)).to_rfc3339();
        let url = self.api_url(&format!(
            "customers/count.json?created_at_min={}",
            urlencoding::encode(&since)
        ));
        let response = self.build_request(&url).send().await?;

        if !response.status().is_success() {
            return Err(self.handle_error_status(response, "customers/count/recent"));
        }

        let data: CountResponse = response.json().await?;
        Ok(data.count)
    }

    // --- Aggregate Fetch Methods with Resilience ---

    async fn fetch_order_metrics(&self, store: &str) -> Result<OrderMetrics, ConnectorError> {
        let entity = format!("{}/orders", store);

        let total = self
            .execute_with_resilience(&entity, || self.fetch_order_count_once())
            .await?;

        let stats_30d = self
            .execute_with_resilience(&entity, || self.fetch_order_stats_once(30))
            .await?;

        let stats_today = self
            .execute_with_resilience(&entity, || self.fetch_today_order_stats_once())
            .await
            .unwrap_or(OrderStats::default());

        Ok(OrderMetrics {
            total,
            today: stats_today.count,
            last_30d: stats_30d.count,
            revenue_total: 0.0, // Would need separate API call for all-time
            revenue_today: stats_today.revenue,
            revenue_30d: stats_30d.revenue,
            avg_order_value: stats_30d.avg_order_value,
        })
    }

    async fn fetch_product_metrics(&self, store: &str) -> Result<ProductStats, ConnectorError> {
        let entity = format!("{}/products", store);
        self.execute_with_resilience(&entity, || self.fetch_product_counts_once())
            .await
    }

    async fn fetch_customer_metrics(&self, store: &str) -> Result<CustomerMetrics, ConnectorError> {
        let entity = format!("{}/customers", store);

        let total = self
            .execute_with_resilience(&entity, || self.fetch_customer_count_once())
            .await?;

        let last_30d = self
            .execute_with_resilience(&entity, || self.fetch_new_customers_once(30))
            .await
            .unwrap_or(0);

        Ok(CustomerMetrics { total, last_30d })
    }

    async fn fetch_operations_metrics(
        &self,
        store: &str,
    ) -> Result<OperationsMetrics, ConnectorError> {
        let entity = format!("{}/operations", store);

        let unfulfilled = self
            .execute_with_resilience(&entity, || self.fetch_unfulfilled_orders_once())
            .await
            .unwrap_or(0);

        let refunds = self
            .execute_with_resilience(&entity, || self.fetch_refunds_once(30))
            .await
            .unwrap_or_default();

        let inventory = self
            .execute_with_resilience(&entity, || self.fetch_inventory_stats_once())
            .await
            .unwrap_or_default();

        let abandoned = self
            .execute_with_resilience(&entity, || self.fetch_abandoned_checkouts_once())
            .await
            .unwrap_or(0);

        Ok(OperationsMetrics {
            unfulfilled_orders: unfulfilled,
            refunds_30d: refunds.count,
            refund_amount_30d: refunds.amount,
            abandoned_checkouts: abandoned,
            inventory_items: inventory.total_items,
            low_stock_count: inventory.low_stock_count,
        })
    }

    /// Build snapshot payload for the store
    fn build_payload(
        &self,
        store: &str,
        orders: Option<OrderMetrics>,
        products: Option<ProductStats>,
        customers: Option<CustomerMetrics>,
        operations: Option<OperationsMetrics>,
    ) -> SnapshotPayload {
        let mut metrics = FilteredMetrics::default();

        if let Some(o) = orders {
            if self.include_metric("orders_total") {
                metrics.orders_total = Some(o.total);
            }
            if self.include_metric("orders_today") {
                metrics.orders_today = Some(o.today);
            }
            if self.include_metric("orders_30d") {
                metrics.orders_30d = Some(o.last_30d);
            }
            if self.include_metric("revenue_today") {
                metrics.revenue_today = Some(o.revenue_today);
            }
            if self.include_metric("revenue_30d") {
                metrics.revenue_30d = Some(o.revenue_30d);
            }
            if self.include_metric("average_order_value") {
                metrics.average_order_value = Some(o.avg_order_value);
            }
        }

        if let Some(p) = products {
            if self.include_metric("products_total") {
                metrics.products_total = Some(p.total);
            }
            if self.include_metric("products_active") {
                metrics.products_active = Some(p.active);
            }
        }

        if let Some(c) = customers {
            if self.include_metric("customers_total") {
                metrics.customers_total = Some(c.total);
            }
            if self.include_metric("customers_30d") {
                metrics.customers_30d = Some(c.last_30d);
            }
        }

        if let Some(ops) = operations {
            if self.include_metric("unfulfilled_orders") {
                metrics.unfulfilled_orders = Some(ops.unfulfilled_orders);
            }
            if self.include_metric("refunds_30d") {
                metrics.refunds_30d = Some(ops.refunds_30d);
            }
            if self.include_metric("refund_amount_30d") {
                metrics.refund_amount_30d = Some(ops.refund_amount_30d);
            }
            if self.include_metric("abandoned_checkouts") {
                metrics.abandoned_checkouts = Some(ops.abandoned_checkouts);
            }
            if self.include_metric("inventory_items") {
                metrics.inventory_items = Some(ops.inventory_items);
            }
            if self.include_metric("low_stock_count") {
                metrics.low_stock_count = Some(ops.low_stock_count);
            }
        }

        SnapshotPayload {
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            source: "shopify".to_string(),
            entity: store.to_string(),
            metrics,
        }
    }

    /// Get circuit breaker status
    pub fn circuit_breaker_open(&self) -> bool {
        self.circuit_breaker.is_open()
    }

    /// Get current failure count
    pub fn failure_count(&self) -> u32 {
        self.circuit_breaker.failure_count()
    }
}

impl Connector for Shopify {
    fn name(&self) -> &'static str {
        "shopify"
    }

    async fn pull(&self, entity: &str) -> Result<Batch, ConnectorError> {
        // Entity format: "orders", "products", "customers", "operations", or "all"
        let store = &self.store;

        let (orders, products, customers, operations) = match entity {
            "orders" => {
                let o = self.fetch_order_metrics(store).await?;
                (Some(o), None, None, None)
            }
            "products" => {
                let p = self.fetch_product_metrics(store).await?;
                (None, Some(p), None, None)
            }
            "customers" => {
                let c = self.fetch_customer_metrics(store).await?;
                (None, None, Some(c), None)
            }
            "operations" | "inventory" => {
                let ops = self.fetch_operations_metrics(store).await?;
                (None, None, None, Some(ops))
            }
            "all" | "store" => {
                // Fetch all metrics, logging errors but continuing with partial data
                let orders = match self.fetch_order_metrics(store).await {
                    Ok(o) => Some(o),
                    Err(e) => {
                        warn!(connector = "shopify", store = %store, error = %e, "failed to fetch order metrics");
                        None
                    }
                };
                let products = match self.fetch_product_metrics(store).await {
                    Ok(p) => Some(p),
                    Err(e) => {
                        warn!(connector = "shopify", store = %store, error = %e, "failed to fetch product metrics");
                        None
                    }
                };
                let customers = match self.fetch_customer_metrics(store).await {
                    Ok(c) => Some(c),
                    Err(e) => {
                        warn!(connector = "shopify", store = %store, error = %e, "failed to fetch customer metrics");
                        None
                    }
                };
                let operations = match self.fetch_operations_metrics(store).await {
                    Ok(o) => Some(o),
                    Err(e) => {
                        warn!(connector = "shopify", store = %store, error = %e, "failed to fetch operations metrics");
                        None
                    }
                };
                (orders, products, customers, operations)
            }
            _ => {
                return Err(ConnectorError::InvalidEntity(format!(
                    "Unknown entity: {}. Use: orders, products, customers, operations, or all",
                    entity
                )));
            }
        };

        // Build payload
        let payload = self.build_payload(store, orders, products, customers, operations);
        let payload_bytes = serde_json::to_vec(&payload)?;

        // Build batch
        let mut builder = BatchBuilder::new(BatchType::Snapshot, self.source_id.clone());
        builder.set_workspace_id(self.workspace_id);
        builder.add(&payload_bytes, 1);

        debug!(
            connector = "shopify",
            store = %store,
            entity = %entity,
            workspace_id = self.workspace_id,
            "pulled snapshot"
        );

        Ok(builder.finish())
    }
}

// --- API Response Types ---

#[derive(Debug, Deserialize)]
struct CountResponse {
    count: u64,
}

#[derive(Debug, Deserialize)]
struct OrdersResponse {
    orders: Vec<Order>,
}

#[derive(Debug, Deserialize)]
struct Order {
    #[allow(dead_code)]
    id: u64,
    total_price: String,
}

#[derive(Debug, Deserialize)]
struct OrdersWithRefundsResponse {
    orders: Vec<OrderWithRefunds>,
}

#[derive(Debug, Deserialize)]
struct OrderWithRefunds {
    #[allow(dead_code)]
    id: u64,
    #[serde(default)]
    refunds: Vec<Refund>,
}

#[derive(Debug, Deserialize)]
struct Refund {
    #[serde(default)]
    transactions: Vec<RefundTransaction>,
}

#[derive(Debug, Deserialize)]
struct RefundTransaction {
    amount: String,
}

#[derive(Debug, Deserialize)]
struct LocationsResponse {
    locations: Vec<Location>,
}

#[derive(Debug, Deserialize)]
struct Location {
    id: u64,
}

#[derive(Debug, Deserialize)]
struct InventoryLevelsResponse {
    inventory_levels: Vec<InventoryLevel>,
}

#[derive(Debug, Deserialize)]
struct InventoryLevel {
    available: Option<i64>,
}

// --- Internal Stats Types ---

#[derive(Debug, Default)]
struct OrderStats {
    count: u64,
    revenue: f64,
    avg_order_value: f64,
}

#[derive(Debug)]
struct OrderMetrics {
    total: u64,
    today: u64,
    last_30d: u64,
    #[allow(dead_code)]
    revenue_total: f64,
    revenue_today: f64,
    revenue_30d: f64,
    avg_order_value: f64,
}

#[derive(Debug)]
struct ProductStats {
    total: u64,
    active: u64,
}

#[derive(Debug)]
struct CustomerMetrics {
    total: u64,
    last_30d: u64,
}

#[derive(Debug, Default)]
struct RefundStats {
    count: u64,
    amount: f64,
}

#[derive(Debug, Default)]
struct InventoryStats {
    total_items: u64,
    low_stock_count: u64,
}

#[derive(Debug, Default)]
struct OperationsMetrics {
    unfulfilled_orders: u64,
    refunds_30d: u64,
    refund_amount_30d: f64,
    abandoned_checkouts: u64,
    inventory_items: u64,
    low_stock_count: u64,
}

// --- Snapshot Payload ---

#[derive(Debug, Serialize)]
struct SnapshotPayload {
    timestamp_ms: u64,
    source: String,
    entity: String,
    metrics: FilteredMetrics,
}

#[derive(Debug, Default, Serialize)]
struct FilteredMetrics {
    // Order metrics
    #[serde(skip_serializing_if = "Option::is_none")]
    orders_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    orders_today: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    orders_30d: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revenue_today: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revenue_30d: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    average_order_value: Option<f64>,

    // Product metrics
    #[serde(skip_serializing_if = "Option::is_none")]
    products_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    products_active: Option<u64>,

    // Customer metrics
    #[serde(skip_serializing_if = "Option::is_none")]
    customers_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    customers_30d: Option<u64>,

    // Operations metrics
    #[serde(skip_serializing_if = "Option::is_none")]
    unfulfilled_orders: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refunds_30d: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refund_amount_30d: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    abandoned_checkouts: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    inventory_items: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    low_stock_count: Option<u64>,
}
