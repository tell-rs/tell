# Shopify Connector

Pulls store KPIs from the Shopify Admin API.

## Quick Start

```toml
[connectors.my_store]
type = "shopify"
store = "mystore.myshopify.com"
access_token = "shpat_xxxxxxxxxxxxx"
entities = ["all"]
schedule = "0 0 */6 * * *"  # every 6 hours
```

## Setup

### 1. Create a Custom App in Shopify

1. Go to **Shopify Admin** → **Settings** → **Apps and sales channels**
2. Click **Develop apps** → **Create an app**
3. Name it anything (e.g., "CDP Metrics")

### 2. Configure API Scopes

Click **Configure Admin API scopes** and enable:

| Scope | Required For |
|-------|--------------|
| `read_orders` | Order metrics, revenue, refunds |
| `read_products` | Product counts |
| `read_customers` | Customer metrics |
| `read_inventory` | Inventory levels, low stock |
| `read_checkouts` | Abandoned checkout count |

### 3. Install and Get Token

1. Click **Install app**
2. Copy the **Admin API access token** (starts with `shpat_`)

That's it. You need:
- `store`: your `*.myshopify.com` domain
- `access_token`: the `shpat_xxx` token

## Configuration

```toml
[connectors.shopify_main]
type = "shopify"
store = "mystore.myshopify.com"      # required
access_token = "shpat_xxx"           # required
entities = ["orders", "products", "customers", "operations"]  # or ["all"]
schedule = "0 0 */6 * * *"           # 6-field cron (optional)
metrics = ["orders_total", "revenue_30d"]  # optional, default: all
workspace_id = 1                     # optional
api_version = "2024-01"              # optional
timeout_secs = 30                    # optional
max_retries = 3                      # optional
```

### Entities

| Entity | Description |
|--------|-------------|
| `orders` | Order counts, revenue, AOV |
| `products` | Product counts |
| `customers` | Customer counts |
| `operations` | Unfulfilled orders, refunds, inventory, abandoned carts |
| `all` | Everything above |

## Available Metrics

### Order Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `orders_total` | u64 | Total orders (all time) |
| `orders_today` | u64 | Orders created today |
| `orders_30d` | u64 | Orders in last 30 days |
| `revenue_today` | f64 | Revenue from today |
| `revenue_30d` | f64 | Revenue in last 30 days |
| `average_order_value` | f64 | AOV (30-day) |

### Product Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `products_total` | u64 | Total products |
| `products_active` | u64 | Active products |

### Customer Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `customers_total` | u64 | Total customers |
| `customers_30d` | u64 | New customers (30 days) |

### Operations Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `unfulfilled_orders` | u64 | Orders waiting to ship |
| `refunds_30d` | u64 | Refund count (30 days) |
| `refund_amount_30d` | f64 | Refund amount (30 days) |
| `abandoned_checkouts` | u64 | Abandoned cart count |
| `inventory_items` | u64 | Total inventory units |
| `low_stock_count` | u64 | Items with < 10 units |

## Output Example

```json
{
  "timestamp_ms": 1768134100369,
  "source": "shopify",
  "entity": "mystore.myshopify.com",
  "metrics": {
    "orders_total": 15420,
    "orders_today": 23,
    "orders_30d": 890,
    "revenue_today": 2300.00,
    "revenue_30d": 89000.00,
    "average_order_value": 100.00,
    "products_total": 150,
    "products_active": 120,
    "customers_total": 8500,
    "customers_30d": 340,
    "unfulfilled_orders": 45,
    "refunds_30d": 12,
    "refund_amount_30d": 1200.00,
    "abandoned_checkouts": 230,
    "inventory_items": 5000,
    "low_stock_count": 8
  }
}
```

## Rate Limits

Shopify allows **2 requests/second** (leaky bucket, 40 req capacity). The connector makes ~10-15 API calls per full pull. With a 6-hour schedule, this is negligible.

## Notes

- **Read-only**: The connector only fetches data, never writes
- **Abandoned checkouts**: Requires Shopify Basic plan or higher
- **Inventory**: Requires `read_inventory` scope and inventory tracking enabled
