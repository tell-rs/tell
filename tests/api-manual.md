# API Manual Verification Checklist

Quick smoke tests for verifying API functionality with curl.

## Prerequisites

```bash
# Start server
cargo run --release --bin tell -- serve --port 8080

# Set base URL
export API=http://localhost:8080/api/v1
```

## Auth Flow

### Initial Setup (first run only)
```bash
# Check if setup needed
curl -s $API/auth/setup/status | jq

# Create admin user
curl -X POST $API/auth/setup \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@test.com","password":"testpass123"}' | jq

# Save token
export TOKEN=$(curl -s -X POST $API/auth/setup \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@test.com","password":"testpass123"}' | jq -r '.data.token')
```

### Login
```bash
curl -X POST $API/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@test.com","password":"testpass123"}' | jq

# Wrong password should fail
curl -X POST $API/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@test.com","password":"wrong"}' | jq
# Expected: 401
```

## Protected Routes (require auth)

```bash
# Without auth - should fail
curl $API/user/workspaces | jq
# Expected: 401

# With auth - should work
curl $API/user/workspaces \
  -H "Authorization: Bearer $TOKEN" | jq
# Expected: 200
```

## Metrics Endpoints

```bash
# DAU
curl "$API/metrics/dau?range=7d" \
  -H "Authorization: Bearer $TOKEN" | jq

# WAU
curl "$API/metrics/wau?range=7d" \
  -H "Authorization: Bearer $TOKEN" | jq

# MAU
curl "$API/metrics/mau?range=30d" \
  -H "Authorization: Bearer $TOKEN" | jq

# Events count
curl "$API/metrics/events?range=7d" \
  -H "Authorization: Bearer $TOKEN" | jq

# Top events
curl "$API/metrics/events/top?range=7d&limit=10" \
  -H "Authorization: Bearer $TOKEN" | jq

# Logs volume
curl "$API/metrics/logs?range=7d" \
  -H "Authorization: Bearer $TOKEN" | jq

# Invalid range should fail
curl "$API/metrics/dau?range=invalid" \
  -H "Authorization: Bearer $TOKEN" | jq
# Expected: 400
```

## Data Endpoints

```bash
# List sources
curl "$API/data/sources" \
  -H "Authorization: Bearer $TOKEN" | jq

# Get fields for events
curl "$API/data/sources/events/fields" \
  -H "Authorization: Bearer $TOKEN" | jq

# Get field values
curl "$API/data/sources/events/values/event_name?limit=10" \
  -H "Authorization: Bearer $TOKEN" | jq

# Execute query
curl -X POST "$API/data/query" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query":"SELECT * FROM events_v1 LIMIT 5"}' | jq

# Non-SELECT should fail
curl -X POST "$API/data/query" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query":"DELETE FROM events_v1"}' | jq
# Expected: 400
```

## Board CRUD

```bash
# Create board
curl -X POST "$API/boards" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"Test Board","description":"A test board"}' | jq

# List boards
curl "$API/boards" \
  -H "Authorization: Bearer $TOKEN" | jq

# Get board (replace ID)
curl "$API/boards/BOARD_ID" \
  -H "Authorization: Bearer $TOKEN" | jq

# Update board
curl -X PATCH "$API/boards/BOARD_ID" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"Updated Name"}' | jq

# Pin board
curl -X POST "$API/boards/BOARD_ID/pin" \
  -H "Authorization: Bearer $TOKEN" | jq

# Delete board
curl -X DELETE "$API/boards/BOARD_ID" \
  -H "Authorization: Bearer $TOKEN" | jq
```

## Sharing

```bash
# Create share link
curl -X POST "$API/boards/BOARD_ID/share" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{}' | jq

# Access shared board (no auth needed)
curl "http://localhost:8080/s/b/HASH" | jq
```

## Admin Endpoints

```bash
# List workspace members
curl "$API/admin/workspaces/WS_ID/members" \
  -H "Authorization: Bearer $TOKEN" | jq

# Create API key
curl -X POST "$API/admin/api-keys" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"Test Key"}' | jq

# List API keys
curl "$API/admin/api-keys" \
  -H "Authorization: Bearer $TOKEN" | jq
```

## Health Check (no auth)

```bash
curl http://localhost:8080/health | jq
# Expected: {"status":"ok"}
```

## Expected Status Codes

| Scenario | Expected |
|----------|----------|
| Success | 200 |
| Created | 201 |
| No auth header | 401 |
| Invalid token | 401 |
| Wrong password | 401 |
| Insufficient permission | 403 |
| Resource not found | 404 |
| Validation error | 400 |
| Server error | 500 |
