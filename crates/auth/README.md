# tell-auth

Authentication and authorization for Tell.

## Auth Providers

| Provider | Use Case | Config |
|----------|----------|--------|
| **Local** | Self-hosted (default) | `provider = "local"` |
| **WorkOS** | Cloud/enterprise SSO | `provider = "workos"` |

## Local Auth (Self-Hosted)

First-run creates admin, then email/password login:

```toml
[auth]
provider = "local"
jwt_secret = "your-secret-key-at-least-32-characters"
jwt_expires_in = "24h"

[auth.local]
db_path = "data/users.db"
```

**Endpoints:**
- `GET /api/v1/auth/setup/status` - Check if setup needed
- `POST /api/v1/auth/setup` - Create initial admin (first-run only)
- `POST /api/v1/auth/login` - Email/password → JWT

## Two Token Systems

| System | Format | Use Case |
|--------|--------|----------|
| Streaming | `000102...0f` (32 hex) | Collector ingestion, O(1) lookup |
| HTTP API | `tell_<jwt>` | Dashboard/API, role-based access |

## Roles (4)

| Role | Capabilities |
|------|--------------|
| `Viewer` | View analytics and shared content |
| `Editor` | Create/edit own dashboards, metrics, queries |
| `Admin` | Manage workspace (members, settings, API keys) |
| `Platform` | Cross-workspace operations (enterprise) |

Roles are hierarchical: Platform > Admin > Editor > Viewer

## Permissions (3)

| Permission | Min Role | What it allows |
|------------|----------|----------------|
| `Create` | Editor | Create/edit/share own content |
| `Admin` | Admin | Manage workspace |
| `Platform` | Platform | Cross-workspace ops |

Note: **View is implicit** for all workspace members.

## Content Ownership

Most checks are ownership-based, not just role-based:

```rust
fn can_edit(content: &Content, user: &User) -> bool {
    content.owner_id == user.id || user.is_admin()
}
```

- You can always edit your own content
- Admins can edit anyone's content
- Sharing controls visibility, not edit access

## API Keys

- Any user can create API keys
- API keys inherit the creator's role
- If you're an Editor, your API key has Editor permissions

## Usage

### Route Protection

```rust
use tell_api::auth::{Permission, RouterExt};

// Routes that create content (Editor+)
Router::new()
    .route("/dashboard", post(create_dashboard))
    .route("/query", post(save_query))
    .with_permission(Permission::Create)

// Admin routes
Router::new()
    .route("/members", get(list_members).post(add_member))
    .route("/settings", put(update_settings))
    .with_permission(Permission::Admin)
```

### User Checks

```rust
use tell_auth::{UserInfo, Permission, Role};

// Permission check
if user.has_permission(Permission::Create) {
    // Can create content
}

// Role check
if user.is_admin() {
    // Can manage workspace
}

// Ownership check
if content.owner_id == user.id || user.is_admin() {
    // Can edit this content
}
```

### Extractors

```rust
use tell_api::auth::{AuthUser, WorkspaceId};

async fn handler(
    user: AuthUser,           // Requires auth (401 if missing)
    workspace: WorkspaceId,   // From header/query/token
) -> impl IntoResponse {
    if user.can_create() {
        // Editor+ can create
    }
}
```

## Crate Structure

```
tell-auth/
├── provider.rs    # AuthProvider trait, LocalJwtProvider
├── user_store.rs  # SQLite user storage (local auth)
├── password.rs    # Argon2 password hashing
├── claims.rs      # JWT TokenClaims
├── store.rs       # ApiKeyStore (streaming auth)
├── roles.rs       # Role, Permission enums
├── user.rs        # UserInfo struct
├── workspace.rs   # WorkspaceId type
├── error.rs       # AuthError
└── test_utils.rs  # JWT generation for tests
```

## Design Decisions

**Why so few permissions?**

Product analytics is not enterprise ACL software. Users either:
- View stuff (Viewer)
- Create stuff (Editor)
- Manage stuff (Admin)

Fine-grained permissions (28 in our first attempt) add complexity without value.

**Why ownership-based checks?**

"Can I edit this dashboard?" depends on who created it, not just your role. Editors can edit their own content. Admins can edit anything.

**Why no SuperAdmin?**

For cloud multi-tenant, use a separate admin console. For self-hosted, Platform role covers cross-workspace needs.
