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

## Workspace Isolation

Multi-tenant workspaces with membership-based access control.

### Membership Model

```
workspace_memberships
├── user_id        # User
├── workspace_id   # Workspace
├── role           # Viewer/Editor/Admin
└── status         # Invited/Active/Removed
```

### MembershipProvider Trait

Abstraction for workspace membership validation:

```rust
#[async_trait]
pub trait MembershipProvider: Send + Sync {
    async fn get_membership(&self, user_id: &str, workspace_id: &str)
        -> Result<Option<Membership>>;
}
```

Implementations:
- `LocalUserStore` - SQLite-backed (self-hosted)
- `AllowAllMembership` - Development/testing mode

## Type-Safe Extractors (Recommended)

The `Auth<P>` and `Workspace<P>` extractors enforce permissions at compile-time and extraction-time, before your handler runs.

### Permission Levels

| Marker | Required Permission | Use Case |
|--------|---------------------|----------|
| `AnyUser` | None (default) | Any authenticated user |
| `CanCreate` | `Create` | Editor+ |
| `CanAdmin` | `Admin` | Admin+ |
| `CanPlatform` | `Platform` | Platform only |

### Auth<P> - User Authentication

Validates the user and optionally checks global permission level.

```rust
use tell_api::auth::{Auth, CanCreate, CanAdmin};

// Any authenticated user
async fn get_profile(auth: Auth) -> impl IntoResponse {
    let user_id = auth.user_id();
    let email = auth.email();
}

// Requires Editor+ globally
async fn create_api_key(auth: Auth<CanCreate>) -> impl IntoResponse {
    // Guaranteed: user has Create permission
}

// Requires Admin+ globally
async fn admin_action(auth: Auth<CanAdmin>) -> impl IntoResponse {
    // Guaranteed: user has Admin permission
}
```

### Workspace<P> - Workspace Scoped Access

Validates user + workspace membership + workspace-level permission.

```rust
use tell_api::auth::{Workspace, CanCreate, CanAdmin};

// Any workspace member (sends X-Workspace-ID header)
async fn list_boards(ws: Workspace) -> impl IntoResponse {
    let workspace_id = ws.id();
    let user_id = ws.user_id();
    let role = ws.role();  // User's role in this workspace
}

// Requires Editor+ in workspace
async fn create_board(ws: Workspace<CanCreate>) -> impl IntoResponse {
    // Guaranteed: user is workspace member with Editor+ role
}

// Requires Admin+ in workspace
async fn manage_members(ws: Workspace<CanAdmin>) -> impl IntoResponse {
    // Guaranteed: user is workspace admin
}
```

### Guarantees

When you have a `Workspace<P>`, it means:
1. User is authenticated (valid JWT)
2. Workspace ID was provided (`X-Workspace-ID` header or `?workspace_id=` query)
3. User is an **active** member of the workspace
4. User has the required permission level in the workspace

### Error Responses

| Error | Status | Code |
|-------|--------|------|
| Missing token | 401 | `AUTH_REQUIRED` |
| Invalid token | 401 | `INVALID_TOKEN` |
| Token expired | 401 | `TOKEN_EXPIRED` |
| Missing workspace header | 400 | `MISSING_WORKSPACE` |
| Not workspace member | 403 | `NOT_MEMBER` |
| Membership inactive | 403 | `MEMBERSHIP_INACTIVE` |
| Insufficient permission | 403 | `INSUFFICIENT_PERMISSION` |

## Content Ownership

Most checks are ownership-based, not just role-based:

```rust
fn can_edit(content: &Content, user: &UserInfo) -> bool {
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

## Legacy Extractors

Still available for backwards compatibility:

```rust
use tell_api::auth::{AuthUser, RequirePermissionLayer, RouterExt};

// Manual permission check
async fn handler(user: AuthUser) -> impl IntoResponse {
    if user.has_permission(Permission::Create) {
        // Editor+ can create
    }
}

// Router-level permission layer
Router::new()
    .route("/dashboard", post(create_dashboard))
    .with_permission(Permission::Create)
```

## Crate Structure

```
tell-auth/
├── provider.rs    # AuthProvider trait, LocalJwtProvider
├── user_store.rs  # SQLite user storage (local auth)
├── context.rs     # AuthContext, WorkspaceAccess
├── membership.rs  # MembershipProvider trait
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

**Why type-safe extractors?**

Permission checks in the type signature make requirements explicit and move validation to extraction time:

```rust
// Old: permission check buried in handler
async fn create_board(user: AuthUser) -> impl IntoResponse {
    if !user.has_permission(Permission::Create) {
        return Err(ApiError::forbidden("..."));
    }
    // ... actual logic
}

// New: permission is in the type
async fn create_board(ws: Workspace<CanCreate>) -> impl IntoResponse {
    // Permission already validated, just do the work
}
```

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
