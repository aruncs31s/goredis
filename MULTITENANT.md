# Multi-Tenant Support

`MultiTenantClient` provides per-tenant cache isolation over a shared Redis connection pool. Each tenant gets its own namespace `<base>:<tenantID>` — keys, tags, and singleflight groups never cross tenant boundaries.

## Quick Start

```go
import (
    "github.com/go-redis/redis/v8"
    "github.com/aruncs31s/goredis"
)

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

// Single client for all tenants
cache := goredis.NewMultiTenant(rdb, "prod")

// Per-tenant methods
user, err := goredis.CacheGetOrFetchT(ctx, cache, "acme", "user:42", 5*time.Minute,
    func() (*User, error) {
        return &User{ID: 42, Name: "Alice"}, nil
    },
    "users",
)

cache.InvalidateByTags(ctx, "acme", "users")
```

## Key Isolation

| Tenant | Key | Tag |
|--------|-----|-----|
| acme | `prod:acme:user:42` | `prod:acme:tag:users` |
| corp | `prod:corp:user:42` | `prod:corp:tag:users` |

The `base` parameter (e.g. `"prod"`, `"staging"`) separates environments. Within an environment, the `tenantID` separates tenants.

## Shared Options

Options passed to `NewMultiTenant` apply to every tenant:

```go
cache := goredis.NewMultiTenant(rdb, "prod",
    goredis.WithCodec(msgpackCodec{}),
    goredis.WithMetrics(promMetrics),
)
```

Tenant clients are created lazily on first use and cached forever.

## API

### `NewMultiTenant`

```go
func NewMultiTenant(rdb *redis.Client, base string, opts ...Option) *MultiTenantClient
```

- `rdb` — shared Redis client
- `base` — environment prefix (e.g. `"prod"`, `"staging"`), passed as `""` to omit
- `opts` — shared options (codec, metrics)

### `Tenant`

```go
func (m *MultiTenantClient) Tenant(tenantID string) *Client
```

Returns the underlying per-tenant `*Client`, creating it lazily. Use this to access any `Client` method directly.

### Per-Tenant Functions

All functions take `tenantID` as the first parameter after `ctx`.

```go
func CacheGetOrFetchT[T any](ctx, m, tenantID, key, ttl, fetch, tags...) (T, error)
func CacheGetT[T any](ctx, m, tenantID, key) (T, bool)
func CacheGetManyT[T any](ctx, m, tenantID, keys...) map[string]T
```

### Per-Tenant Methods

```go
func (m *MultiTenantClient) CacheSet(ctx, tenantID, key, value, ttl, tags...) error
func (m *MultiTenantClient) CacheSetMany(ctx, tenantID, items, ttl, tags...) error
func (m *MultiTenantClient) InvalidateByTags(ctx, tenantID, tags...) error
func (m *MultiTenantClient) InvalidatePrefix(ctx, tenantID, prefix) error
func (m *MultiTenantClient) InvalidatePattern(ctx, tenantID, pattern) error
func (m *MultiTenantClient) CleanTag(ctx, tenantID, tag) error
func (m *MultiTenantClient) HashRequest(ctx, tenantID, req) (string, error)
```

## Underlying `*Client`

Tenant clients are ordinary `*Client` instances. Any feature available on `*Client` (namespace, codec, metrics, singleflight) works identically within each tenant.

```go
acme := cache.Tenant("acme")
acme.CleanTag(ctx, "sessions")
acme.InvalidatePrefix(ctx, "temp")
```
