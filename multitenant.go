package goredis

import (
	"context"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// MultiTenantClient manages per-tenant cache clients with isolated namespaces.
// Each tenant gets its own *Client with namespace <base>:<tenantID>,
// ensuring key and tag isolation across tenants while sharing the
// underlying Redis connection pool.
type MultiTenantClient struct {
	rdb     *redis.Client
	base    string
	opts    []Option
	mu      sync.RWMutex
	clients map[string]*Client
}

// NewMultiTenant creates a MultiTenantClient.
// base is the environment prefix (e.g. "prod", "staging").
// opts are shared across all tenant clients (codec, metrics, etc.).
func NewMultiTenant(rdb *redis.Client, base string, opts ...Option) *MultiTenantClient {
	return &MultiTenantClient{
		rdb:     rdb,
		base:    base,
		opts:    opts,
		clients: make(map[string]*Client),
	}
}

// Client returns the per-tenant *Client for the given tenantID, creating it lazily.
func (m *MultiTenantClient) Tenant(tenantID string) *Client {
	m.mu.RLock()
	c, ok := m.clients[tenantID]
	m.mu.RUnlock()
	if ok {
		return c
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if c, ok := m.clients[tenantID]; ok {
		return c
	}

	ns := m.base + ":" + tenantID
	c = New(m.rdb, append(m.opts, WithNamespace(ns))...)
	m.clients[tenantID] = c
	return c
}

// CacheGetOrFetch retrieves a value by key for a tenant, calling fetch on miss.
func CacheGetOrFetchT[T any](
	ctx context.Context,
	m *MultiTenantClient,
	tenantID, key string,
	ttl time.Duration,
	fetch func() (T, error),
	tags ...string,
) (T, error) {
	return CacheGetOrFetch(ctx, m.Tenant(tenantID), key, ttl, fetch, tags...)
}

// CacheGet retrieves a cached value for a tenant.
func CacheGetT[T any](ctx context.Context, m *MultiTenantClient, tenantID, key string) (T, bool) {
	return CacheGet[T](ctx, m.Tenant(tenantID), key)
}

// CacheGetMany retrieves multiple values at once for a tenant.
func CacheGetManyT[T any](ctx context.Context, m *MultiTenantClient, tenantID string, keys ...string) map[string]T {
	return CacheGetMany[T](ctx, m.Tenant(tenantID), keys...)
}

// CacheSet stores a value and registers it under tags for a tenant.
func (m *MultiTenantClient) CacheSet(
	ctx context.Context,
	tenantID, key string,
	value any,
	ttl time.Duration,
	tags ...string,
) error {
	return m.Tenant(tenantID).CacheSet(ctx, key, value, ttl, tags...)
}

// CacheSetMany stores multiple values atomically for a tenant.
func (m *MultiTenantClient) CacheSetMany(
	ctx context.Context,
	tenantID string,
	items map[string]any,
	ttl time.Duration,
	tags ...string,
) error {
	return m.Tenant(tenantID).CacheSetMany(ctx, items, ttl, tags...)
}

// InvalidateByTags deletes all cache keys associated with the given tags for a tenant.
func (m *MultiTenantClient) InvalidateByTags(ctx context.Context, tenantID string, tags ...string) error {
	return m.Tenant(tenantID).InvalidateByTags(ctx, tags...)
}

// InvalidatePrefix deletes all cache keys with the given key prefix for a tenant.
func (m *MultiTenantClient) InvalidatePrefix(ctx context.Context, tenantID, prefix string) error {
	return m.Tenant(tenantID).InvalidatePrefix(ctx, prefix)
}

// InvalidatePattern deletes all cache keys matching the given glob pattern for a tenant.
func (m *MultiTenantClient) InvalidatePattern(ctx context.Context, tenantID, pattern string) error {
	return m.Tenant(tenantID).InvalidatePattern(ctx, pattern)
}

// CleanTag removes stale key references from a tag set for a tenant.
func (m *MultiTenantClient) CleanTag(ctx context.Context, tenantID, tag string) error {
	return m.Tenant(tenantID).CleanTag(ctx, tag)
}

// HashRequest generates a deterministic hex hash from a request struct for a tenant.
func (m *MultiTenantClient) HashRequest(ctx context.Context, tenantID string, req any) (string, error) {
	return m.Tenant(tenantID).HashRequest(req)
}
