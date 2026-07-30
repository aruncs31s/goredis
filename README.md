# goredis

Tag-based cache invalidation layer over [go-redis](https://github.com/redis/go-redis).

## The Problem

Cache invalidation is hard. When a piece of data changes, you need to clear every cached key that depends on it. Without a relation tracker, you either:

- TTL everything out (stale data window)
- Delete by fragile key patterns (SCAN with prefix)
- Manually track every key (error-prone)

## The Solution

Each cache key is registered under one or more **tags** (Redis Sets). When data changes, invalidate by tag — all associated keys are discovered and deleted in parallel.

```
SET goredis:user:42        → "goredis:tag:users"    SADD user:42
SET goredis:user:list      → "goredis:tag:users"    SADD user:list
SET goredis:stats          → "goredis:tag:dashboard" SADD stats
                             "goredis:tag:users"    SADD stats

InvalidateByTags(ctx, cache, "users")
    → deletes: goredis:user:42, goredis:user:list, goredis:stats
    → deletes: goredis:tag:users
```

A reverse mapping (`goredis:keytags:<key>`) is also stored with the same TTL, enabling periodic garbage collection of stale tag set members via `CleanTag`.

## Installation

```
go get github.com/aruncs31s/goredis
```

## Quick Start

```go
import (
    "context"
    "time"
    "github.com/go-redis/redis/v8"
    "github.com/aruncs31s/goredis"
)

ctx := context.Background()
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
cache := goredis.New(rdb)

type User struct {
    ID   int    `json:"id"`
    Name string `json:"name"`
}

// Cache a user under the "users" tag
user, err := goredis.CacheGetOrFetch(ctx, cache, "user:42", 5*time.Minute,
    func() (*User, error) {
        return &User{ID: 42, Name: "Alice"}, nil
    },
    "users",
)

// After a user update, clear all cached data tagged "users"
cache.InvalidateByTags(ctx, "users")
```

## Features

- **Generic** — typed `CacheGetOrFetch[T]`, no type assertions
- **Singleflight** — concurrent misses for the same key coalesce into one fetch (stampede protection)
- **Tag-based invalidation** — delete all keys under a tag in one call
- **Key→tags reverse mapping** — enables periodic stale-key cleanup via `CleanTag`
- **Pluggable codec** — swap JSON for msgpack, protobuf, gob, etc.
- **Metrics hooks** — plug in OpenTelemetry, Prometheus, or your own
- **Namespace support** — prefix all keys to support multi-tenant or environment isolation
- **Atomic SET+SADD** — uses `MULTI/EXEC` (TxPipeline) for atomicity
- **Batch APIs** — `CacheGetMany`, `CacheSetMany`, `InvalidatePrefix`, `InvalidatePattern`
- **Nil-safe** — pass `nil` for the Redis client to silently skip caching

## API

### `New`

```go
func New(rdb *redis.Client, opts ...Option) *Client
```

Creates a cache client. Options:

- `WithCodec(codec Codec)` — custom serializer (default: JSON)
- `WithNamespace(ns string)` — key prefix (default: `"goredis"`)
- `WithMetrics(m Metrics)` — observability hooks

### `CacheGetOrFetch[T]`

```go
func CacheGetOrFetch[T any](
    ctx context.Context,
    c *Client,
    key string,
    ttl time.Duration,
    fetch func() (T, error),
    tags ...string,
) (T, error)
```

Cache-aside pattern with singleflight stampede protection. Returns cached value if found, otherwise calls `fetch`, stores the result, and registers the key under the given tags.

### `CacheGet[T]`

```go
func CacheGet[T any](ctx context.Context, c *Client, key string) (T, bool)
```

Low-level get. Returns the value and `true` on cache hit.

### `CacheGetMany[T]`

```go
func CacheGetMany[T any](ctx context.Context, c *Client, keys ...string) map[string]T
```

Bulk retrieval via `MGET`. Missing keys are omitted from the result map.

### `CacheSet`

```go
func (c *Client) CacheSet(ctx context.Context, key string, value any, ttl time.Duration, tags ...string) error
```

Store a value and register it under the given tags. Uses `MULTI/EXEC` for atomicity across SET, SADD, and EXPIRE.

### `CacheSetMany`

```go
func (c *Client) CacheSetMany(ctx context.Context, items map[string]any, ttl time.Duration, tags ...string) error
```

Atomic bulk set. All keys are registered under the same tags.

### `InvalidateByTags`

```go
func (c *Client) InvalidateByTags(ctx context.Context, tags ...string) error
```

Delete every cache key registered under the given tags, clean up the tag index Sets and reverse mappings. Keys are deduplicated across overlapping tags. `SMembers` lookups run concurrently (one goroutine per tag). All deletions are batched in a pipeline.

### `InvalidatePrefix`

```go
func (c *Client) InvalidatePrefix(ctx context.Context, prefix string) error
```

Delete all cache keys whose logical key starts with `prefix` (uses SCAN + DEL).

### `InvalidatePattern`

```go
func (c *Client) InvalidatePattern(ctx context.Context, pattern string) error
```

Delete all cache keys matching a glob pattern (uses SCAN + DEL).

### `CleanTag`

```go
func (c *Client) CleanTag(ctx context.Context, tag string) error
```

Remove stale (expired) key references from a tag set. Useful as a periodic maintenance task — run it as a cron job to prevent tag sets from accumulating garbage.

### `HashRequest`

```go
func (c *Client) HashRequest(req any) (string, error)
```

Deterministic SHA-256 hex string from a request struct. Useful for generating cache keys.

## Passing `nil` for `rdb`

All functions accept `nil` for the Redis client — caching is silently skipped. This makes it easy to conditionally enable Redis without sprinkling `if` checks at every call site.

## Codec

The default codec is JSON. Swap it out:

```go
import "github.com/vmihailenco/msgpack/v5"

cache := goredis.New(rdb, goredis.WithCodec(msgpackCodec{}))
```

Implement the `Codec` interface for any format:

```go
type Codec interface {
    Marshal(any) ([]byte, error)
    Unmarshal([]byte, any) error
}
```

## Metrics

Implement the `Metrics` interface to hook into cache events:

```go
type Metrics interface {
    Hit(key string)
    Miss(key string)
    Set(key string)
    Invalidate(tags []string)
    FetchDuration(key string, d time.Duration)
}
```

## Namespace

Isolate environments or tenants:

```go
cache := goredis.New(rdb, goredis.WithNamespace("prod"))
```

Keys will be prefixed as `prod:<key>`, `prod:tag:<tagname>`, `prod:keytags:<key>`.

## How It Works

1. `CacheSet` marshals the value using the configured codec, then opens a `MULTI/EXEC` transaction to atomically `SET` the key, `SADD` it to each tag's Set, and store the reverse key→tags mapping. The tag Set gets a TTL of `ttl + 1 minute` to automatically clean up stale references.

2. `InvalidateByTags` fans out `SMembers` calls across goroutines (one per tag), deduplicates all referenced cache keys, then issues a pipeline `DEL` for all keys, reverse mappings, and tag Sets.

3. `CacheGetOrFetch` uses `singleflight` to coalesce concurrent misses for the same key — only one goroutine calls the fetch function while the others wait for its result.

4. `CleanTag` checks each member of a tag Set against `EXISTS` and removes stale entries, preventing garbage accumulation over time.
