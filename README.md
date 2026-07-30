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
SET cache:user:42      → "goredis:tag:users"    SADD cache:user:42
SET cache:user:list    → "goredis:tag:users"    SADD cache:user:list
SET cache:stats        → "goredis:tag:dashboard" SADD cache:stats
                         "goredis:tag:users"    SADD cache:stats

InvalidateByTags(ctx, rdb, "users")
    → deletes: cache:user:42, cache:user:list, cache:stats
    → deletes: goredis:tag:users
```

## Installation

```
go get github.com/aruncs31s/goredis
```

## Quick Start

```go
import (
    "time"
    "github.com/redis/go-redis/v9"
    "github.com/aruncs31s/goredis"
)

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

type User struct {
    ID   int    `json:"id"`
    Name string `json:"name"`
}

// Cache a user under the "users" tag
user, err := goredis.CacheGetOrFetch(ctx, rdb, "user:42", 5*time.Minute,
    func() (*User, error) {
        return &User{ID: 42, Name: "Alice"}, nil
    },
    "users",
)

// After a user update, clear all cached data tagged "users"
goredis.InvalidateByTags(ctx, rdb, "users")
```

## API

### `CacheGetOrFetch[T]`

```go
func CacheGetOrFetch[T any](
    ctx context.Context,
    rdb *redis.Client,
    key string,
    ttl time.Duration,
    fetch func() (T, error),
    tags ...string,
) (T, error)
```

Cache-aside pattern. Returns cached value if found, otherwise calls `fetch`, stores the result, and registers the key under the given tags. Pass no tags for plain caching without invalidation tracking.

### `CacheGet[T]`

```go
func CacheGet[T any](ctx context.Context, rdb *redis.Client, key string) (T, bool)
```

Low-level get. Returns the value and `true` on cache hit.

### `CacheSet`

```go
func CacheSet(ctx context.Context, rdb *redis.Client, key string, value any, ttl time.Duration, tags ...string) error
```

Store a value and register it under the given tags. Uses a Redis pipeline for atomicity.

### `InvalidateByTags`

```go
func InvalidateByTags(ctx context.Context, rdb *redis.Client, tags ...string) error
```

Delete every cache key registered under the given tags, then clean up the tag index Sets. `SMembers` lookups run concurrently (one goroutine per tag).

### `HashRequest`

```go
func HashRequest(req any) string
```

Deterministic SHA-256 hex string from a request struct. Useful for generating cache keys.

## Passing `nil` for `rdb`

All functions accept `nil` for the Redis client — caching is silently skipped. This makes it easy to conditionally enable Redis without sprinkling `if` checks at every call site.

## How It Works

1. `CacheSet` marshals the value to JSON, then opens a pipeline to atomically `SET` the key and `SADD` it to each tag's Redis Set. The tag Set gets a TTL of `ttl + 1 minute` to automatically clean up stale references.

2. `InvalidateByTags` fans out `SMembers` calls across goroutines (one per tag), collects all referenced cache keys, issues a single `DEL` for all of them, then deletes the tag Sets.
