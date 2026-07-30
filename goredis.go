/*
Package goredis provides a tag-based cache invalidation layer over go-redis.

Usage:

 1. Cache a value with tags – later invalidate all keys sharing a tag at once:

    user, err := goredis.CacheGetOrFetch(ctx, rdb, "user:42", 5*time.Minute,
    func() (*User, error) { return db.GetUser(42) },
    "users", "admins",
    )

 2. Invalidate every cached key tagged "admins" (e.g. after an update):

    goredis.InvalidateByTags(ctx, rdb, "admins")

How it works:
  - Each tag is a Redis Set keyed as "goredis:tag:<tagname>" containing all cache keys
    registered under that tag.
  - CacheSet uses a Redis pipeline to atomically SET the value and SADD the key to each
    tag's Set in a single round-trip.
  - InvalidateByTags fans out SMembers lookups across goroutines (one per tag), collects
    every referenced cache key, DEL them all at once, then cleans up the tag Sets.
  - Pass nil for rdb to disable caching (useful when Redis is not configured).

Functions:

	CacheGetOrFetch[T] – cache-aside pattern with optional tag registration
	CacheGet[T]        – retrieve a cached value
	CacheSet            – store a value and register it under tags
	InvalidateByTags    – delete all cache keys associated with tags
	HashRequest         – generate a deterministic key from a request struct
*/
package goredis

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	logger "github.com/aruncs31s/gologger"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	GoRdisCacheKey = "goredis"
)

func CacheGetOrFetch[T any](
	ctx context.Context,
	rdb *redis.Client,
	key string,
	ttl time.Duration,
	fetch func() (T, error),
	tags ...string,
) (T, error) {
	var zero T

	if val, ok := CacheGet[T](ctx, rdb, key); ok {
		return val, nil
	}

	result, err := fetch()
	if err != nil {
		return zero, err
	}

	if err := CacheSet(ctx, rdb, key, result, ttl, tags...); err != nil {
		logger.GetLogger().Error("error setting cache", zap.String("key", key), zap.Error(err))
	}

	return result, nil
}

func HashRequest(req any) string {
	reqBytes, _ := json.Marshal(req)
	return fmt.Sprintf("%x", sha256.Sum256(reqBytes))
}

func tagKey(tag string) string {
	return GoRdisCacheKey + ":tag:" + tag
}

func CacheSet(
	ctx context.Context,
	rdb *redis.Client,
	key string,
	value any,
	ttl time.Duration,
	tags ...string,
) error {
	if rdb == nil {
		return nil
	}

	jsonData, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value for key %s: %w", key, err)
	}

	pipe := rdb.Pipeline()

	pipe.Set(ctx, key, jsonData, ttl)

	for _, tag := range tags {
		tk := tagKey(tag)
		pipe.SAdd(ctx, tk, key)
		pipe.Expire(ctx, tk, ttl+time.Minute)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		logger.GetLogger().Error("failed to cache with tags", zap.String("key", key), zap.Error(err))
		return err
	}

	return nil
}

func CacheGet[T any](ctx context.Context, rdb *redis.Client, key string) (T, bool) {
	var zero T

	if rdb == nil {
		return zero, false
	}

	data, err := rdb.Get(ctx, key).Result()
	if err != nil || data == "" {
		return zero, false
	}

	var result T
	if json.Unmarshal([]byte(data), &result) != nil {
		return zero, false
	}

	return result, true
}

func InvalidateByTags(ctx context.Context, rdb *redis.Client, tags ...string) error {
	if rdb == nil {
		return nil
	}

	var mu sync.Mutex
	var allKeys []string
	var wg sync.WaitGroup

	for _, tag := range tags {
		wg.Add(1)
		go func(tag string) {
			defer wg.Done()

			keys, err := rdb.SMembers(ctx, tagKey(tag)).Result()
			if err != nil {
				logger.GetLogger().Error("failed to fetch tag members", zap.String("tag", tag), zap.Error(err))
				return
			}

			mu.Lock()
			allKeys = append(allKeys, keys...)
			mu.Unlock()
		}(tag)
	}

	wg.Wait()

	if len(allKeys) > 0 {
		if err := rdb.Del(ctx, allKeys...).Err(); err != nil {
			logger.GetLogger().Error("failed to delete cached keys", zap.Error(err))
		}
	}

	for _, tag := range tags {
		rdb.Del(ctx, tagKey(tag))
	}

	return nil
}
