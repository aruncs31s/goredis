package goredis

import (
	"context"
	"time"
)

// CacheGetOrFetch retrieves a value by key, calling fetch on miss.
// Concurrent requests for the same key are coalesced using singleflight
// to prevent cache stampedes.
func CacheGetOrFetch[T any](
	ctx context.Context,
	c *Client,
	key string,
	ttl time.Duration,
	fetch func() (T, error),
	tags ...string,
) (T, error) {
	var zero T
	if c.rdb == nil {
		return fetch()
	}

	if val, ok := CacheGet[T](ctx, c, key); ok {
		c.metrics.Hit(key)
		return val, nil
	}
	c.metrics.Miss(key)

	ch := c.sf.DoChan(key, func() (any, error) {
		if val, ok := CacheGet[T](ctx, c, key); ok {
			c.metrics.Hit(key)
			return val, nil
		}

		start := time.Now()
		val, err := fetch()
		c.metrics.FetchDuration(key, time.Since(start))
		if err != nil {
			return zero, err
		}

		if err := c.set(ctx, key, val, ttl, tags...); err != nil {
			return zero, err
		}
		return val, nil
	})

	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case result := <-ch:
		if result.Err != nil {
			return zero, result.Err
		}
		return result.Val.(T), nil
	}
}

// CacheGet retrieves a cached value.
func CacheGet[T any](ctx context.Context, c *Client, key string) (T, bool) {
	var zero T
	if c.rdb == nil {
		return zero, false
	}

	data, err := c.rdb.Get(ctx, c.cacheKey(key)).Result()
	if err != nil || data == "" {
		return zero, false
	}

	var result T
	if err := c.codec.Unmarshal([]byte(data), &result); err != nil {
		return zero, false
	}
	return result, true
}

// CacheGetMany retrieves multiple values at once via MGET.
func CacheGetMany[T any](ctx context.Context, c *Client, keys ...string) map[string]T {
	result := make(map[string]T)
	if c.rdb == nil || len(keys) == 0 {
		return result
	}

	cacheKeys := make([]string, len(keys))
	for i, k := range keys {
		cacheKeys[i] = c.cacheKey(k)
	}

	data, err := c.rdb.MGet(ctx, cacheKeys...).Result()
	if err != nil {
		return result
	}

	for i, raw := range data {
		if raw == nil {
			continue
		}
		str, ok := raw.(string)
		if !ok || str == "" {
			continue
		}
		var val T
		if err := c.codec.Unmarshal([]byte(str), &val); err != nil {
			continue
		}
		result[keys[i]] = val
	}
	return result
}
