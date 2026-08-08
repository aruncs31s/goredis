package goredis

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"golang.org/x/sync/singleflight"
)

const DefaultNamespace = "goredis"

var ErrCacheDisabled = errors.New("redis client is nil, caching disabled")

// Client is a tag-based cache invalidation layer over go-redis.
type Client struct {
	rdb       *redis.Client
	codec     Codec
	namespace string
	metrics   Metrics
	sf        singleflight.Group
}

type Option func(*Client)

func WithCodec(codec Codec) Option {
	return func(c *Client) { c.codec = codec }
}

func WithNamespace(ns string) Option {
	return func(c *Client) { c.namespace = ns }
}

func WithMetrics(m Metrics) Option {
	return func(c *Client) { c.metrics = m }
}

func New(rdb *redis.Client, opts ...Option) *Client {
	c := &Client{
		rdb:       rdb,
		codec:     JSONCodec{},
		namespace: DefaultNamespace,
		metrics:   noopMetrics{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *Client) cacheKey(key string) string {
	return c.namespace + ":" + key
}

func (c *Client) tagKey(tag string) string {
	return c.namespace + ":tag:" + tag
}

func (c *Client) keyTagsKey(key string) string {
	return c.namespace + ":keytags:" + key
}

// CacheSet stores a value and registers it under the given tags.
// Uses MULTI/EXEC (TxPipeline) for atomicity across SET, SADD, EXPIRE.
func (c *Client) CacheSet(
	ctx context.Context,
	key string,
	value any,
	ttl time.Duration,
	tags ...string,
) error {
	if c.rdb == nil {
		return nil
	}

	jsonData, err := c.codec.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value for key %s: %w", key, err)
	}
	return c.setRaw(ctx, key, jsonData, ttl, tags...)
}

func (c *Client) setRaw(
	ctx context.Context,
	key string,
	data []byte,
	ttl time.Duration,
	tags ...string,
) error {
	pipe := c.rdb.TxPipeline()
	pipe.Set(ctx, c.cacheKey(key), data, ttl)

	for _, tag := range tags {
		tk := c.tagKey(tag)
		pipe.SAdd(ctx, tk, key)
		pipe.Expire(ctx, tk, ttl+time.Minute)
	}

	if len(tags) > 0 {
		ktk := c.keyTagsKey(key)
		pipe.SAdd(ctx, ktk, tags)
		pipe.Expire(ctx, ktk, ttl+time.Minute)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to cache key %s: %w", key, err)
	}

	c.metrics.Set(key)
	return nil
}

func (c *Client) set(
	ctx context.Context,
	key string,
	value any,
	ttl time.Duration,
	tags ...string,
) error {
	data, err := c.codec.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value for key %s: %w", key, err)
	}
	return c.setRaw(ctx, key, data, ttl, tags...)
}

// CacheSetMany stores multiple values atomically.
func (c *Client) CacheSetMany(
	ctx context.Context,
	items map[string]any,
	ttl time.Duration,
	tags ...string,
) error {
	if c.rdb == nil || len(items) == 0 {
		return nil
	}

	pipe := c.rdb.TxPipeline()

	for key, value := range items {
		jsonData, err := c.codec.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal key %s: %w", key, err)
		}
		pipe.Set(ctx, c.cacheKey(key), jsonData, ttl)
	}

	for _, tag := range tags {
		tk := c.tagKey(tag)
		for key := range items {
			pipe.SAdd(ctx, tk, key)
		}
		pipe.Expire(ctx, tk, ttl+time.Minute)
	}

	if len(tags) > 0 {
		for key := range items {
			ktk := c.keyTagsKey(key)
			pipe.SAdd(ctx, ktk, tags)
			pipe.Expire(ctx, ktk, ttl+time.Minute)
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to cache multiple keys: %w", err)
	}

	return nil
}

func HashRequest(req any) string {
	reqBytes, _ := json.Marshal(req)
	return fmt.Sprintf("%x", sha256.Sum256(reqBytes))
}
