package goredis

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-redis/redis/v8"
)

// InvalidateByTags deletes all cache keys associated with the given tags.
// Keys are deduplicated across tags, and all deletions are batched in a pipeline.
func (c *Client) InvalidateByTags(ctx context.Context, tags ...string) error {
	if c.rdb == nil || len(tags) == 0 {
		return nil
	}

	var mu sync.Mutex
	keysSet := make(map[string]struct{})
	var wg sync.WaitGroup

	for _, tag := range tags {
		wg.Add(1)
		go func(tag string) {
			defer wg.Done()

			members, err := c.rdb.SMembers(ctx, c.tagKey(tag)).Result()
			if err != nil {
				return
			}

			mu.Lock()
			for _, k := range members {
				keysSet[k] = struct{}{}
			}
			mu.Unlock()
		}(tag)
	}

	wg.Wait()

	keys := make([]string, 0, len(keysSet))
	for k := range keysSet {
		keys = append(keys, k)
	}

	pipe := c.rdb.Pipeline()
	for _, k := range keys {
		pipe.Del(ctx, c.cacheKey(k))
		pipe.Del(ctx, c.keyTagsKey(k))
	}
	for _, tag := range tags {
		pipe.Del(ctx, c.tagKey(tag))
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to invalidate tags %v: %w", tags, err)
	}

	c.metrics.Invalidate(tags)
	return nil
}

// InvalidatePrefix deletes all cache keys with the given key prefix.
func (c *Client) InvalidatePrefix(ctx context.Context, prefix string) error {
	if c.rdb == nil {
		return nil
	}
	return c.scanDelete(ctx, c.cacheKey(prefix)+"*")
}

// InvalidatePattern deletes all cache keys matching the given glob pattern.
func (c *Client) InvalidatePattern(ctx context.Context, pattern string) error {
	if c.rdb == nil {
		return nil
	}
	return c.scanDelete(ctx, c.cacheKey(pattern))
}

func (c *Client) scanDelete(ctx context.Context, pattern string) error {
	iter := c.rdb.Scan(ctx, 0, pattern, 0).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("scan failed for pattern %s: %w", pattern, err)
	}
	if len(keys) > 0 {
		if err := c.rdb.Del(ctx, keys...).Err(); err != nil {
			return fmt.Errorf("delete failed for pattern %s: %w", pattern, err)
		}
	}
	return nil
}

// CleanTag removes stale key references from a tag set by checking
// whether each member still exists in the cache. Useful as a periodic
// maintenance task to prevent tag sets from accumulating garbage.
func (c *Client) CleanTag(ctx context.Context, tag string) error {
	if c.rdb == nil {
		return nil
	}

	members, err := c.rdb.SMembers(ctx, c.tagKey(tag)).Result()
	if err != nil {
		return fmt.Errorf("failed to get tag members for %s: %w", tag, err)
	}

	if len(members) == 0 {
		return nil
	}

	cmds, err := c.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, member := range members {
			pipe.Exists(ctx, c.cacheKey(member))
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to check key existence for tag %s: %w", tag, err)
	}

	var stale []string
	for i, member := range members {
		if i < len(cmds) {
			if existsCmd, ok := cmds[i].(*redis.IntCmd); ok {
				if existsCmd.Val() == 0 {
					stale = append(stale, member)
				}
			}
		}
	}

	if len(stale) > 0 {
		pipe := c.rdb.Pipeline()
		pipe.SRem(ctx, c.tagKey(tag), stale)
		for _, member := range stale {
			pipe.Del(ctx, c.keyTagsKey(member))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("failed to clean stale keys from tag %s: %w", tag, err)
		}
	}

	return nil
}
