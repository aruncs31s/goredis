package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aruncs31s/goredis"
	goredisprom "github.com/aruncs31s/goredis/prometheus"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var redisClient *redis.Client

func init() {
	redisClient = redis.NewClient(
		&redis.Options{
			Addr:     "localhost:8998",
			Password: "greenIsBest",
		},
	)
}

// Product represents the data we are caching.
type Product struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

func main() {
	ctx := context.Background()

	// 1. Create the Prometheus metrics adapter.
	// We'll define a KeyMapper to group cache key metrics by their prefix
	// to avoid high cardinality issues (e.g. product:123, product:456 -> product).
	promMetrics, err := goredisprom.New(goredisprom.Options{
		Namespace: "myapp",
		Subsystem: "cache",
		Registry:  prometheus.DefaultRegisterer,
		KeyMapper: func(key string) string {
			if strings.HasPrefix(key, "product:") {
				return "product"
			}
			if strings.HasPrefix(key, "user:") {
				return "user"
			}
			return "other"
		},
	})
	if err != nil {
		log.Fatalf("Failed to initialize Prometheus metrics: %v", err)
	}

	// 2. Initialize the goredis client with Prometheus metrics.
	cache := goredis.New(redisClient, goredis.WithMetrics(promMetrics))

	// 3. Simulate cache hits, misses, sets, and invalidations in a background goroutine.
	go func() {
		for {
			log.Println("Simulating cache operations...")

			// Miss & Fetch
			_, _ = goredis.CacheGetOrFetch(ctx, cache, "product:101", 5*time.Minute, func() (*Product, error) {
				time.Sleep(50 * time.Millisecond) // simulate origin fetch duration
				return &Product{ID: "101", Name: "Premium Gadget", Price: 99.99}, nil
			}, "products")

			// Hit
			_, _ = goredis.CacheGetOrFetch(ctx, cache, "product:101", 5*time.Minute, func() (*Product, error) {
				return nil, fmt.Errorf("should not be called on cache hit")
			}, "products")

			// Invalidating tag
			_ = cache.InvalidateByTags(ctx, "products")

			time.Sleep(2 * time.Second)
		}
	}()

	// 4. Start HTTP server to expose Prometheus metrics.
	http.Handle("/metrics", promhttp.Handler())
	log.Println("Prometheus metrics server starting on :8080/metrics")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}
