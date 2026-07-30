package goredis

import "time"

// Metrics hooks for observability.
type Metrics interface {
	Hit(key string)
	Miss(key string)
	Set(key string)
	Invalidate(tags []string)
	FetchDuration(key string, d time.Duration)
}

type noopMetrics struct{}

func (noopMetrics) Hit(string)                           {}
func (noopMetrics) Miss(string)                          {}
func (noopMetrics) Set(string)                           {}
func (noopMetrics) Invalidate([]string)                   {}
func (noopMetrics) FetchDuration(string, time.Duration) {}
