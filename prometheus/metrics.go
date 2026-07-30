package prometheus

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Options configures the Prometheus metrics collector.
type Options struct {
	// Namespace and Subsystem for the Prometheus metrics.
	Namespace string
	Subsystem string

	// Registry is the Prometheus registerer to register the collectors.
	// If nil, prometheus.DefaultRegisterer is used.
	Registry prometheus.Registerer

	// KeyMapper maps a raw cache key to a low-cardinality string for labeling.
	// If nil, metrics are aggregated globally and key labels are not registered.
	KeyMapper func(key string) string
}

// Metrics implements the goredis.Metrics interface.
type Metrics struct {
	opts Options

	cacheHits           *prometheus.CounterVec
	cacheMisses          *prometheus.CounterVec
	cacheSets           *prometheus.CounterVec
	cacheInvalidates    *prometheus.CounterVec
	cacheFetchDurations *prometheus.HistogramVec
}

// New creates a new Prometheus Metrics collector and registers it.
func New(opts Options) (*Metrics, error) {
	if opts.Registry == nil {
		opts.Registry = prometheus.DefaultRegisterer
	}

	labels := []string{}
	if opts.KeyMapper != nil {
		labels = []string{"key_group"}
	}

	m := &Metrics{
		opts: opts,
		cacheHits: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: opts.Namespace,
				Subsystem: opts.Subsystem,
				Name:      "goredis_cache_hits_total",
				Help:      "Total number of cache hits.",
			},
			labels,
		),
		cacheMisses: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: opts.Namespace,
				Subsystem: opts.Subsystem,
				Name:      "goredis_cache_misses_total",
				Help:      "Total number of cache misses.",
			},
			labels,
		),
		cacheSets: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: opts.Namespace,
				Subsystem: opts.Subsystem,
				Name:      "goredis_cache_sets_total",
				Help:      "Total number of cache sets.",
			},
			labels,
		),
		cacheInvalidates: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: opts.Namespace,
				Subsystem: opts.Subsystem,
				Name:      "goredis_cache_invalidations_total",
				Help:      "Total number of cache invalidations.",
			},
			[]string{"tag"},
		),
		cacheFetchDurations: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: opts.Namespace,
				Subsystem: opts.Subsystem,
				Name:      "goredis_cache_fetch_duration_seconds",
				Help:      "Duration of source fetches on cache miss.",
				Buckets:   prometheus.DefBuckets,
			},
			labels,
		),
	}

	// Register metrics
	collectors := []prometheus.Collector{
		m.cacheHits,
		m.cacheMisses,
		m.cacheSets,
		m.cacheInvalidates,
		m.cacheFetchDurations,
	}

	for _, c := range collectors {
		if err := opts.Registry.Register(c); err != nil {
			return nil, err
		}
	}

	return m, nil
}

// Hit records a cache hit.
func (m *Metrics) Hit(key string) {
	if m.opts.KeyMapper != nil {
		m.cacheHits.WithLabelValues(m.opts.KeyMapper(key)).Inc()
	} else {
		m.cacheHits.WithLabelValues().Inc()
	}
}

// Miss records a cache miss.
func (m *Metrics) Miss(key string) {
	if m.opts.KeyMapper != nil {
		m.cacheMisses.WithLabelValues(m.opts.KeyMapper(key)).Inc()
	} else {
		m.cacheMisses.WithLabelValues().Inc()
	}
}

// Set records a cache write/update.
func (m *Metrics) Set(key string) {
	if m.opts.KeyMapper != nil {
		m.cacheSets.WithLabelValues(m.opts.KeyMapper(key)).Inc()
	} else {
		m.cacheSets.WithLabelValues().Inc()
	}
}

// Invalidate records invalidation events by tag.
func (m *Metrics) Invalidate(tags []string) {
	for _, tag := range tags {
		m.cacheInvalidates.WithLabelValues(tag).Inc()
	}
}

// FetchDuration records cache fetch duration.
func (m *Metrics) FetchDuration(key string, d time.Duration) {
	secs := d.Seconds()
	if m.opts.KeyMapper != nil {
		m.cacheFetchDurations.WithLabelValues(m.opts.KeyMapper(key)).Observe(secs)
	} else {
		m.cacheFetchDurations.WithLabelValues().Observe(secs)
	}
}
