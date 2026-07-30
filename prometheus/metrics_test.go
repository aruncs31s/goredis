package prometheus

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsWithoutKeyMapper(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(Options{
		Namespace: "test",
		Subsystem: "cache",
		Registry:  reg,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m.Hit("some-key")
	m.Hit("another-key")
	m.Miss("some-key")
	m.Set("some-key")
	m.Invalidate([]string{"tag1", "tag2"})
	m.FetchDuration("some-key", 250*time.Millisecond)

	metricFamilies, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather error: %v", err)
	}

	foundMetrics := make(map[string]float64)
	var foundDurationCount uint64
	var foundDurationSum float64

	for _, family := range metricFamilies {
		name := family.GetName()
		for _, metric := range family.GetMetric() {
			if name == "test_cache_goredis_cache_invalidations_total" {
				var tag string
				for _, label := range metric.GetLabel() {
					if label.GetName() == "tag" {
						tag = label.GetValue()
					}
				}
				foundMetrics["invalidate_"+tag] = metric.GetCounter().GetValue()
			} else if name == "test_cache_goredis_cache_hits_total" {
				foundMetrics["hits"] = metric.GetCounter().GetValue()
			} else if name == "test_cache_goredis_cache_misses_total" {
				foundMetrics["misses"] = metric.GetCounter().GetValue()
			} else if name == "test_cache_goredis_cache_sets_total" {
				foundMetrics["sets"] = metric.GetCounter().GetValue()
			} else if name == "test_cache_goredis_cache_fetch_duration_seconds" {
				foundDurationCount = metric.GetHistogram().GetSampleCount()
				foundDurationSum = metric.GetHistogram().GetSampleSum()
			}
		}
	}

	if foundMetrics["hits"] != 2 {
		t.Errorf("expected 2 hits, got %f", foundMetrics["hits"])
	}
	if foundMetrics["misses"] != 1 {
		t.Errorf("expected 1 miss, got %f", foundMetrics["misses"])
	}
	if foundMetrics["sets"] != 1 {
		t.Errorf("expected 1 set, got %f", foundMetrics["sets"])
	}
	if foundMetrics["invalidate_tag1"] != 1 {
		t.Errorf("expected 1 invalidate for tag1, got %f", foundMetrics["invalidate_tag1"])
	}
	if foundMetrics["invalidate_tag2"] != 1 {
		t.Errorf("expected 1 invalidate for tag2, got %f", foundMetrics["invalidate_tag2"])
	}
	if foundDurationCount != 1 {
		t.Errorf("expected 1 fetch duration measurement, got %d", foundDurationCount)
	}
	if foundDurationSum != 0.25 {
		t.Errorf("expected fetch duration sum of 0.25 seconds, got %f", foundDurationSum)
	}
}

func TestMetricsWithKeyMapper(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(Options{
		Namespace: "test",
		Subsystem: "cache",
		Registry:  reg,
		KeyMapper: func(key string) string {
			if strings.HasPrefix(key, "user:") {
				return "user"
			}
			return "other"
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m.Hit("user:123")
	m.Hit("user:456")
	m.Hit("product:999")

	metricFamilies, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather error: %v", err)
	}

	userCount := 0.0
	otherCount := 0.0

	for _, family := range metricFamilies {
		if family.GetName() == "test_cache_goredis_cache_hits_total" {
			for _, metric := range family.GetMetric() {
				var keyGroup string
				for _, label := range metric.GetLabel() {
					if label.GetName() == "key_group" {
						keyGroup = label.GetValue()
					}
				}
				if keyGroup == "user" {
					userCount = metric.GetCounter().GetValue()
				} else if keyGroup == "other" {
					otherCount = metric.GetCounter().GetValue()
				}
			}
		}
	}

	if userCount != 2 {
		t.Errorf("expected 2 hits for 'user' key group, got %f", userCount)
	}
	if otherCount != 1 {
		t.Errorf("expected 1 hit for 'other' key group, got %f", otherCount)
	}
}
