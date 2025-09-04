package proxy

import (
	"NetManager/TableEntryCache"
	"testing"
	"time"
)

// Test that selectBestLocalInstance excludes instances whose effective load exceeds configured threshold.
func TestSelectBestLocalInstance_SkipsOverThreshold(t *testing.T) {
	p := New()
	p.SetLoadThreshold(0.4) // any instance with (cpu+mem)/2 > 0.4 should be skipped
	instances := []TableEntryCache.TableEntry{
		{LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.6, MemoryUsage: 0.6, Timestamp: time.Now().UnixMilli()}}, // avg 0.6 > 0.4 -> skip
		{LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.3, MemoryUsage: 0.3, Timestamp: time.Now().UnixMilli()}}, // avg 0.3 -> eligible
	}
	chosen := p.selectBestLocalInstance(instances)
	if chosen == nil {
		t.Fatalf("expected a local instance to be selected")
	}
	if chosen.LoadMetrics.CpuUsage != 0.3 {
		t.Fatalf("expected lower-load instance chosen, got cpu=%v", chosen.LoadMetrics.CpuUsage)
	}
}

// Test that stale metrics (older than TTL) are heavily penalized relative to fresh metrics and thus not selected when a fresh option exists.
func TestSelectBestLocalInstance_PrefersFreshOverStale(t *testing.T) {
	p := New()
	p.SetMetricsTTL(10) // 10s TTL
	// stale timestamp 30s ago
	staleTs := time.Now().Add(-30 * time.Second).UnixMilli()
	freshTs := time.Now().UnixMilli()
	// Both have identical raw loads so only staleness should differentiate
	instances := []TableEntryCache.TableEntry{
		{LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.2, MemoryUsage: 0.2, ActiveConnections: 10, Timestamp: staleTs}},
		{LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.2, MemoryUsage: 0.2, ActiveConnections: 10, Timestamp: freshTs}},
	}
	chosen := p.selectBestLocalInstance(instances)
	if chosen == nil {
		t.Fatalf("expected selection of a fresh instance")
	}
	if chosen.LoadMetrics.Timestamp != freshTs {
		t.Fatalf("expected fresh instance chosen; got timestamp %d", chosen.LoadMetrics.Timestamp)
	}
}
