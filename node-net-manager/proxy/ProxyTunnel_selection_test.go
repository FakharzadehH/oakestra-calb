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
	for i, inst := range instances { avg := (inst.LoadMetrics.CpuUsage + inst.LoadMetrics.MemoryUsage)/2; t.Logf("Instance[%d] cpu=%.2f mem=%.2f avg=%.2f", i, inst.LoadMetrics.CpuUsage, inst.LoadMetrics.MemoryUsage, avg) }
	chosen := p.selectBestLocalInstance(instances)
	if chosen == nil {
		t.Fatalf("expected a local instance to be selected")
	}
	avgChosen := (chosen.LoadMetrics.CpuUsage + chosen.LoadMetrics.MemoryUsage)/2
	t.Logf("Chosen cpu=%.2f mem=%.2f avg=%.2f threshold=%.2f", chosen.LoadMetrics.CpuUsage, chosen.LoadMetrics.MemoryUsage, avgChosen, p.loadThreshold)
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
	for i, inst := range instances { age := time.Since(time.UnixMilli(inst.LoadMetrics.Timestamp)); t.Logf("Instance[%d] ts=%d age=%s cpu=%.2f mem=%.2f", i, inst.LoadMetrics.Timestamp, age, inst.LoadMetrics.CpuUsage, inst.LoadMetrics.MemoryUsage) }
	chosen := p.selectBestLocalInstance(instances)
	if chosen == nil {
		t.Fatalf("expected selection of a fresh instance")
	}
	chosenAge := time.Since(time.UnixMilli(chosen.LoadMetrics.Timestamp))
	t.Logf("Chosen ts=%d age=%s", chosen.LoadMetrics.Timestamp, chosenAge)
	if chosen.LoadMetrics.Timestamp != freshTs {
		t.Fatalf("expected fresh instance chosen; got timestamp %d", chosen.LoadMetrics.Timestamp)
	}
}
