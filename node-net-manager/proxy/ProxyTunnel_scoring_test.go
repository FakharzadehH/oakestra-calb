package proxy

import (
	"NetManager/TableEntryCache"
	"net"
	"testing"
	"time"
)

// minimal fake environment implementing needed method
type fakeEnvForScore struct{ entry TableEntryCache.TableEntry }

func (f *fakeEnvForScore) GetTableEntryByServiceIP(ip net.IP) []TableEntryCache.TableEntry {
	return nil
}
func (f *fakeEnvForScore) GetTableEntryByNsIP(ip net.IP) (TableEntryCache.TableEntry, bool) {
	return TableEntryCache.TableEntry{}, false
}
func (f *fakeEnvForScore) GetTableEntryByInstanceIP(ip net.IP) (TableEntryCache.TableEntry, bool) {
	return TableEntryCache.TableEntry{}, false
}
func (f *fakeEnvForScore) RefreshServiceTable(jobname string) {}

func TestCalculateInstanceScore_WeightsApplied(t *testing.T) {
	p := New()
	p.SetWeights(0.5, 0.3, 0.2)
	inst := TableEntryCache.TableEntry{LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.2, MemoryUsage: 0.4, ActiveConnections: 20, Timestamp: time.Now().UnixMilli()}}
	score := p.calculateInstanceScore(&inst)
	if score <= 0 || score > 1 {
		t.Fatalf("unexpected score range: %f", score)
	}
}

func TestCalculateInstanceScore_StaleMetricsPenalized(t *testing.T) {
	p := New()
	p.SetWeights(0.4, 0.4, 0.2)
	p.SetMetricsTTL(10) // 10s TTL
	staleTs := time.Now().Add(-30 * time.Second).UnixMilli()
	inst := TableEntryCache.TableEntry{LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.1, MemoryUsage: 0.1, ActiveConnections: 5, Timestamp: staleTs}}
	score := p.calculateInstanceScore(&inst)
	if score > 0.001 {
		t.Fatalf("expected heavy penalty for stale metrics, got %f", score)
	}
}

func TestCalculateInstanceScore_DefaultWeightsFallback(t *testing.T) {
	p := New()
	// Intentionally set zero weights to trigger fallback
	p.SetWeights(0, 0, 0)
	inst := TableEntryCache.TableEntry{LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.5, MemoryUsage: 0.5, ActiveConnections: 50, Timestamp: time.Now().UnixMilli()}}
	score := p.calculateInstanceScore(&inst)
	if score <= 0 {
		t.Fatalf("expected positive score with fallback weights, got %f", score)
	}
}
