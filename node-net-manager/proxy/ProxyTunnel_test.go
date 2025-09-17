package proxy

import (
	"NetManager/TableEntryCache"
	"net"
	"testing"
)

type fakeEnv struct {
	entriesBySIP map[string][]TableEntryCache.TableEntry
	entriesByNs  map[string]TableEntryCache.TableEntry
}

func (f *fakeEnv) GetTableEntryByServiceIP(ip net.IP) []TableEntryCache.TableEntry {
	return f.entriesBySIP[ip.String()]
}
func (f *fakeEnv) GetTableEntryByNsIP(ip net.IP) (TableEntryCache.TableEntry, bool) {
	e, ok := f.entriesByNs[ip.String()]
	return e, ok
}
func (f *fakeEnv) GetTableEntryByInstanceIP(ip net.IP) (TableEntryCache.TableEntry, bool) {
	return TableEntryCache.TableEntry{}, false
}
func (f *fakeEnv) RefreshServiceTable(jobname string) {}

func TestGetClusterAwareDestination_PrefersLocalBelowThreshold(t *testing.T) {
	p := New()
	p.SetClusterAwareRouting(true)
	p.SetLocalClusterId("cluster_1")
	p.SetLoadThreshold(0.6)

	svcIP := net.ParseIP("10.30.2.1")
	localNs := net.ParseIP("10.18.0.10")
	remoteNs := net.ParseIP("10.18.0.20")

	entries := []TableEntryCache.TableEntry{
		{JobName: "a.b.c.d", Nsip: localNs, Nsipv6: net.ParseIP("fc00::10"), ClusterId: "cluster_1", LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.3, MemoryUsage: 0.4}},
		{JobName: "a.b.c.d", Nsip: remoteNs, Nsipv6: net.ParseIP("fc00::20"), ClusterId: "cluster_2", LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.1, MemoryUsage: 0.2}},
	}

	fe := &fakeEnv{entriesBySIP: map[string][]TableEntryCache.TableEntry{svcIP.String(): entries}}
	p.SetEnvironment(fe)

	dst, err := p.getClusterAwareDestination(svcIP, "cluster_1", false)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// Logging
	t.Logf("PrefersLocalBelowThreshold: svcIP=%s chosen=%s local=%s remote=%s threshold=%.2f localLoad(cpu%.2f/mem%.2f) remoteLoad(cpu%.2f/mem%.2f)", svcIP, dst, localNs, remoteNs, p.loadThreshold, entries[0].LoadMetrics.CpuUsage, entries[0].LoadMetrics.MemoryUsage, entries[1].LoadMetrics.CpuUsage, entries[1].LoadMetrics.MemoryUsage)
	if !dst.Equal(localNs) {
		t.Fatalf("expected local nsip %s, got %s", localNs, dst)
	}
}

func TestGetClusterAwareDestination_FallbackToRemoteWhenOverThreshold(t *testing.T) {
	p := New()
	p.SetClusterAwareRouting(true)
	p.SetLocalClusterId("cluster_1")
	p.SetLoadThreshold(0.3)

	svcIP := net.ParseIP("10.30.2.1")
	localNs := net.ParseIP("10.18.0.10")
	remoteNs := net.ParseIP("10.18.0.20")

	entries := []TableEntryCache.TableEntry{
		{JobName: "a.b.c.d", Nsip: localNs, Nsipv6: net.ParseIP("fc00::10"), ClusterId: "cluster_1", LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.5, MemoryUsage: 0.6}},
		{JobName: "a.b.c.d", Nsip: remoteNs, Nsipv6: net.ParseIP("fc00::20"), ClusterId: "cluster_2", LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.2, MemoryUsage: 0.2}},
	}

	fe := &fakeEnv{entriesBySIP: map[string][]TableEntryCache.TableEntry{svcIP.String(): entries}}
	p.SetEnvironment(fe)

	dst, err := p.getClusterAwareDestination(svcIP, "cluster_1", false)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	t.Logf("FallbackToRemoteWhenOverThreshold: svcIP=%s chosen=%s local=%s remote=%s threshold=%.2f localLoad(cpu%.2f/mem%.2f) remoteLoad(cpu%.2f/mem%.2f)", svcIP, dst, localNs, remoteNs, p.loadThreshold, entries[0].LoadMetrics.CpuUsage, entries[0].LoadMetrics.MemoryUsage, entries[1].LoadMetrics.CpuUsage, entries[1].LoadMetrics.MemoryUsage)
	if !dst.Equal(remoteNs) {
		t.Fatalf("expected remote nsip %s, got %s", remoteNs, dst)
	}
}

func TestToServiceIP_ClusterAwareSelection_UsesIPv6WhenPreferred(t *testing.T) {
	p := New()
	p.SetClusterAwareRouting(true)
	p.SetLocalClusterId("cluster_1")
	p.SetLoadThreshold(1.0)

	svcIP := net.ParseIP("fdff:2000::2")
	localNs := net.ParseIP("10.18.0.10")
	localNs6 := net.ParseIP("fc00::10")

	entries := []TableEntryCache.TableEntry{
		{JobName: "a.b.c.d", Nsip: localNs, Nsipv6: localNs6, ClusterId: "cluster_1", LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.1, MemoryUsage: 0.1}},
	}

	fe := &fakeEnv{entriesBySIP: map[string][]TableEntryCache.TableEntry{svcIP.String(): entries}}
	p.SetEnvironment(fe)

	dst, err := p.getClusterAwareDestination(svcIP, "cluster_1", true)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	t.Logf("IPv6Preference: svcIP=%s chosen=%s expectedIPv6=%s cpu=%.2f mem=%.2f", svcIP, dst, localNs6, entries[0].LoadMetrics.CpuUsage, entries[0].LoadMetrics.MemoryUsage)
	if !dst.Equal(localNs6) {
		t.Fatalf("expected local nsipv6 %s, got %s", localNs6, dst)
	}
}
