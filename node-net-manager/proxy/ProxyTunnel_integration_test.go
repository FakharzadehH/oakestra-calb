package proxy

import (
    "NetManager/TableEntryCache"
    "net"
    "os"
    "testing"
    "time"
)

// reusable fake environment keyed by service IP string
type integFakeEnv struct {
    entriesBySIP map[string][]TableEntryCache.TableEntry
}

func (f *integFakeEnv) GetTableEntryByServiceIP(ip net.IP) []TableEntryCache.TableEntry { return f.entriesBySIP[ip.String()] }
func (f *integFakeEnv) GetTableEntryByNsIP(ip net.IP) (TableEntryCache.TableEntry, bool) { return TableEntryCache.TableEntry{}, false }
func (f *integFakeEnv) GetTableEntryByInstanceIP(ip net.IP) (TableEntryCache.TableEntry, bool) { return TableEntryCache.TableEntry{}, false }
func (f *integFakeEnv) RefreshServiceTable(jobname string)                               {}

// 1. Mixed local / remote instances: ensure local preferred when under threshold and fresher
func TestClusterAware_MixedLocalRemoteSelection(t *testing.T) {
    os.Setenv("TEST_LIGHTWEIGHT_PROXY", "1")
    p := New()
    p.SetClusterAwareRouting(true)
    p.SetLocalClusterId("cluster_local")
    p.SetLoadThreshold(0.7) // threshold high enough so local healthy instance considered
    p.SetWeights(0.4, 0.4, 0.2)
    svcIP := net.ParseIP("10.250.0.1")

    localGood := TableEntryCache.TableEntry{JobName: "a.b.c.d", Nsip: net.ParseIP("10.18.0.11"), Nsipv6: net.ParseIP("fc00::11"), ClusterId: "cluster_local", LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.30, MemoryUsage: 0.25, ActiveConnections: 10, Timestamp: time.Now().UnixMilli()}}
    localOver := TableEntryCache.TableEntry{JobName: "a.b.c.d", Nsip: net.ParseIP("10.18.0.12"), Nsipv6: net.ParseIP("fc00::12"), ClusterId: "cluster_local", LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.95, MemoryUsage: 0.90, ActiveConnections: 50, Timestamp: time.Now().UnixMilli()}}
    remote := TableEntryCache.TableEntry{JobName: "a.b.c.d", Nsip: net.ParseIP("10.28.0.21"), Nsipv6: net.ParseIP("fc00::21"), ClusterId: "cluster_remote", LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.20, MemoryUsage: 0.40, ActiveConnections: 5, Timestamp: time.Now().UnixMilli()}}

    fe := &integFakeEnv{entriesBySIP: map[string][]TableEntryCache.TableEntry{svcIP.String(): {localGood, localOver, remote}}}
    p.SetEnvironment(fe)

    dst, err := p.getClusterAwareDestination(svcIP, "cluster_local", false)
    if err != nil { t.Fatalf("unexpected error: %v", err) }
    if !dst.Equal(localGood.Nsip) { t.Fatalf("expected healthy local instance %s, got %s", localGood.Nsip, dst) }
}

// 2. Staleness rollover: local instance metrics stale -> remote chosen
func TestClusterAware_StaleLocalFallsBackToRemote(t *testing.T) {
    os.Setenv("TEST_LIGHTWEIGHT_PROXY", "1")
    p := New()
    p.SetClusterAwareRouting(true)
    p.SetLocalClusterId("cluster_local")
    p.SetLoadThreshold(0.9) // do not exclude local by threshold
    p.SetMetricsTTL(15)     // 15s TTL
    p.SetWeights(0.4, 0.4, 0.2)
    svcIP := net.ParseIP("10.250.0.2")

    staleTs := time.Now().Add(-40 * time.Second).UnixMilli() // older than TTL
    freshTs := time.Now().UnixMilli()
    // Make local resource-wise clearly better (very low load) so without staleness it would win
    localStale := TableEntryCache.TableEntry{JobName: "a.b.c.d", Nsip: net.ParseIP("10.18.0.13"), Nsipv6: net.ParseIP("fc00::13"), ClusterId: "cluster_local", LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.05, MemoryUsage: 0.05, ActiveConnections: 1, Timestamp: staleTs}}
    // Remote is moderately loaded; should only win because local metrics are stale
    remoteFresh := TableEntryCache.TableEntry{JobName: "a.b.c.d", Nsip: net.ParseIP("10.28.0.22"), Nsipv6: net.ParseIP("fc00::22"), ClusterId: "cluster_remote", LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.40, MemoryUsage: 0.40, ActiveConnections: 20, Timestamp: freshTs}}

    fe := &integFakeEnv{entriesBySIP: map[string][]TableEntryCache.TableEntry{svcIP.String(): {localStale, remoteFresh}}}
    p.SetEnvironment(fe)

    dst, err := p.getClusterAwareDestination(svcIP, "cluster_local", false)
    if err != nil { t.Fatalf("unexpected error: %v", err) }
    if !dst.Equal(remoteFresh.Nsip) { t.Fatalf("expected remote fallback %s due to stale local metrics, got %s", remoteFresh.Nsip, dst) }
}

// 3. Weights influence selection between local instances only
func TestClusterAware_WeightInfluenceLocalChoice(t *testing.T) {
    os.Setenv("TEST_LIGHTWEIGHT_PROXY", "1")
    p := New()
    p.SetClusterAwareRouting(true)
    p.SetLocalClusterId("cluster_local")
    p.SetLoadThreshold(1.0) // no threshold exclusion
    p.SetMetricsTTL(60)     // generous TTL
    svcIP := net.ParseIP("10.250.0.3")

    now := time.Now().UnixMilli()
    // Instance A: better CPU, worse Memory
    instA := TableEntryCache.TableEntry{JobName: "a.b.c.d", Nsip: net.ParseIP("10.18.0.14"), Nsipv6: net.ParseIP("fc00::14"), ClusterId: "cluster_local", LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.20, MemoryUsage: 0.80, ActiveConnections: 10, Timestamp: now}}
    // Instance B: worse CPU, better Memory
    instB := TableEntryCache.TableEntry{JobName: "a.b.c.d", Nsip: net.ParseIP("10.18.0.15"), Nsipv6: net.ParseIP("fc00::15"), ClusterId: "cluster_local", LoadMetrics: TableEntryCache.LoadMetrics{CpuUsage: 0.60, MemoryUsage: 0.20, ActiveConnections: 10, Timestamp: now}}

    fe := &integFakeEnv{entriesBySIP: map[string][]TableEntryCache.TableEntry{svcIP.String(): {instA, instB}}}
    p.SetEnvironment(fe)

    // Emphasize CPU -> expect instA
    p.SetWeights(0.7, 0.2, 0.1)
    dst1, err := p.getClusterAwareDestination(svcIP, "cluster_local", false)
    if err != nil { t.Fatalf("unexpected error (cpu weight phase): %v", err) }
    if !dst1.Equal(instA.Nsip) { t.Fatalf("expected CPU-favored instance A %s, got %s", instA.Nsip, dst1) }

    // Emphasize Memory -> expect instB
    p.SetWeights(0.2, 0.7, 0.1)
    dst2, err := p.getClusterAwareDestination(svcIP, "cluster_local", false)
    if err != nil { t.Fatalf("unexpected error (memory weight phase): %v", err) }
    if !dst2.Equal(instB.Nsip) { t.Fatalf("expected Memory-favored instance B %s, got %s", instB.Nsip, dst2) }
}
