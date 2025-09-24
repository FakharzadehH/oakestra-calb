package env

import (
	"NetManager/TableEntryCache"
	mqttifce "NetManager/mqtt"
	"net"
	"testing"
)

func TestResponseParser_ClusterIdAndLoadMetrics(t *testing.T) {
	resp := mqttifce.TableQueryResponse{
		JobName: "app.ns.svc.sns",
		InstanceList: []mqttifce.ServiceInstance{
			{
				InstanceNumber: 1,
				NamespaceIp:    "10.18.0.10",
				NamespaceIpv6:  "fc00::10",
				HostIp:         "10.30.0.10",
				HostPort:       15010,
				ServiceIp: []mqttifce.Sip{
					{Type: "RR", Address: "10.30.1.1", Address_v6: "fdff:2000::1"},
					{Type: "ClusterAware", Address: "10.30.2.1", Address_v6: "fdff:2000::2"},
				},
				ClusterId: "cluster_1",
				LoadMetrics: struct {
					CpuUsage          float64 `json:"cpu_usage,omitempty"`
					MemoryUsage       float64 `json:"memory_usage,omitempty"`
					ActiveConnections int     `json:"active_connections,omitempty"`
					Timestamp         int64   `json:"timestamp,omitempty"`
					ClusterId         string  `json:"cluster_id,omitempty"`
				}{CpuUsage: 0.35, MemoryUsage: 0.5, ActiveConnections: 12, ClusterId: "cluster_1"},
			},
		},
		QueryKey: "10.30.2.1",
	}

	entries, err := responseParser(resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	entry := entries[0]
	t.Logf("Parsed entry: clusterId=%s cpu=%.2f mem=%.2f active_connections=%d serviceIPs=%d", entry.ClusterId, entry.LoadMetrics.CpuUsage, entry.LoadMetrics.MemoryUsage, entry.LoadMetrics.ActiveConnections, len(entry.ServiceIP))

	if entry.ClusterId != "cluster_1" {
		t.Errorf("ClusterId not propagated. got=%q", entry.ClusterId)
	}
	if entry.LoadMetrics.CpuUsage != 0.35 || entry.LoadMetrics.MemoryUsage != 0.5 || entry.LoadMetrics.ActiveConnections != 12 {
		t.Errorf("LoadMetrics not propagated. got=%+v", entry.LoadMetrics)
	}

	// Validate ServiceIP mapping and ClusterAware type
	foundCA := false
	for _, sip := range entry.ServiceIP {
		if sip.Address.Equal(net.ParseIP("10.30.2.1")) || sip.Address_v6.Equal(net.ParseIP("fdff:2000::2")) {
			if sip.IpType != TableEntryCache.ClusterAware {
				t.Errorf("expected ClusterAware IpType, got %v", sip.IpType)
			}
			foundCA = true
		}
		// per SIP logging
		ipType := "other"
		if sip.IpType == TableEntryCache.ClusterAware {
			ipType = "ClusterAware"
		}
		if sip.IpType == TableEntryCache.RoundRobin {
			ipType = "RR"
		}
		if sip.IpType == TableEntryCache.Closest {
			ipType = "Closest"
		}
		if sip.IpType == TableEntryCache.InstanceNumber {
			ipType = "InstanceNumber"
		}
		t.Logf("ServiceIP parsed: addr=%s addr_v6=%s type=%s", sip.Address, sip.Address_v6, ipType)
	}
	if !foundCA {
		t.Errorf("ClusterAware ServiceIP not found in parsed entry")
	}
}

func TestToServiceIP_ClusterAwareMapping(t *testing.T) {
	types := []string{"ClusterAware", "cluster_aware_ip", "CLUSTER_AWARE"}
	for _, typ := range types {
		sip := toServiceIP(typ, "10.30.9.9", "fdff:2000::9")
		if sip.IpType != TableEntryCache.ClusterAware {
			t.Errorf("type %q did not map to ClusterAware, got %v", typ, sip.IpType)
		}
		t.Logf("Mapping test: inputType=%s mappedType=%v", typ, sip.IpType)
	}
}
