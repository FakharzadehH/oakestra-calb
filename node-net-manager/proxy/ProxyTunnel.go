package proxy

import (
	"NetManager/TableEntryCache"
	"NetManager/env"
	"NetManager/logger"
	"NetManager/proxy/iputils"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/songgao/water"
)

// const
var BUFFER_SIZE = 64 * 1024

// Config
type Configuration struct {
	HostTUNDeviceName         string `json:"HostTunnelDeviceName"`
	ProxySubnetwork           string `json:"ProxySubnetwork"`
	ProxySubnetworkMask       string `json:"ProxySubnetworkMask"`
	TunNetIP                  string `json:"TunnelIP"`
	TunnelPort                int    `json:"TunnelPort"`
	Mtusize                   int    `json:"MTUSize"`
	TunNetIPv6                string `json:"TunNetIPv6"`
	ProxySubnetworkIPv6       string `json:"ProxySubnetworkIPv6"`
	ProxySubnetworkIPv6Prefix int    `json:"ProxySubnetworkIPv6Prefix"`
}

type GoProxyTunnel struct {
	environment         env.EnvironmentManager
	listenConnection    *net.UDPConn
	incomingChannel     chan incomingMessage
	connectionBuffer    map[string]*net.UDPConn
	randseed            *rand.Rand
	ifce                *water.Interface
	outgoingChannel     chan outgoingMessage
	finishChannel       chan bool
	errorChannel        chan error
	stopChannel         chan bool
	HostTUNDeviceName   string
	tunNetIPv6          string
	tunNetIP            string
	mtusize             string
	ProxyIpSubnetwork   net.IPNet
	ProxyIPv6Subnetwork net.IPNet
	localIP             net.IP
	proxycache          ProxyCache
	TunnelPort          int
	bufferPort          int
	udpwrite            sync.RWMutex
	tunwrite            sync.RWMutex
	isListening         bool
	localClusterId      string
	clusterAwareEnabled bool
	loadThreshold       float64
	updateInterval      time.Duration
	cpuWeight           float64
	memoryWeight        float64
	connWeight          float64
	metricsTTL          time.Duration
	// reverse-route cache: maps client namespace IP -> origin node IP
	reverseRoutes map[string]reverseRouteEntry
	reverseMu     sync.RWMutex
	reverseTTL    time.Duration
}

// incoming message from UDP channel
type incomingMessage struct {
	content *[]byte
	from    net.UDPAddr
}

// outgoing message from bridge
type outgoingMessage struct {
	content *[]byte
}

// reverseRouteEntry keeps where to send replies for a given client namespace IP
type reverseRouteEntry struct {
	nodeIP   net.IP
	lastSeen time.Time
}

// handler function for all outgoing messages that are received by the TUN device
func (proxy *GoProxyTunnel) outgoingMessage() {
	for msg := range proxy.outgoingChannel {
		ip, prot := decodePacket(*msg.content)
		if ip == nil {
			continue
		}
		logger.DebugLogger().Printf("Outgoing packet:\t\t\t%s ---> %s\n", ip.GetSrcIP().String(), ip.GetDestIP().String())

		if prot == nil { // only TCP/UDP supported
			logger.DebugLogger().Println("Neither TCP, nor UDP packet received. Dropping it.")
			continue
		}
		newPacket := proxy.outgoingProxy(ip, prot)
		if newPacket == nil {
			// As a safety net, try forwarding the original packet using reverse-route lookup
			// This covers replies towards foreign namespace IPs (non-ServiceIP) in cross-node flows
			if host, port := proxy.getReverseForwardTarget(ip.GetDestIP()); port > 0 {
				logger.DebugLogger().Printf("Reverse-route fallback: forwarding unchanged packet to %s:%d", host.String(), port)
				proxy.forward(host, port, ip.SerializePacket(ip.GetDestIP(), ip.GetSrcIP(), prot), 0)
				continue
			}
			logger.ErrorLogger().Println("Unable to convert the packet")
			continue
		}
		// Determine the actual destination from the new packet (outgoingProxy may have rewritten dst)
		newPacketBytes := packetToByte(newPacket)
		rewrittenNet, _ := decodePacket(newPacketBytes)
		if rewrittenNet == nil {
			logger.ErrorLogger().Println("Unable to decode rewritten packet to determine destination")
			continue
		}
		dstHost, dstPort := proxy.locateRemoteAddress(rewrittenNet.GetDestIP())
		proxy.forward(dstHost, dstPort, newPacket, 0)
	}
}

// handler function for all ingoing messages that are received by the UDP socket
func (proxy *GoProxyTunnel) ingoingMessage() {
	for msg := range proxy.incomingChannel {
		ip, prot := decodePacket(*msg.content)
		if ip == nil {
			continue
		}
		logger.DebugLogger().Printf("Ingoing packet:\t\t\t %s <--- %s\n", ip.GetDestIP().String(), ip.GetSrcIP().String())
		// If this packet arrived via UDP from a remote node, remember how to return to the origin
		if msg.from.Port != 0 && !msg.from.IP.Equal(proxy.localIP) {
			proxy.recordReverseRoute(ip.GetSrcIP(), msg.from.IP)
		}
		if prot == nil { // only TCP/UDP handled
			continue
		}
		newPacket := proxy.ingoingProxy(ip, prot)
		var packetBytes []byte
		if newPacket == nil {
			packetBytes = *msg.content
		} else {
			packetBytes = packetToByte(newPacket)
		}
		if _, err := proxy.ifce.Write(packetBytes); err != nil {
			logger.ErrorLogger().Println(err)
		}
	}
}

// If packet destination is in the range of proxy.ProxyIpSubnetwork
// then find enable load balancing policy and find out the actual dstIP address
func (proxy *GoProxyTunnel) outgoingProxy(ip iputils.NetworkLayerPacket, prot iputils.TransportLayerProtocol) gopacket.Packet {
	// apply cluster-aware only when enabled and the destination belongs to the ServiceIP subnetwork
	if proxy.clusterAwareEnabled && proxy.isServiceIPSubnet(ip.GetDestIP(), int(ip.GetProtocolVersion())) {
		// Check if destination is a ServiceIP and of type ClusterAware
		entries := proxy.environment.GetTableEntryByServiceIP(ip.GetDestIP())
		if len(entries) > 0 {
			isClusterAware := false
			for _, e := range entries {
				for _, sip := range e.ServiceIP {
					if (sip.Address.Equal(ip.GetDestIP()) || sip.Address_v6.Equal(ip.GetDestIP())) && sip.IpType == TableEntryCache.ClusterAware {
						isClusterAware = true
						break
					}
				}
			}
			if isClusterAware {
				sourceClusterId := proxy.getSourceClusterId(ip.GetSrcIP())
				preferIPv6 := ip.GetProtocolVersion() == 6
				logger.InfoLogger().Printf("Cluster-aware routing requested: src=%s dstServiceIP=%s srcCluster=%s preferIPv6=%v", ip.GetSrcIP().String(), ip.GetDestIP().String(), sourceClusterId, preferIPv6)
				clusterAwareDest, err := proxy.getClusterAwareDestination(ip.GetDestIP(), sourceClusterId, preferIPv6)
				if err == nil {
					logger.InfoLogger().Printf("Cluster-aware selection chosen dest=%s", clusterAwareDest.String())
					// Create a proxycache entry for reverse translations so replies from the instance
					// can be mapped back to the original client. Use the table entry matched by
					// the chosen namespace IP to populate node-specific fields.
					logger.DebugLogger().Printf("Cluster-aware pre-cache: orig src=%s:%d dstService=%s dstPort=%d candidateInstance=%s", ip.GetSrcIP(), prot.GetSourcePort(), ip.GetDestIP(), prot.GetDestPort(), clusterAwareDest)
					var conv ConversionEntry
					conv.srcip = ip.GetSrcIP()
					conv.dstServiceIp = ip.GetDestIP()
					conv.srcport = int(prot.GetSourcePort())
					conv.dstport = int(prot.GetDestPort())
					// Lookup table entry by namespace IP to obtain the node IP and canonical instance IP
					if te, ok := proxy.environment.GetTableEntryByNsIP(clusterAwareDest); ok {
						conv.dstip = te.Nodeip
						// srcInstanceIp is the instance's IP within its namespace
						conv.srcInstanceIp = clusterAwareDest
					} else {
						// Fallback: use namespace IP as both instance and node address — best-effort
						conv.dstip = clusterAwareDest
						conv.srcInstanceIp = clusterAwareDest
					}
					// Only add to proxycache if we have ports
					if conv.dstport > 0 && conv.srcport > 0 {
						proxy.proxycache.Add(conv)
					}
					logger.DebugLogger().Printf("Cluster-aware rewrite: new dst=%s keep src=%s", clusterAwareDest, ip.GetSrcIP())
					return proxy.createClusterAwarePacket(ip, prot, clusterAwareDest)
				} else {
					logger.ErrorLogger().Printf("Cluster-aware selection fallback: %v", err)
				}
			}
		}
	}
	logger.InfoLogger().Println("Non cluster-aware routing or no cluster info available, using existing logic")

	// Fall back to existing logic for non-service calls
	return proxy.outgoingProxyOriginal(ip, prot)
}

func (proxy *GoProxyTunnel) getSourceClusterId(srcIP net.IP) string {
	// Look up the source IP in the table to find which cluster it belongs to
	// Prefer explicit cluster id on the table entry; if absent, try load metrics' cluster id
	entry, exists := proxy.environment.GetTableEntryByNsIP(srcIP)
	if exists {
		if entry.ClusterId != "" {
			return entry.ClusterId
		}
		if entry.LoadMetrics.ClusterId != "" {
			logger.DebugLogger().Printf("Using LoadMetrics.ClusterId for src %s: %s", srcIP.String(), entry.LoadMetrics.ClusterId)
			return entry.LoadMetrics.ClusterId
		}
	}

	// If not found in NS-IP, try instance IP lookup (covers replies from instances)
	entry, exists = proxy.environment.GetTableEntryByInstanceIP(srcIP)
	if exists {
		if entry.ClusterId != "" {
			return entry.ClusterId
		}
		if entry.LoadMetrics.ClusterId != "" {
			logger.DebugLogger().Printf("Using LoadMetrics.ClusterId for instance src %s: %s", srcIP.String(), entry.LoadMetrics.ClusterId)
			return entry.LoadMetrics.ClusterId
		}
	}

	// Fallback: use proxy's configured local cluster id and log the fallback
	logger.DebugLogger().Printf("No cluster id found for src %s, falling back to localClusterId=%s", srcIP.String(), proxy.localClusterId)
	return proxy.localClusterId
}

func (proxy *GoProxyTunnel) createClusterAwarePacket(ip iputils.NetworkLayerPacket, prot iputils.TransportLayerProtocol, destIP net.IP) gopacket.Packet {
	// Create a new packet with the cluster-aware destination
	return ip.SerializePacket(destIP, ip.GetSrcIP(), prot)
}

func (proxy *GoProxyTunnel) outgoingProxyOriginal(ip iputils.NetworkLayerPacket, prot iputils.TransportLayerProtocol) gopacket.Packet {
	dstIP := ip.GetDestIP()
	srcIP := ip.GetSrcIP()
	var semanticRoutingSubnetwork bool
	srcport := -1
	dstport := -1
	if prot != nil {
		srcport = int(prot.GetSourcePort())
		dstport = int(prot.GetDestPort())
	}

	// If packet destination is part of the semantic routing subnetwork let the proxy handle it
	if ip.GetProtocolVersion() == 4 {
		semanticRoutingSubnetwork = proxy.ProxyIpSubnetwork.IP.Mask(proxy.ProxyIpSubnetwork.Mask).
			Equal(ip.GetDestIP().Mask(proxy.ProxyIpSubnetwork.Mask))
	}
	if ip.GetProtocolVersion() == 6 {
		semanticRoutingSubnetwork = proxy.ProxyIPv6Subnetwork.IP.Mask(proxy.ProxyIPv6Subnetwork.Mask).
			Equal(ip.GetDestIP().Mask(proxy.ProxyIPv6Subnetwork.Mask))
	}

	if semanticRoutingSubnetwork {
		// Check if the ServiceIP is known
		tableEntryList := proxy.environment.GetTableEntryByServiceIP(dstIP)
		if len(tableEntryList) < 1 {
			return nil
		}

		// Find the instanceIP of the current service
		instanceIP, err := proxy.convertToInstanceIp(ip)
		if err != nil {
			return nil
		}

		// Check proxy proxycache (if any active flow is there already)
		entry, exist := proxy.proxycache.RetrieveByServiceIP(srcIP, instanceIP, srcport, dstIP, dstport)

		if !exist || entry.dstport < 1 || !TableEntryCache.IsNamespaceStillValid(entry.dstip, &tableEntryList) {
			// Choose between the table entry according to the ServiceIP algorithm
			// TODO: so far this only uses RR, ServiceIP policies should be implemented here
			tableEntry := tableEntryList[proxy.randseed.Intn(len(tableEntryList))]

			entryDstIP := tableEntry.Nsipv6
			if ip.GetProtocolVersion() == 4 {
				entryDstIP = tableEntry.Nsip
			}

			// Update proxycache
			entry = ConversionEntry{
				srcip:         srcIP,
				dstip:         entryDstIP,
				dstServiceIp:  dstIP,
				srcInstanceIp: instanceIP,
				srcport:       srcport,
				dstport:       dstport,
			}
			proxy.proxycache.Add(entry)
		}
		return ip.SerializePacket(entry.dstip, entry.srcInstanceIp, prot)
	}
	// Not a ServiceIP destination: attempt reverse-route forwarding (reply path of cross-node flow)
	if host, port := proxy.getReverseForwardTarget(dstIP); port > 0 {
		logger.DebugLogger().Printf("Reverse-route: will forward reply towards %s via %s:%d", dstIP.String(), host.String(), port)
		// keep src/dst unchanged; forwarding will use locateRemoteAddress on rewritten packet
		return ip.SerializePacket(dstIP, srcIP, prot)
	}
	return nil
}

func (proxy *GoProxyTunnel) convertToInstanceIp(ip iputils.NetworkLayerPacket) (net.IP, error) {
	instanceTableEntry, instanceexist := proxy.environment.GetTableEntryByNsIP(ip.GetSrcIP())
	instanceIP := net.IP{}
	if instanceexist {
		for _, sip := range instanceTableEntry.ServiceIP {
			if sip.IpType == TableEntryCache.InstanceNumber {
				instanceIP = sip.Address_v6
				if ip.GetProtocolVersion() == 4 {
					instanceIP = sip.Address
				}
			}
		}
	} else {
		logger.ErrorLogger().Println("Unable to find instance IP for service: ", ip.GetSrcIP())
		return nil, fmt.Errorf("unable to find instance IP for service: %s ", ip.GetSrcIP().String())
	}
	return instanceIP, nil
}

// If packet destination port is proxy.tunnelport then is a packet forwarded by the proxy. The src address must beù
// changed with he original packet destination
func (proxy *GoProxyTunnel) ingoingProxy(ip iputils.NetworkLayerPacket, prot iputils.TransportLayerProtocol) gopacket.Packet {
	dstport := -1
	srcport := -1

	if prot != nil {
		dstport = int(prot.GetDestPort())
		srcport = int(prot.GetSourcePort())
	}

	// Check proxy proxycache for REVERSE entry conversion
	// DstIP -> srcip, DstPort->srcport, srcport -> dstport
	entry, exist := proxy.proxycache.RetrieveByInstanceIp(ip.GetDestIP(), dstport, srcport)

	if !exist {
		// primary lookup missed — attempt defensive alternate ordering in case
		// the entry was inserted with swapped src/dst port ordering
		logger.DebugLogger().Printf("ProxyCache primary miss; trying alternate lookup: dest=%s dstport=%d srcport=%d", ip.GetDestIP().String(), dstport, srcport)
		altEntry, altExist := proxy.proxycache.RetrieveByInstanceIp(ip.GetDestIP(), srcport, dstport)
		if altExist {
			logger.DebugLogger().Printf("ProxyCache alternate hit: using alternate mapping (swapped ports)")
			entry = altEntry
			exist = true
		}
	}

	if !exist {
		// No proxy proxycache entry, log detailed info for debugging
		logger.DebugLogger().Printf("No proxycache entry for incoming packet after fallback attempts: dest=%s dstport=%d srcport=%d", ip.GetDestIP().String(), dstport, srcport)
		return nil
	}

	// Reverse conversion
	return ip.SerializePacket(entry.srcip, entry.dstServiceIp, prot)
}

// Enable listening to outgoing packets
// if the goroutine must be stopped, send true to the stop channel
// when the channels finish listening a "true" is sent back to the finish channel
// in case of fatal error they are routed back to the err channel
func (proxy *GoProxyTunnel) tunOutgoingListen() {
	readerror := make(chan error)

	// async listener
	go proxy.ifaceread(proxy.ifce, proxy.outgoingChannel, readerror)

	// async handler
	go proxy.outgoingMessage()

	proxy.isListening = true
	logger.InfoLogger().Println("GoProxyTunnel outgoing listening started")
	for {
		select {
		case stopmsg := <-proxy.stopChannel:
			if stopmsg {
				logger.DebugLogger().Println("Outgoing listener received stop message")
				proxy.isListening = false
				proxy.finishChannel <- true
				return
			}
		case errormsg := <-readerror:
			proxy.errorChannel <- errormsg
		}
	}
}

// Enable listening for ingoing packets
// if the goroutine must be stopped, send true to the stop channel
// when the channels finish listening a "true" is sent back to the finish channel
// in case of fatal error they are routed back to the err channel
func (proxy *GoProxyTunnel) tunIngoingListen() {
	readerror := make(chan error)

	// async listener
	go proxy.udpread(proxy.listenConnection, proxy.incomingChannel, readerror)

	// async handler
	go proxy.ingoingMessage()

	proxy.isListening = true
	logger.InfoLogger().Println("GoProxyTunnel ingoing listening started")
	for {
		select {
		case stopmsg := <-proxy.stopChannel:
			if stopmsg {
				logger.DebugLogger().Println("Ingoing listener received stop message")
				_ = proxy.listenConnection.Close()
				proxy.isListening = false
				proxy.finishChannel <- true
				return
			}
		case errormsg := <-readerror:
			proxy.errorChannel <- errormsg
			// go udpread(proxy.listenConnection, readoutput, readerror)
		}
	}
}

// Given a network namespace IP find the machine IP and port for the tunneling
func (proxy *GoProxyTunnel) locateRemoteAddress(nsIP net.IP) (net.IP, int) {
	// if no local cache entry convert namespace IP to host IP via table query
	tableElement, found := proxy.environment.GetTableEntryByNsIP(nsIP)
	if found {
		logger.DebugLogger().Println("Remote NS IP", nsIP.String(), " translated to ", tableElement.Nodeip.String())
		return tableElement.Nodeip, tableElement.Nodeport
	}

	// Fallback to reverse-route cache: reply destined to a foreign namespace IP
	if host, port := proxy.getReverseForwardTarget(nsIP); port > 0 {
		logger.DebugLogger().Printf("Reverse-route locate: %s -> %s:%d", nsIP.String(), host.String(), port)
		return host, port
	}

	// If nothing found, just drop the packet using an invalid port
	return nsIP, -1
}

// forward message to final destination via UDP tunneling
func (proxy *GoProxyTunnel) forward(dstHost net.IP, dstPort int, packet gopacket.Packet, attemptNumber int) {
	if attemptNumber > 10 {
		return
	}

	packetBytes := packetToByte(packet)

	// If destination host is this machine, forward packet directly to the ingoing traffic method
	if dstHost.Equal(proxy.localIP) {
		logger.InfoLogger().Println("Packet forwarded locally")
		msg := incomingMessage{
			from: net.UDPAddr{
				IP:   proxy.localIP,
				Port: 0,
				Zone: "",
			},
			content: &packetBytes,
		}
		proxy.incomingChannel <- msg
		return
	}

	// Check udp channel buffer to avoid creating a new channel
	proxy.udpwrite.Lock()
	if dstPort <= 0 {
		logger.ErrorLogger().Printf("Missing node port for %s, defaulting to proxy tunnel port %d", dstHost.String(), proxy.TunnelPort)
		dstPort = proxy.TunnelPort
	}
	hoststring := net.JoinHostPort(dstHost.String(), strconv.Itoa(dstPort))
	con, exist := proxy.connectionBuffer[hoststring]
	proxy.udpwrite.Unlock()
	// TODO: flush connection buffer by time to time
	if !exist {
		logger.DebugLogger().Println("Establishing a new connection to node ", hoststring)
		connection, err := createUDPChannel(hoststring)
		if nil != err {
			return
		}
		_ = connection.SetWriteBuffer(BUFFER_SIZE)
		proxy.udpwrite.Lock()
		proxy.connectionBuffer[hoststring] = connection
		proxy.udpwrite.Unlock()
		con = connection
	}

	// send via UDP channel
	logger.DebugLogger().Printf("Forwarding packet to %s:%d (attempt=%d)", dstHost.String(), dstPort, attemptNumber)
	proxy.udpwrite.Lock()
	_, _, err := (*con).WriteMsgUDP(packetBytes, nil, nil)
	proxy.udpwrite.Unlock()
	if err != nil {
		_ = (*con).Close()
		logger.ErrorLogger().Printf("UDP write error to %s: %v", hoststring, err)
		connection, err := createUDPChannel(hoststring)
		if nil != err {
			return
		}
		proxy.udpwrite.Lock()
		proxy.connectionBuffer[hoststring] = connection
		proxy.udpwrite.Unlock()
		// Try again
		attemptNumber++
		proxy.forward(dstHost, dstPort, packet, attemptNumber)
	}
}

func createUDPChannel(hoststring string) (*net.UDPConn, error) {
	raddr, err := net.ResolveUDPAddr("udp", hoststring)
	if err != nil {
		logger.ErrorLogger().Println("Unable to resolve remote addr:", err)
		return nil, err
	}
	connection, err := net.DialUDP("udp", nil, raddr)
	if nil != err {
		logger.ErrorLogger().Println("Unable to connect to remote addr:", err)
		return nil, err
	}
	err = connection.SetWriteBuffer(BUFFER_SIZE)
	if nil != err {
		logger.ErrorLogger().Println("Buffer error:", err)
		return nil, err
	}
	return connection, nil
}

// read output from an interface and wrap the read operation with a channel
// out channel gives back the byte array of the output
// errchannel is the channel where in case of error the error is routed
func (proxy *GoProxyTunnel) ifaceread(ifce *water.Interface, out chan<- outgoingMessage, errchannel chan<- error) {
	buffer := make([]byte, BUFFER_SIZE)
	for {
		n, err := ifce.Read(buffer)
		if err != nil {
			errchannel <- err
		} else {
			res := make([]byte, n)
			copy(res, buffer[:n])
			logger.DebugLogger().Printf("Outgoing packet ready for decode action \n")
			out <- outgoingMessage{
				content: &res,
			}
		}
	}
}

// read output from an UDP connection and wrap the read operation with a channel
// out channel gives back the byte array of the output
// errchannel is the channel where in case of error the error is routed
func (proxy *GoProxyTunnel) udpread(conn *net.UDPConn, out chan<- incomingMessage, errchannel chan<- error) {
	if conn == nil {
		// lightweight mode: no UDP listener created
		return
	}
	buffer := make([]byte, BUFFER_SIZE)
	for {
		packet := buffer
		n, from, err := conn.ReadFromUDP(packet)
		if err != nil {
			errchannel <- err
		} else {
			res := make([]byte, n)
			copy(res, buffer[:n])
			out <- incomingMessage{
				from:    *from,
				content: &res,
			}
		}
	}
}

func packetToByte(packet gopacket.Packet) []byte {
	options := gopacket.SerializeOptions{
		ComputeChecksums: false,
		FixLengths:       true,
	}
	newBuffer := gopacket.NewSerializeBuffer()
	err := gopacket.SerializePacket(newBuffer, options, packet)
	if err != nil {
		logger.ErrorLogger().Println(err)
	}
	return newBuffer.Bytes()
}

// GetName returns the name of the tun interface
func (proxy *GoProxyTunnel) GetName() string {
	return proxy.HostTUNDeviceName
}

// GetErrCh returns the error channel
// this channel sends all the errors of the tun device
func (proxy *GoProxyTunnel) GetErrCh() <-chan error {
	return proxy.errorChannel
}

// GetStopCh returns the errCh
// this channel is used to stop the service. After a shutdown the TUN device stops listening
func (proxy *GoProxyTunnel) GetStopCh() chan<- bool {
	return proxy.stopChannel
}

// GetFinishCh returns the confirmation that the channel stopped listening for packets
func (proxy *GoProxyTunnel) GetFinishCh() <-chan bool {
	return proxy.finishChannel
}

func decodePacket(msg []byte) (iputils.NetworkLayerPacket, iputils.TransportLayerProtocol) {
	var ipType layers.IPProtocol
	switch msg[0] & 0xf0 {
	case 0x40:
		ipType = layers.IPProtocolIPv4
	case 0x60:
		ipType = layers.IPProtocolIPv6
	default:
		logger.DebugLogger().Println("Was neither IPv4 Packet, nor IPv6 packet.")
		return nil, nil
	}

	packet := iputils.NewGoPacket(msg, ipType)
	if packet == nil {
		logger.DebugLogger().Println("Error decoding Network Layer of Packet")
	}

	ipLayer := packet.NetworkLayer()
	if ipLayer == nil {
		logger.ErrorLogger().Println("Network Layer could not have been decoded.")
		return nil, nil
	}

	res := iputils.NewNetworkLayerPacket(ipType, ipLayer)
	if ipType == layers.IPProtocolIPv6 {
		res.DecodeNetworkLayer(packet)
	}

	err := res.Defragment()
	if err != nil {
		logger.ErrorLogger().Println("Error in defragmentation")
		return nil, nil
	}

	return res, res.GetTransportLayer()
}

// Given a service IP and the source cluster ID, select the best destination instance
func (proxy *GoProxyTunnel) getClusterAwareDestination(serviceIP net.IP, sourceClusterId string, preferIPv6 bool) (net.IP, error) {
	// Get all instances of the target service
	serviceEntries := proxy.environment.GetTableEntryByServiceIP(serviceIP)
	if len(serviceEntries) == 0 {
		return net.IP{}, fmt.Errorf("no service instances found for IP: %s", serviceIP.String())
	}

	// If cluster information is missing across the board, bail out to preserve existing behavior
	knownClusterInfo := false
	for _, e := range serviceEntries {
		logger.DebugLogger().Printf("service instance: %v, cluster_id: %s", e, e.LoadMetrics.ClusterId)
		if e.LoadMetrics.ClusterId != "" {
			knownClusterInfo = true
			break
		}
	}
	if !knownClusterInfo {
		return net.IP{}, fmt.Errorf("no cluster id information available in table entries")
	}

	// Separate instances by cluster
	localInstances := make([]TableEntryCache.TableEntry, 0)
	remoteInstances := make([]TableEntryCache.TableEntry, 0)

	for _, entry := range serviceEntries {
		if entry.LoadMetrics.ClusterId == sourceClusterId {
			localInstances = append(localInstances, entry)
		} else {
			remoteInstances = append(remoteInstances, entry)
		}
	}

	// First, try to find a suitable local instance
	if len(localInstances) > 0 {
		logger.DebugLogger().Printf("Found %d local instances for cluster %s", len(localInstances), sourceClusterId)
		localInstance := proxy.selectBestLocalInstance(localInstances)
		if localInstance != nil {
			logger.DebugLogger().Printf("Selected local instance %s (inst=%d) score metrics: cpu=%.2f mem=%.2f conns=%d ts=%d", localInstance.Nsip.String(), localInstance.Instancenumber, localInstance.LoadMetrics.CpuUsage, localInstance.LoadMetrics.MemoryUsage, localInstance.LoadMetrics.ActiveConnections, localInstance.LoadMetrics.Timestamp)
			if preferIPv6 {
				return localInstance.Nsipv6, nil
			}
			return localInstance.Nsip, nil
		}
	}

	// If no suitable local instance, fall back to remote instances
	if len(remoteInstances) > 0 {
		remoteInstance := proxy.selectBestRemoteInstance(remoteInstances)
		if remoteInstance != nil {
			if preferIPv6 {
				return remoteInstance.Nsipv6, nil
			}
			return remoteInstance.Nsip, nil
		}
	}

	return net.IP{}, fmt.Errorf("no suitable service instances found")
}

func (proxy *GoProxyTunnel) selectBestLocalInstance(instances []TableEntryCache.TableEntry) *TableEntryCache.TableEntry {
	// Collect top-scoring candidates (allow ties) and break ties by freshest metrics or randomly
	var candidates []*TableEntryCache.TableEntry
	bestScore := math.Inf(-1)
	eps := 1e-9

	for i := range instances {
		instance := &instances[i]

		// Log metrics presence and staleness check
		if instance.LoadMetrics.Timestamp == 0 {
			logger.DebugLogger().Printf("Instance %d has no load metrics; treating as unknown for scoring", instance.Instancenumber)
		} else if proxy.metricsTTL > 0 {
			age := time.Since(time.UnixMilli(instance.LoadMetrics.Timestamp))
			if age > proxy.metricsTTL {
				logger.DebugLogger().Printf("Instance %d metrics stale (age=%v > ttl=%v), skipping", instance.Instancenumber, age, proxy.metricsTTL)
				continue
			}
		}

		// If threshold configured, skip local instances that are considered overloaded
		if proxy.loadThreshold > 0 {
			// simple aggregate load: average of cpu and memory usage in [0,1]
			effectiveLoad := 0.0
			if instance.LoadMetrics.CpuUsage > 0 || instance.LoadMetrics.MemoryUsage > 0 {
				effectiveLoad = (instance.LoadMetrics.CpuUsage + instance.LoadMetrics.MemoryUsage) / 2.0
			}
			logger.DebugLogger().Printf("Instance %d effectiveLoad=%.3f (cpu=%.3f mem=%.3f)", instance.Instancenumber, effectiveLoad, instance.LoadMetrics.CpuUsage, instance.LoadMetrics.MemoryUsage)
			if effectiveLoad > proxy.loadThreshold {
				logger.DebugLogger().Printf("Instance %d exceeds load threshold %.3f; skipping", instance.Instancenumber, proxy.loadThreshold)
				continue
			}
		}

		score := proxy.calculateInstanceScore(instance)
		logger.DebugLogger().Printf("Instance %d score=%.4f (cpu=%.3f mem=%.3f conns=%d)", instance.Instancenumber, score, instance.LoadMetrics.CpuUsage, instance.LoadMetrics.MemoryUsage, instance.LoadMetrics.ActiveConnections)

		if score < 0 { // penalized / invalid
			continue
		}

		if score > bestScore+eps {
			// new best
			candidates = candidates[:0]
			candidates = append(candidates, instance)
			bestScore = score
		} else if math.Abs(score-bestScore) <= eps {
			// tie
			candidates = append(candidates, instance)
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	if len(candidates) == 1 {
		return candidates[0]
	}

	// If multiple top candidates, prefer the one with freshest metrics
	freshestTS := int64(0)
	for _, c := range candidates {
		if c.LoadMetrics.Timestamp > freshestTS {
			freshestTS = c.LoadMetrics.Timestamp
		}
	}

	freshCandidates := make([]*TableEntryCache.TableEntry, 0)
	if freshestTS > 0 {
		for _, c := range candidates {
			if c.LoadMetrics.Timestamp == freshestTS {
				freshCandidates = append(freshCandidates, c)
			}
		}
	}

	if len(freshCandidates) == 0 {
		// No metrics at all — pick randomly among candidates
		idx := proxy.randseed.Intn(len(candidates))
		return candidates[idx]
	}

	// If multiple freshest, pick randomly among freshest
	idx := proxy.randseed.Intn(len(freshCandidates))
	return freshCandidates[idx]
}

func (proxy *GoProxyTunnel) selectBestRemoteInstance(instances []TableEntryCache.TableEntry) *TableEntryCache.TableEntry {
	// For remote instances, we might want to consider network latency as well
	// For now, we use the same logic as local instances
	return proxy.selectBestLocalInstance(instances)
}

func (proxy *GoProxyTunnel) calculateInstanceScore(instance *TableEntryCache.TableEntry) float64 {
	// If metrics are stale, penalize heavily (or treat as unknown load -> lower priority). Stale if timestamp older than TTL.
	if proxy.metricsTTL > 0 && instance.LoadMetrics.Timestamp > 0 {
		age := time.Since(time.UnixMilli(instance.LoadMetrics.Timestamp))
		if age > proxy.metricsTTL {
			// use negative score so any fresh instance with non-negative wins deterministically
			logger.DebugLogger().Printf("Penalizing instance %d for stale metrics (age=%v > ttl=%v)", instance.Instancenumber, age, proxy.metricsTTL)
			return -1.0
		}
	}

	// Base inversion to convert usage to free capacity
	cpuScore := 1.0 - clamp01(instance.LoadMetrics.CpuUsage)
	memoryScore := 1.0 - clamp01(instance.LoadMetrics.MemoryUsage)
	// Simple normalization of connections: assume 0..200 baseline
	connNorm := float64(instance.LoadMetrics.ActiveConnections) / 200.0
	if connNorm > 1 {
		connNorm = 1
	}
	connectionScore := 1.0 - connNorm

	// Weighted average (configurable)
	wCPU := proxy.cpuWeight
	wMEM := proxy.memoryWeight
	wCONN := proxy.connWeight
	total := wCPU + wMEM + wCONN
	if total <= 0 { // fallback defaults
		wCPU, wMEM, wCONN = 0.4, 0.4, 0.2
		total = 1.0
	}
	score := (cpuScore*wCPU + memoryScore*wMEM + connectionScore*wCONN) / total
	return score
}

// clamp01 ensures a value is within [0,1]
func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// Expose configuration setters used by server from netcfg.json
func (proxy *GoProxyTunnel) SetClusterAwareRouting(enabled bool) {
	proxy.clusterAwareEnabled = enabled
}

func (proxy *GoProxyTunnel) SetLocalClusterId(id string) {
	proxy.localClusterId = id
}

func (proxy *GoProxyTunnel) SetLoadThreshold(threshold float64) {
	proxy.loadThreshold = threshold
}

// New setters for weighting and advanced behavior
func (proxy *GoProxyTunnel) SetWeights(cpu, mem, conn float64) {
	proxy.cpuWeight, proxy.memoryWeight, proxy.connWeight = cpu, mem, conn
}
func (proxy *GoProxyTunnel) SetMetricsTTL(seconds int) {
	if seconds <= 0 {
		proxy.metricsTTL = 0
		return
	}
	proxy.metricsTTL = time.Duration(seconds) * time.Second
}

// isServiceIPSubnet returns true if ip is within the configured ServiceIP subnetwork (v4 or v6)
func (proxy *GoProxyTunnel) isServiceIPSubnet(ip net.IP, version int) bool {
	if version == 4 {
		return proxy.ProxyIpSubnetwork.IP.Mask(proxy.ProxyIpSubnetwork.Mask).Equal(ip.Mask(proxy.ProxyIpSubnetwork.Mask))
	}
	if version == 6 {
		return proxy.ProxyIPv6Subnetwork.IP.Mask(proxy.ProxyIPv6Subnetwork.Mask).Equal(ip.Mask(proxy.ProxyIPv6Subnetwork.Mask))
	}
	return false
}

// recordReverseRoute remembers that replies for clientNsIP must be sent to originNode
func (proxy *GoProxyTunnel) recordReverseRoute(clientNsIP net.IP, originNode net.IP) {
	if clientNsIP == nil || originNode == nil {
		return
	}
	key := clientNsIP.String()
	proxy.reverseMu.Lock()
	proxy.reverseRoutes[key] = reverseRouteEntry{nodeIP: originNode, lastSeen: time.Now()}
	proxy.reverseMu.Unlock()
	logger.DebugLogger().Printf("Reverse-route recorded: %s -> %s", key, originNode.String())
}

// getReverseForwardTarget returns the node and port to reach a foreign namespace IP, if known
func (proxy *GoProxyTunnel) getReverseForwardTarget(nsIP net.IP) (net.IP, int) {
	if nsIP == nil {
		return net.IP{}, -1
	}
	key := nsIP.String()
	proxy.reverseMu.RLock()
	entry, ok := proxy.reverseRoutes[key]
	proxy.reverseMu.RUnlock()
	if !ok {
		return net.IP{}, -1
	}
	ttl := proxy.reverseTTL
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	if time.Since(entry.lastSeen) > ttl {
		// expire stale entry
		proxy.reverseMu.Lock()
		delete(proxy.reverseRoutes, key)
		proxy.reverseMu.Unlock()
		return net.IP{}, -1
	}
	return entry.nodeIP, proxy.TunnelPort
}

func (proxy *GoProxyTunnel) StartLoadMonitoring(intervalSeconds int) {
	if intervalSeconds <= 0 {
		intervalSeconds = 300
	}
	proxy.updateInterval = time.Duration(intervalSeconds) * time.Second
	proxy.startLoadMonitoring()
}

// Add periodic load monitoring
func (proxy *GoProxyTunnel) startLoadMonitoring() {
	go func() {
		interval := proxy.updateInterval
		if interval == 0 {
			interval = 300 * time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				proxy.updateLoadMetrics()
			case <-proxy.stopChannel:
				return
			}
		}
	}()
}

func (proxy *GoProxyTunnel) updateLoadMetrics() {
	// Get entries by service IP for each known service
	for _, serviceIP := range proxy.getKnownServiceIPs() {
		entries := proxy.environment.GetTableEntryByServiceIP(serviceIP)
		for i := range entries {
			entry := &entries[i]
			// fetch updated metrics using full entry context (nsip, node ip, cluster id)
			loadMetrics := proxy.getInstanceLoadMetrics(entry.Nsip)
			// Optionally apply smoothing (EMA) if enabled and we have previous metrics
			entry.LoadMetrics = loadMetrics
		}
	}
}

func ema(prev, current, alpha float64) float64 { return prev*(1-alpha) + current*alpha }

// getInstanceLoadMetrics retrieves metrics via MQTT table query and reads metrics from updated TableEntry in memory.
func (proxy *GoProxyTunnel) getInstanceLoadMetrics(instanceIP net.IP) TableEntryCache.LoadMetrics {
	// Locate current entry to get job context
	entry, ok := proxy.environment.GetTableEntryByNsIP(instanceIP)
	if !ok {
		return TableEntryCache.LoadMetrics{}
	}

	// Force refresh from Cluster Service Manager (Mongo-backed)
	proxy.environment.RefreshServiceTable(entry.JobName)

	// Read back updated entry and return latest metrics
	if updated, ok2 := proxy.environment.GetTableEntryByNsIP(instanceIP); ok2 {
		return updated.LoadMetrics
	}
	// Fallback to previously known metrics
	return entry.LoadMetrics
}

func (proxy *GoProxyTunnel) getKnownServiceIPs() []net.IP {
	// Use proxy cache to return a list of known destination Service IPs
	return proxy.proxycache.KnownServiceIPs()
}
