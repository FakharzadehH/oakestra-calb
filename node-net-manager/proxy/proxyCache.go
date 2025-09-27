package proxy

import (
	"NetManager/logger"
	"net"
	"sync"
	"time"
)

type ConversionEntry struct {
	srcip         net.IP
	dstip         net.IP
	dstServiceIp  net.IP
	srcInstanceIp net.IP
	srcport       int
	dstport       int
}

type ConversionList struct {
	nextEntry      int
	lastUsed       int64
	conversionList []ConversionEntry
}

type ProxyCache struct {
	//One position for each port number. Higher mem usage but lower cpu usage
	cache                 []ConversionList
	conversionListMaxSize int
	rwlock                sync.RWMutex
}

func NewProxyCache() ProxyCache {
	return ProxyCache{
		cache:                 make([]ConversionList, 65535),
		conversionListMaxSize: 10,
		rwlock:                sync.RWMutex{},
	}
}

// RetrieveByServiceIP Retrieve proxy proxycache entry based on source ip and source port and destination ServiceIP
func (cache *ProxyCache) RetrieveByServiceIP(srcip net.IP, instanceIP net.IP, srcport int, dstServiceIp net.IP, dstport int) (ConversionEntry, bool) {
	cache.rwlock.Lock()
	defer cache.rwlock.Unlock()

	elem := cache.cache[srcport]
	elem.lastUsed = time.Now().Unix()
	if elem.conversionList != nil {
		for _, cacheEntry := range elem.conversionList {
			if cacheEntry.dstport == dstport &&
				cacheEntry.dstServiceIp.Equal(dstServiceIp) &&
				cacheEntry.srcip.Equal(srcip) &&
				cacheEntry.srcInstanceIp.Equal(instanceIP) {
				return cacheEntry, true
			}
		}
	}
	return ConversionEntry{}, false
}

// RetrieveByInstanceIp Retrieve proxy proxycache entry based on source ip and source port and destination ip
func (cache *ProxyCache) RetrieveByInstanceIp(srcip net.IP, srcport int, dstport int) (ConversionEntry, bool) {
	cache.rwlock.Lock()
	defer cache.rwlock.Unlock()

	elem := cache.cache[srcport]
	elem.lastUsed = time.Now().Unix()
	if elem.conversionList != nil {
		for _, entry := range elem.conversionList {
			if entry.dstport == dstport && entry.srcip.Equal(srcip) {
				logger.DebugLogger().Printf("ProxyCache hit: srcip=%s srcport=%d dstport=%d -> dstip=%s dstServiceIp=%s srcInstanceIp=%s", entry.srcip.String(), entry.srcport, entry.dstport, entry.dstip.String(), entry.dstServiceIp.String(), entry.srcInstanceIp.String())
				return entry, true
			}
		}
	}
	// Try a global scan as a defensive fallback in case entry was added to a different port bucket
	logger.DebugLogger().Printf("ProxyCache bucket miss: lookup srcip=%s srcport=%d dstport=%d - scanning global cache", srcip.String(), srcport, dstport)
	for i, bucket := range cache.cache {
		if bucket.conversionList == nil {
			continue
		}
		for _, e := range bucket.conversionList {
			if e.dstport == dstport && e.srcip.Equal(srcip) {
				logger.DebugLogger().Printf("ProxyCache global-scan hit: found in bucket=%d srcip=%s srcport=%d dstport=%d -> dstip=%s dstServiceIp=%s srcInstanceIp=%s", i, e.srcip.String(), e.srcport, e.dstport, e.dstip.String(), e.dstServiceIp.String(), e.srcInstanceIp.String())
				return e, true
			}
		}
	}
	logger.DebugLogger().Printf("ProxyCache miss after global scan: lookup srcip=%s srcport=%d dstport=%d", srcip.String(), srcport, dstport)
	return ConversionEntry{}, false
}

// RetrieveByInstanceIpWithLog is helper that logs retrieval attempts (used for debugging)
func (cache *ProxyCache) RetrieveByInstanceIpWithLog(srcip net.IP, srcport int, dstport int) (ConversionEntry, bool) {
	e, ok := cache.RetrieveByInstanceIp(srcip, srcport, dstport)
	if ok {
		// best-effort log
		// Note: avoid too verbose production logging
		return e, true
	}
	return e, false
}

// Add new conversion entry, if srcpip && srcport already added the entry is updated
func (cache *ProxyCache) Add(entry ConversionEntry) {
	cache.rwlock.Lock()
	defer cache.rwlock.Unlock()

	// Debug: log cache addition summary (avoid heavy printing of IPs in production)
	logger.DebugLogger().Printf("ProxyCache add: srcip=%s srcport=%d dstip=%s dstport=%d dstServiceIp=%s srcInstanceIp=%s", entry.srcip.String(), entry.srcport, entry.dstip.String(), entry.dstport, entry.dstServiceIp.String(), entry.srcInstanceIp.String())

	elem := cache.cache[entry.srcport]
	if elem.conversionList == nil || len(elem.conversionList) == 0 {
		elem.nextEntry = 0
		elem.conversionList = make([]ConversionEntry, cache.conversionListMaxSize)
	}
	cache.cache[entry.srcport] = elem

	cache.addToConversionList(entry)
	// Also add to the dstport bucket so reverse lookups that index by dstport succeed
	if entry.dstport > 0 && entry.dstport < len(cache.cache) {
		cache.addToConversionListAtPort(entry.dstport, entry)
	}
}

func (cache *ProxyCache) addToConversionList(entry ConversionEntry) {
	elem := cache.cache[entry.srcport]
	elem.lastUsed = time.Now().Unix()
	alreadyExist := false
	alreadyExistPosition := 0
	//check if used port is already in proxycache
	for i, elementry := range elem.conversionList {
		if elementry.dstport == entry.dstport {
			alreadyExistPosition = i
			alreadyExist = true
			break
		}
	}
	if alreadyExist {
		//if sourceport already in proxycache overwrite the proxycache entry
		elem.conversionList[alreadyExistPosition] = entry

	} else {
		//otherwise add a new proxycache entry in the next slot available
		elem.conversionList[elem.nextEntry] = entry
		elem.nextEntry = (elem.nextEntry + 1) % cache.conversionListMaxSize
	}
}

// addToConversionListAtPort inserts the conversion entry into the bucket for a specific port.
// Caller must hold the rwlock.
func (cache *ProxyCache) addToConversionListAtPort(port int, entry ConversionEntry) {
	if port < 0 || port >= len(cache.cache) {
		return
	}
	elem := cache.cache[port]
	elem.lastUsed = time.Now().Unix()
	alreadyExist := false
	alreadyExistPosition := 0
	// check if dstport already present
	if elem.conversionList == nil || len(elem.conversionList) == 0 {
		elem.nextEntry = 0
		elem.conversionList = make([]ConversionEntry, cache.conversionListMaxSize)
	}
	for i, elementry := range elem.conversionList {
		if elementry.dstport == entry.dstport {
			alreadyExistPosition = i
			alreadyExist = true
			break
		}
	}
	if alreadyExist {
		elem.conversionList[alreadyExistPosition] = entry
	} else {
		elem.conversionList[elem.nextEntry] = entry
		elem.nextEntry = (elem.nextEntry + 1) % cache.conversionListMaxSize
	}
	cache.cache[port] = elem
}

// KnownServiceIPs returns a de-duplicated list of destination Service IPs seen in the cache
func (cache *ProxyCache) KnownServiceIPs() []net.IP {
	cache.rwlock.RLock()
	defer cache.rwlock.RUnlock()

	seen := make(map[string]struct{})
	result := make([]net.IP, 0)

	for _, elem := range cache.cache {
		if elem.conversionList == nil {
			continue
		}
		for _, e := range elem.conversionList {
			if e.dstServiceIp == nil || len(e.dstServiceIp) == 0 {
				continue
			}
			k := e.dstServiceIp.String()
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				result = append(result, e.dstServiceIp)
			}
		}
	}
	return result
}

// ActiveConnectionsForInstance returns a heuristic count of active flows targeting the given namespace IP.
// Returns -1 if the cache has no data.
func (cache *ProxyCache) ActiveConnectionsForInstance(nsip net.IP) int {
	cache.rwlock.RLock()
	defer cache.rwlock.RUnlock()

	if cache.cache == nil || len(cache.cache) == 0 {
		return -1
	}
	count := 0
	for _, elem := range cache.cache {
		if elem.conversionList == nil {
			continue
		}
		for _, e := range elem.conversionList {
			if e.dstip != nil && e.dstip.Equal(nsip) {
				count++
			}
		}
	}
	return count
}
