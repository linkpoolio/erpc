package util

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

func EvmNetworkId(chainId interface{}) string {
	return fmt.Sprintf("evm:%d", chainId)
}

func JsonRpcNetworkId(id string) string {
	return fmt.Sprintf("jsonrpc:%s", id)
}

var validIdentifierRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func IsValidIdentifier(s string) bool {
	return validIdentifierRegex.MatchString(s)
}

func IsValidNetworkId(s string) bool {
	if strings.HasPrefix(s, "evm:") {
		_, err := strconv.Atoi(s[4:])
		return err == nil
	}
	if strings.HasPrefix(s, "jsonrpc:") {
		id := s[len("jsonrpc:"):]
		return id != "" && IsValidIdentifier(id)
	}
	return false
}

var counters = make(map[string]int)
var countersMutex = sync.Mutex{}

func IncrementAndGetIndex(parts ...string) string {
	countersMutex.Lock()
	defer countersMutex.Unlock()
	counterKey := strings.Join(parts, "</@/>")
	if _, ok := counters[counterKey]; !ok {
		counters[counterKey] = 0
	}
	counters[counterKey]++
	return strconv.Itoa(counters[counterKey])
}
