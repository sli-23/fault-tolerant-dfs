package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	hashes := []string{}
	for h, _ := range c.ServerMap {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)
	server := ""
	flag := false
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			server = c.ServerMap[hashes[i]]
			flag = true
			break
		}
	}
	if !flag || server == "" {
		server = c.ServerMap[hashes[0]]
	}
	return server
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	hashRing := &ConsistentHashRing{ServerMap: make(map[string]string)}
	for _, serverName := range serverAddrs {
		serverHash := hashRing.Hash("blockstore" + serverName)
		hashRing.ServerMap[serverHash] = serverName
	}

	return hashRing
}
