// Package cluster implements Redis Cluster key routing: the 16384-slot
// keyspace, CRC16 hashing with hash tags, and the topology a node advertises
// through CLUSTER SLOTS / SHARDS / NODES and enforces with MOVED redirects.
//
// Adopting Redis's slot scheme rather than MoCache's MD5 ring is what lets
// stock cluster clients (redis-py RedisCluster, go-redis ClusterClient) shard
// for us: they already know how to read the slot map and follow redirects.
package cluster

import "strings"

// SlotCount is fixed by the Redis Cluster specification. Clients hard-code it.
const SlotCount = 16384

// KeySlot returns the slot a key belongs to: CRC16 of the hash tag if the key
// has one, otherwise of the whole key, modulo 16384.
func KeySlot(key string) int {
	return int(crc16([]byte(HashTag(key)))) % SlotCount
}

// HashTag returns the substring between the first '{' and the next '}' after it,
// when that substring is non-empty; otherwise the whole key. This is how a
// client forces related keys onto one slot ("{user:1}:name", "{user:1}:email"),
// which is the only way multi-key commands can work in a sharded cluster.
func HashTag(key string) string {
	open := strings.IndexByte(key, '{')
	if open < 0 {
		return key
	}
	close := strings.IndexByte(key[open+1:], '}')
	if close <= 0 {
		return key
	}
	return key[open+1 : open+1+close]
}

// crc16 is CRC-16/XMODEM (polynomial 0x1021, zero init, no reflection), the
// exact variant Redis uses in crc16.c. Computed bitwise instead of carrying a
// 256-entry table: at these key lengths the difference is noise next to the
// network round trip, and the constant is easier to verify by eye.
func crc16(data []byte) uint16 {
	var crc uint16
	for _, b := range data {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
