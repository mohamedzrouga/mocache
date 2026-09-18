package mocache

import (
	"fmt"
	"strconv"
	"strings"
)

// Redis Cluster slot routing, as an alternative to the MD5 hash ring.
//
// A MoCache node can be reached three ways, and the two routing schemes do not
// agree on placement: the SDK's ring puts a key wherever MD5 says, a Redis
// client puts it wherever CRC16 says. Both reach the same LRU on the same
// process and neither redirects the other's traffic, so a key written through
// this SDK and read through redis-py can land on different nodes and read as a
// miss. Configuring a slot map makes this SDK compute placement exactly as a
// Redis client does, which is the only way one keyspace can be shared by both.
//
// The map is static, given as the same slot ranges that -cluster-peer declares.
// It does not follow a failover: after one, the slots of the promoted shard are
// served by a different node than the map names, and this SDK will miss on them
// until the map is updated — the same limitation the ring has always had, since
// neither knows anything about cluster membership. A client that needs to track
// failover should use the RESP port with a real cluster client, which follows
// MOVED. See docs/cluster.md.

// SlotCount is fixed by the Redis Cluster specification.
const SlotCount = 16384

// SlotRange is an inclusive span of the keyspace.
type SlotRange struct{ Start, End int }

// KeySlot returns the slot a key belongs to: CRC16 of the hash tag if the key
// has one, otherwise of the whole key, modulo 16384. It is the same function
// every Redis cluster client computes, and it must stay that way — see HashTag.
func KeySlot(key string) int {
	return int(crc16([]byte(HashTag(key)))) % SlotCount
}

// HashTag returns the substring between the first '{' and the next '}' after
// it, when that substring is non-empty; otherwise the whole key. It is how a
// caller forces related keys onto one node ("{user:1}:name", "{user:1}:email").
func HashTag(key string) string {
	open := strings.IndexByte(key, '{')
	if open < 0 {
		return key
	}
	closing := strings.IndexByte(key[open+1:], '}')
	if closing <= 0 {
		return key
	}
	return key[open+1 : open+1+closing]
}

// crc16 is CRC-16/XMODEM (polynomial 0x1021, zero init, no reflection), the
// exact variant Redis uses. Its check value over "123456789" is 0x31C3 and the
// tests assert it: a client that computes slots differently does not produce an
// error, it produces silent misrouting.
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

// ParseSlotRanges reads the slot list form -cluster-peer uses: "0-5460", or
// "5461-10922,12000-12100" for a node with several ranges.
func ParseSlotRanges(s string) ([]SlotRange, error) {
	var out []SlotRange
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lo, hi, isRange := strings.Cut(part, "-")
		start, err := strconv.Atoi(strings.TrimSpace(lo))
		if err != nil {
			return nil, fmt.Errorf("mocache: bad slot %q", part)
		}
		end := start
		if isRange {
			if end, err = strconv.Atoi(strings.TrimSpace(hi)); err != nil {
				return nil, fmt.Errorf("mocache: bad slot range %q", part)
			}
		}
		if start < 0 || end >= SlotCount || start > end {
			return nil, fmt.Errorf("mocache: slot range %q outside 0-%d", part, SlotCount-1)
		}
		out = append(out, SlotRange{Start: start, End: end})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("mocache: no slots in %q", s)
	}
	return out, nil
}

// slotRouter maps a key to a node by slot. Immutable after construction, so
// lookups need no lock — the same property the ring has.
type slotRouter struct {
	owner [SlotCount]string
}

// newSlotRouter builds the map from node URL to slot ranges. Every slot must
// have exactly one owner: a gap would silently send part of the keyspace
// nowhere, and an overlap would send one key to two different nodes depending
// on map iteration order.
func newSlotRouter(slots map[string][]SlotRange) (*slotRouter, error) {
	r := &slotRouter{}
	// Sorted so a duplicate is always reported against the same pair of nodes,
	// whichever order the caller built the map in.
	nodes := make([]string, 0, len(slots))
	for node := range slots {
		nodes = append(nodes, node)
	}
	sortStrings(nodes)

	for _, node := range nodes {
		for _, sr := range slots[node] {
			if sr.Start < 0 || sr.End >= SlotCount || sr.Start > sr.End {
				return nil, fmt.Errorf("mocache: node %s has slot range %d-%d outside 0-%d",
					node, sr.Start, sr.End, SlotCount-1)
			}
			for s := sr.Start; s <= sr.End; s++ {
				if prev := r.owner[s]; prev != "" {
					return nil, fmt.Errorf("mocache: slot %d claimed by both %s and %s", s, prev, node)
				}
				r.owner[s] = node
			}
		}
	}
	for s, owner := range r.owner {
		if owner == "" {
			return nil, fmt.Errorf("mocache: slot %d has no node; the map must cover all %d slots", s, SlotCount)
		}
	}
	return r, nil
}

func (r *slotRouter) node(key string) string { return r.owner[KeySlot(key)] }

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
