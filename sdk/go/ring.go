package mocache

import (
	"bytes"
	"crypto/md5"
	"sort"
)

type vnode struct {
	hash [16]byte
	node string
}

// hashRing is immutable after construction, so concurrent lookups need no lock.
type hashRing struct {
	vnodes []vnode
}

func newHashRing(nodes []string, nvirtual int) *hashRing {
	if nvirtual < 1 {
		nvirtual = 100
	}
	// Map last-write-wins on hash collision, matching the Python dict.
	// The ring is built once; lookups are lock-free because the slice is immutable.
	seen := make(map[[16]byte]string, len(nodes)*nvirtual)
	for _, node := range nodes {
		for i := 0; i < nvirtual; i++ {
			h := md5.Sum(fmtNode(node, i))
			seen[h] = node
		}
	}
	vnodes := make([]vnode, 0, len(seen))
	for h, node := range seen {
		vnodes = append(vnodes, vnode{hash: h, node: node})
	}
	sort.Slice(vnodes, func(i, j int) bool {
		return bytes.Compare(vnodes[i].hash[:], vnodes[j].hash[:]) < 0
	})
	return &hashRing{vnodes: vnodes}
}

func fmtNode(node string, i int) []byte {
	// "{node}:{i}" — must match the Python SDK byte-for-byte (no extra zeros).
	b := make([]byte, 0, len(node)+12)
	b = append(b, node...)
	b = append(b, ':')
	if i == 0 {
		return append(b, '0')
	}
	var tmp [20]byte
	n := len(tmp)
	for i > 0 {
		n--
		tmp[n] = byte('0' + i%10)
		i /= 10
	}
	return append(b, tmp[n:]...)
}

func (r *hashRing) node(key string) string {
	if len(r.vnodes) == 0 {
		return ""
	}
	h := md5.Sum([]byte(key))
	v := r.vnodes
	// First hash >= key hash (bisect_left); wrap to 0 so the ring is circular.
	idx := sort.Search(len(v), func(i int) bool {
		return bytes.Compare(v[i].hash[:], h[:]) >= 0
	})
	if idx == len(v) {
		idx = 0
	}
	return v[idx].node
}
