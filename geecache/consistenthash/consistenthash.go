package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint32

type Consistence struct {
	hash     Hash           // hash function
	replicas int            // number of virtual nodes
	ring     []int          // sorted hash ring
	hashMap  map[int]string // virtual node to actual node
}

func New(replicas int, fn Hash) *Consistence {
	c := &Consistence{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if c.hash == nil {
		c.hash = crc32.ChecksumIEEE
	}
	return c
}

// Add adds some nodes to the hash.
func (c *Consistence) Add(nodes ...string) {
	for _, node := range nodes {
		for i := 0; i < c.replicas; i++ {
			hash := int(c.hash([]byte(node + strconv.Itoa(i))))
			c.ring = append(c.ring, hash)
			c.hashMap[hash] = node
		}
	}
	sort.Ints(c.ring)
}

// Get gets the closest item in the hash to the provided key.
func (c *Consistence) Get(key string) string {
	if len(c.ring) == 0 {
		return ""
	}
	hash := int(c.hash([]byte(key)))
	// Binary search for appropriate replica
	idx := sort.Search(len(c.ring), func(i int) bool {
		return c.ring[i] >= hash
	})

	return c.hashMap[c.ring[idx%len(c.ring)]]
}
