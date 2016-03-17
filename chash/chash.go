package chash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint32

type Ring struct {
	hash     Hash
	portions int
	keys     []int
	hashMap  map[int]string
	nodeKeys map[string][]int
}

func New(num int, nodes []string, fun Hash) *Ring {
	n := len(nodes)
	ring := &Ring{
		hash:     fun,
		portions: num,
		hashMap:  make(map[int]string),
		nodeKeys: make(map[string][]int),
	}
	if ring.hash == nil {
		ring.hash = crc32.ChecksumIEEE
	}
	for k := 0; k < ring.portions; k++ {
		hash := int(ring.hash([]byte(strconv.Itoa(k))))
		ring.keys = append(ring.keys, hash)
		ring.hashMap[hash] = nodes[k%n]
		ring.nodeKeys[nodes[k%n]] = append(ring.nodeKeys[nodes[k%n]], hash)
	}
	sort.Ints(ring.keys)
	return ring
}

// Returns true if there are no nodes available.
func (ring *Ring) IsEmpty() bool {
	return len(ring.keys) == 0
}

func (ring *Ring) Find(key string) string {
	if ring.IsEmpty() {
		return ""
	}
	hash := int(ring.hash([]byte(key)))
	// binary search to find the owning node
	ind := sort.Search(len(ring.keys), func(k int) bool { return ring.keys[k] >= hash })
	// wrap around in the ring
	if ind == len(ring.keys) {
		ind = 0
	}
	return ring.hashMap[ring.keys[ind]]
}

func (ring *Ring) Add(node string) {
}

func (ring *Ring) Remove(node string) {
	keyList := ring.nodeKeys[node]
	delete(ring.nodeKeys, node)
	newNodes := make([]string, 0)
	for node := range ring.nodeKeys {
		newNodes = append(newNodes, node)
	}
	n := len(newNodes)
	for i := 0; i < len(keyList); i++ {
		ring.hashMap[keyList[i]] = newNodes[i%n]
		ring.nodeKeys[newNodes[i%n]] = append(ring.nodeKeys[newNodes[i%n]], keyList[i])
	}
}
