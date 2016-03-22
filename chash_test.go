package chash

import (
	"fmt"
	"strconv"
	"testing"
)

func setUp() *Ring {
	nodes := make([]string, 0)
	nodes = append(nodes, "node0", "node1", "node2", "node3")
	// Override the default hash function with simple function
	// that only work with string key that can be converted to int
	ring := New(16, nodes, func(key []byte) uint32 {
		i, err := strconv.Atoi(string(key))
		if err != nil {
			panic(err)
		}
		return uint32(i)
	})
	return ring
}

func TestNew(t *testing.T) {
	ring := setUp()
	fmt.Println(ring.keys)
	fmt.Println(ring.hashMap)
	fmt.Println(ring.nodeKeys)
}

func TestFind(t *testing.T) {
	ring := setUp()
	testCases := map[string]string{
		"1":  "node1",
		"5":  "node1",
		"18": "node0",
	}
	for k, v := range testCases {
		if ring.Find(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}
}
