// ------------------------------------
//             OR-SET
// ------------------------------------
package orset

import (
	"fmt"
	"time"
)

type ORMap struct {
	// [KEY]--> [TIMESTAMP]---> [Value]
	addMap    map[string]map[string]string // Map of Keys to a Map of Timestamps that map to a value
	removeMap map[string]map[string]string
}

func NewORMap() *ORMap {
	return &ORMap{
		addMap:    make(map[string]map[string]string),
		removeMap: make(map[string]map[string]string),
	}
}

func (o *ORMap) Add(key string, val string, tag string, old []string) string {
	if am, ok := o.addMap[key]; ok {
		// if key is known do observed removal
		rm, ok := o.removeMap[key]
		if !ok {
			rm = make(map[string]string)
		}
		for _, t := range old {
			rm[t] = am[t]
		}
		o.removeMap[key] = rm
		am[tag] = val
		o.addMap[key] = am
	} else {
		// otherwise add the key to addMap
		am := make(map[string]string)
		am[tag] = val
		o.addMap[key] = am
	}
	o.gc(key)
	return "Success"
}

func (o *ORMap) Remove(key string, old []string) {
	// if key is in the add Map, copy it to the remove map
	if am, ok := o.addMap[key]; ok {
		// check if the key is already in remove Map
		rm, ok := o.removeMap[key]
		if !ok {
			rm = make(map[string]string)
		}
		for _, t := range old {
			rm[t] = am[t]
		}
		o.removeMap[key] = rm
	}
	o.gc(key)
}

func (o *ORMap) Get(key string) string {
	val := ""
	tag := ""
	// get a valid value from addMap for this key
	am, ok := o.addMap[key]
	if ok {
		// compare unique tag string lexicographically
		// return the largest
		for t, v := range am {
			if tag < t {
				val = v
				tag = t
			}
		}
	}
	return val
}

// return all known triplets for key and a new unique tag
func (o *ORMap) GetTriplet(key string) ([]string, string) {
	val := make([]string, 0)
	// get a valid value from addMap for this key
	am, ok := o.addMap[key]
	if ok {
		fmt.Println(am)
		for t := range am {
			val = append(val, t)
		}
	}
	tag := time.Now().Format(time.StampNano)
	return val, tag
}

// merges remote ORMap into local set
func (local *ORMap) Merge(remote *ORMap) {

	for key, r := range remote.addMap {
		l, ok := local.addMap[key]
		if ok {
			for timestamp, val := range r {
				l[timestamp] = val
			}
			continue
		}
		// write the remote to local
		local.addMap[key] = r
	}

	for key, r := range remote.removeMap {
		l, ok := local.removeMap[key]
		if ok {
			for timestamp, val := range r {
				l[timestamp] = val
			}
			continue
		}
		local.removeMap[key] = r
	}
}

// ------------------------------------
// HELPERS
// ------------------------------------
// remove all values that are in both addMap and removeMap
func (o *ORMap) gc(key string) {
	// go through add and remove Maps and delete similar timestamps
	rm, ok := o.removeMap[key]
	if ok {
		am, ok := o.addMap[key]
		if ok {
			// go through all timestamps in removeMap and delete in both maps
			for ts, _ := range rm {
				delete(am, ts)
				delete(rm, ts)
			}
		}
	}
	// Remove empty map values
	if _, ok := o.removeMap[key]; !ok {
		delete(o.removeMap, key)
	}
	if _, ok := o.addMap[key]; !ok {
		delete(o.addMap, key)
	}
}
