// ------------------------------------
//             OR-SET
// ------------------------------------
package orset

import (
	"time"
)

type ORSet struct {
	// [KEY]--> [TIMESTAMP]---> [Value]
	addMap    map[string]map[string]string  // Map of Keys to a Map of Timestamps that map to a value
	removeMap map[string]map[string]string
}

func NewORSet() *ORSet {
	return &ORSet{
		addMap:    make(map[string]map[string]string),
		removeMap: make(map[string]map[string]string),
	}
}

func (o *ORSet) Add(key string, value string) {
	// clean up maps
	cleanUpMaps(o, key)

	// if the Map already contains the key, set timestamps
	if m, ok := o.addMap[key]; ok {
		timestamp := time.Now().Format(time.StampNano)
		m[timestamp] = value
	} else {
		// otherwise add the value to the map
		m := make(map[string]string)
		timestamp := time.Now().Format(time.StampNano)
		m[timestamp] = value
		o.addMap[key] = m
	}
}

func (o *ORSet) Remove(key string, value string) {
	// if key is in the add Map, copy it to the remove map 
	if am, ok := o.addMap[key]; ok {
		// check if the key is already in remove Map
		rm, ok := o.removeMap[key]
		if !ok {
			rm = make(map[string]string)
		}
		for timestamp, v := range am {
			if v == value {
				rm[timestamp] = v
			}
		}
	o.removeMap[key] = rm	
	}
}

func (o *ORSet) Contains(key string) bool {
	addMap, ok := o.addMap[key]
	if !ok {
		return false
	}

	removeMap, ok := o.removeMap[key]
	if !ok {
		return true
	}

	for timestamp, _ := range addMap {
		if _, ok := removeMap[timestamp]; !ok {
			return true
		}
	}

	return false
}

func (o *ORSet) Get(key string) string {

	var value string
	// get a valid value from addMap for this key
	am, ok := o.addMap[key]
	if ok {
		for _, v := range am {
			value = v
		}
		return value
	}
	// else return nothing
	return ""
}

// merges remote ORSet into local set
func (local *ORSet) Merge(remote *ORSet) {

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
func cleanUpMaps(o *ORSet, key string) {
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
