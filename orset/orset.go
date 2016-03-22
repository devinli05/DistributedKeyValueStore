// ------------------------------------
//             OR-SET
// ------------------------------------
package orset

import (
	"fmt"
	"time"
)

type ORSet struct {
	addMap    map[string]map[string]string
	removeMap map[string]map[string]string
}

func NewORSet() *ORSet {
	return &ORSet{
		addMap:    make(map[string]map[string]string),
		removeMap: make(map[string]map[string]string),
	}
}

func (o *ORSet) Add(key string, value string) {
	// if the Map already contains the value
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
	fmt.Println(o)
}

func (o *ORSet) Remove(key string, value string) {
	r, ok := o.removeMap[key]
	if !ok {
		r = make(map[string]string)
	}

	if m, ok := o.addMap[key]; ok {
		for timestamp, _ := range m {
			r[timestamp] = value
		}
	}
	o.removeMap[key] = r
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
	var counter int
	m, ok := o.addMap[key]
	if ok {
		total := len(m)
		for timestamp, x := range m {
			if counter < total-1 {
				delete(m, timestamp)
				counter++
				continue
			} else {
				return x
			}
		}
	}
	return ""
}


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
		local.addMapkey] = r
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
// ------------------------------------
