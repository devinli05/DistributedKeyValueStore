// ------------------------------------
//             OR-SET
// ------------------------------------
package orset

import (
	"fmt"
	"os"
	"sync"
	"time"
	"./orset"
)

type ORSet struct {
	addMap    map[string]map[string]string
	removeMap map[string]map[string]string
}

func newORSet() *ORSet {
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

func (o *ORSet) Merge(r *ORSet) {
	for key, m := range r.addMap {
		addMap, ok := o.addMap[key]
		if ok {
			for timestamp, _ := range m {
				addMap[timestamp] = ""
			}
			continue
		}
		o.addMap[key] = m
	}

	for key, m := range r.removeMap {
		removeMap, ok := o.removeMap[key]
		if ok {
			for timestamp, _ := range m {
				removeMap[timestamp] = ""
			}
			continue
		}
		o.removeMap[key] = m
	}
}

// ------------------------------------
// ------------------------------------