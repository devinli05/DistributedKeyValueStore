package orset

import (
	"fmt"
	// "strconv"
	"testing"
)
var ors *ORSet 


var KVs = []struct {
  key string // 
  val string // 
}{
  {"Alpha", "1"},
  {"Beta", "1"},
  {"Gamma", "1"},
  {"Alpha", "2"},
  {"Alpha", "3"},
  {"Gamma", "2"},
  {"Beta", "2"},
}

func setUp() {
	ors = NewORSet()
	fmt.Print("Setup Complete============================\n")
}



func TestAdd(t *testing.T) {
	setUp()
	fmt.Print("Test Add============================\n")

	ors.Add("Alpha", "2")
	ors.Add("Beta", "2")
	ors.Add("Beta", "55")
	ors.Add("Alpha", "2")

	Print(ors)
}

func TestRemove(t *testing.T) {
	fmt.Print("Test Remove============================\n")
	ors.Remove("Alpha", "2")
	ors.Remove("Alpha", "2")
	ors.Remove("Beta", "2")
	Print(ors)
}

func TestGet(t *testing.T) {
	fmt.Print("Test Get============================\n")
	value := "55"
	val := ors.Get("Beta")
	if val != value {
		t.Fatalf("expected %s, got %s.", value, val)
	}
	fmt.Printf("Val: %v\n", val)
	Print(ors)
}

func TestMerge(t *testing.T) {
	fmt.Print("Test MERGE===========================\n")
	local := NewORSet()
	remote := NewORSet()


	local.Add("TESLA", "A")
	remote.Add("TESLA", "B")
	remote.Add("VOLT", "X")
	local.Add("LEAF", "Q")
	remote.Add("LEAF", "R")
	local.Add("TESLA", "C")

	PrintMerge(local, remote)


	local.Merge(remote)
	fmt.Print("<==== MERGE 1 (LOCAL <-- REMOTE) COMPLETE===========================\n")
	PrintMerge(local, remote)


	remote.Merge(local)
	fmt.Print("<==== MERGE 2 (LOCAL --> REMOTE) COMPLETE===========================\n")
	PrintMerge(local, remote)

	// WITH REMOVALS
	local.Remove("TESLA", "B")
	remote.Remove("LEAF", "Q")

	fmt.Print("<==== REMOVED SOME ELEMENTS COMPLETE===========================\n")
	PrintMerge(local, remote)


	local.Merge(remote)
	fmt.Print("<==== MERGE 3 (LOCAL <-- REMOTE) COMPLETE===========================\n")
	PrintMerge(local, remote)

	remote.Merge(local)
	fmt.Print("<==== MERGE 4 (LOCAL --> REMOTE) COMPLETE===========================\n")
	PrintMerge(local, remote)
}


func Print(o *ORSet) {
	fmt.Printf("addMap: %v\n", o.addMap)
	fmt.Printf("removeMap: %v\n", o.removeMap)
} 

func PrintMerge(local *ORSet, remote *ORSet) {
	fmt.Print("LOCAL: ")
	Print(local)
	fmt.Print("REMOTE: ")
	Print(remote)
}
