// Simple client to connect to the key-value service and exercise the
// key-value RPC API (put/get/test-set).
//
// Usage: go run kvclientmain.go [ip:port]
//
// - [ip:port] : the ip and TCP port on which the KV service is
//               listening for client connections.
//
// TODOs:
// - Needs refactoring and optional support for vector-timestamps.

package main

import (
	"fmt"
	"net/rpc"
	"os"
)

// args in get(args)
type GetArgs struct {
	Key string // key to look up
}

// args in put(args)
type PutArgs struct {
	Key string // key to associate value with
	Val string // value
}

// args in testset(args)
type TestSetArgs struct {
	Key     string // key to test
	TestVal string // value to test against actual value
	NewVal  string // value to use if testval equals to actual value
}

// Reply from service for all three API calls above.
type ValReply struct {
	Val string // value; depends on the call
}

type KeyValService int

// Main server loop.
func main() {
	// parse args
	usage := fmt.Sprintf("Usage: %s ip:port\n", os.Args[0])
	if len(os.Args) != 2 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	kvAddr := os.Args[1]

	// Connect to the KV-service via RPC.
	kvService, err := rpc.Dial("tcp", kvAddr)
	checkError(err)

	// Use kvVal for all RPC replies.
	var kvVal ValReply

	// Put("my-key", 2016)
	putArgs := PutArgs{
		Key: "my-key",
		Val: "party-like-its-416"}
	err = kvService.Call("NodeService.Put", putArgs, &kvVal)
	checkError(err)
	fmt.Println("KV.put(" + putArgs.Key + "," + putArgs.Val + ") = " + kvVal.Val)
}

// If error is non-nil, print it out and halt.
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		os.Exit(1)
	}
}
