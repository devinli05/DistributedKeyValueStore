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
	"net"
	"net/rpc"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/arcaneiceman/GoVector/govec"
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

// ----------------------------------------
// GoVector Logging
// ----------------------------------------
var Logger *govec.GoLog
var LogMutex *sync.Mutex

//Msg is the message sent over the network
//Msg is capitalized so GoVecs encoder can acess it
//Furthermore its variables are capitalized to make them public
type Msg struct {
	Content, RealTimestamp string
}

func (m Msg) String() string {
	return "content: " + m.Content + "\ntime: " + m.RealTimestamp
}

// Helper Function to sendMsgLog
// Opens a connection to remoteAddr from localAddr
// returns that connection
func openConnection(localAddr, remoteAddr string) *net.UDPConn {

	_, port, err := net.SplitHostPort(localAddr)
	errorCheck(err, "Something is Wrong with the given local address format")

	port = ":" + port

	laddr, err := net.ResolveUDPAddr("udp", port)
	errorCheck(err, "Something is Wrong with the given local address")

	raddr, err := net.ResolveUDPAddr("udp", remoteAddr)
	errorCheck(err, "Something is Wrong with the given remote address")

	conn, err := net.DialUDP("udp", laddr, raddr)
	errorCheck(err, "Something has gone wrong in the initial connection")

	return conn
}

// Open a connection to listeningAddr from sendingAddr
// Log msg then send, close connection
// Format for addresses is "127.0.0.1:8080"
func sendMsgLog(sendingAddr string, listeningAddr string, msg string) {

	outgoingMessage := Msg{msg, time.Now().String()}

	LogMutex.Lock()
	outBuf := Logger.PrepareSend("Sending message to server", outgoingMessage)
	LogMutex.Unlock()

	conn := openConnection(sendingAddr, listeningAddr)

	_, err := conn.Write(outBuf)

	errorCheck(err, "Problem with Sending String: "+outgoingMessage.String())

	conn.Close()
	return
}

// An Infinite Loop
// Format for listeningAddr is "127.0.0.1:8080"
// Opens a connection on listeningAddr, waits for a udp packet
// Upon receiving a packet, Logs the packet's message and
// waits for another packet
func receiveMessagesLog(listeningAddr string) {
	var buf [512]byte

	conn, err := net.ListenPacket("udp", listeningAddr)
	errorCheck(err, "Problem Listening for Packets")

	// Make sure connection is properly closed on program exit
	defer conn.Close()

	// Infinite Loop
	for {
		conn.ReadFrom(buf[0:])
		incommingMessage := new(Msg)

		LogMutex.Lock()
		Logger.UnpackReceive("Received Message", buf[0:], &incommingMessage)
		LogMutex.Unlock()

		LogLocalEvent("Message Says: " + incommingMessage.String())
	}

	return
}

func errorCheck(err error, message string) {

	if err != nil {
		fmt.Println(message)
		fmt.Printf("%s\n", err)
	}
}

func LogLocalEvent(msg string) {
	LogMutex.Lock()
	Logger.LogLocalEvent("Message Says: " + msg)
	LogMutex.Unlock()
}

// ----------------------------------------
// ----------------------------------------

// Main server loop.
func main() {
	// parse args
	usage := fmt.Sprintf("Usage: %s ip:port\n", os.Args[0])
	if len(os.Args) != 2 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	kvAddr := os.Args[1]

	// Set up GoVector Logging
	Logger = govec.Initialize("DummyClient", "DummyClient")
	LogMutex = &sync.Mutex{}
	go receiveMessagesLog("localhost:8888")

	// Determine ID of node dummyclient will connect to
	re := regexp.MustCompile(":666.+")
	nodeId := re.FindString(kvAddr)
	nodeId = strings.Replace(nodeId, ":666", "", -1)
	fmt.Println(nodeId)

	// Determine IP of node dummyclient will connect to
	re = regexp.MustCompile(".+:666")
	nodeIP := re.FindString(kvAddr)
	nodeIP = strings.Replace(nodeIP, ":666", "", -1)
	fmt.Println(nodeIP)
	// Connect to the KV-service via RPC.
	kvService, err := rpc.Dial("tcp", kvAddr)
	checkError(err)

	// Use kvVal for all RPC replies.
	var kvVal ValReply

	//	kvVal.Val = ""
	// Put test
	putArgs := PutArgs{
		Key: "test-key",
		Val: "testing 1 2 3"}
	sendMsgLog("localhost:8880", nodeIP+":777"+nodeId, "RPC Put Call to "+kvAddr)
	err = kvService.Call("NodeService.Put", putArgs, &kvVal)
	checkError(err)
	fmt.Println("KV.put(" + putArgs.Key + "," + putArgs.Val + ") = " + kvVal.Val)

	//	kvVal.Val = ""
	// Put test
	putArgs = PutArgs{
		Key: "test-key",
		Val: "testing 1 2 3 4"}

	sendMsgLog("localhost:8880", nodeIP+":777"+nodeId, "RPC Put Call to "+kvAddr)
	err = kvService.Call("NodeService.Put", putArgs, &kvVal)
	checkError(err)
	fmt.Println("KV.put(" + putArgs.Key + "," + putArgs.Val + ") = " + kvVal.Val)

	//	kvVal.Val = ""
	getArgs := GetArgs{
		Key: "test-key"}
	sendMsgLog("localhost:8880", nodeIP+":777"+nodeId, "RPC Get Call to "+kvAddr)
	err = kvService.Call("NodeService.Get", getArgs, &kvVal)
	checkError(err)
	fmt.Println("KV.get(" + getArgs.Key + ") = " + kvVal.Val)
}

// If error is non-nil, print it out and halt.
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		os.Exit(1)
	}
}
