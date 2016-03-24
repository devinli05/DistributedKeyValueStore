package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"./chash"
	"./orset"
	"github.com/arcaneiceman/GoVector/govec"
	"github.com/hashicorp/memberlist"
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

//args in remove(args)
type NodeRemoveArgs struct {
	Key string // key associated with value
	Val string // value
}

// args in testset(args)
type NodeTestSetArgs struct {
	Key     string // key to test
	TestVal string // value to test against actual value
	NewVal  string // value to use if testval equals to actual value
}

// Reply from service for all three API calls above.
type ValReply struct {
	Val string // value; depends on the call
}

var kvMutex *sync.Mutex
var repFactor int
var nodeId string
var ors *orset.ORSet
var consHash *chash.Ring
var govecUdpAddrMap map[string]string
var nodesRpcAddrMap map[string]string
var rpcAddrRpcCliMap map[string]*rpc.Client

const rpcTimeout time.Duration = time.Duration(500) * time.Millisecond

// Reserved value in the service that is used to indicate that the kv-node
// has failed. This will be used in the error message
const kvnodeFailure string = "kvnodeFailure"

type NodeService int

func getOwnerRpcClient(ownerRpcAddr string) *rpc.Client {
	var err error
	ownerRpcClient, contains := rpcAddrRpcCliMap[ownerRpcAddr]
	if !contains {
		ownerRpcClient, err = rpc.Dial("tcp", ownerRpcAddr)
		checkError(err)
		rpcAddrRpcCliMap[ownerRpcAddr] = ownerRpcClient
	}
	return ownerRpcClient
}

func (ns *NodeService) Get(args *GetArgs, reply *ValReply) error {
	var err error = nil
	ownerRpcAddr := consHash.Find(args.Key)
	//ownerRpcAddr := "127.0.0.1:6661"
	if strings.EqualFold(ownerRpcAddr, nodesRpcAddrMap[nodeId]) {
		kvMutex.Lock()
		defer kvMutex.Unlock()
		reply.Val = ors.Get(args.Key)
		LogLocalEvent("Get Function - Key: " + args.Key + " Reply: " + reply.Val)
	} else {
		ownerRpcClient, contains := rpcAddrRpcCliMap[ownerRpcAddr]
		if !contains {
			ownerRpcClient, err = rpc.Dial("tcp", ownerRpcAddr)
			checkError(err)
			rpcAddrRpcCliMap[ownerRpcAddr] = ownerRpcClient
		}
		err = ownerRpcClient.Call("NodeService.Get", args, reply)
		checkError(err)
	}
	return err
}

func (ns NodeService) Put(args *PutArgs, reply *ValReply) error {
	var err error = nil
	ownerRpcAddr := consHash.Find(args.Key)
	//ownerRpcAddr := "127.0.0.1:6661"
	fmt.Println(" owner: " + ownerRpcAddr)
	fmt.Println("whoami: " + nodesRpcAddrMap[nodeId])
	if strings.EqualFold(ownerRpcAddr, nodesRpcAddrMap[nodeId]) {
		kvMutex.Lock()
		defer kvMutex.Unlock()
		ors.Add(args.Key, args.Val)
		reply.Val = "Success"
		LogLocalEvent("Put Function - Key: " + args.Key + " Reply: " + reply.Val)
	} else {
		ownerRpcClient, contains := rpcAddrRpcCliMap[ownerRpcAddr]
		if !contains {
			ownerRpcClient, err = rpc.Dial("tcp", ownerRpcAddr)
			checkError(err)
			rpcAddrRpcCliMap[ownerRpcAddr] = ownerRpcClient
		}
		err = ownerRpcClient.Call("NodeService.Put", args, reply)
		checkError(err)
	}
	return err
}

/*
func (ns *NodeService) Remove(args *PutArgs, reply *ValReply) error {
	// Acquire mutex for exclusive access to kvmap.
	kvMutex.Lock()
	// Defer mutex unlock to (any) function exit.
	defer kvMutex.Unlock()
	val := kvmap[args.Key]
	if val == unavail {
		reply.Val = unavail
	} else {
		kvmap[args.Key] = args.Val
		reply.Val = ""
	}
	fmt.Println("node " + nodeId + " remove key: " + args.Key + " val: " + args.Val)
	LogLocalEvent("Remove Function - node " + nodeId + " remove key: " + args.Key + " val: " + args.Val)
	return nil
}

// TESTSET
func (kvs *KeyValService) TestSet(args *NodeTestSetArgs, reply *ValReply) error {
	// Acquire mutex for exclusive access to kv nodes.
	kvMutex.Lock()
	// Defer mutex unlock to (any) function exit.
	defer kvMutex.Unlock()
	val, contains := kvmap[args.Key]
	if contains {
		if args.TestVal == val {
			kvmap[args.Key] = args.NewVal
			reply.Val = args.NewVal
		} else {
			reply.Val = val
		}
	} else {
		reply.Val = ""
	}

	LogLocalEvent("Testset Function - Key:" + args.Key + " Old Value: " + args.TestVal + " New Value: " + args.NewVal + " Return Value: " + reply.Val)
	return nil
}
*/
// ----------------------------------------
// GOSSIP PROTOCOL
// ----------------------------------------

// Configuration
// Name - Must be unique
// BindAddr & BindPort - Address and Port to use for Gossip communication

func gossip(gossipID string, gossipAddr string, gossipPort int, bootstrapAddr string) {
	var config = memberlist.DefaultLocalConfig()
	config.Name = "Node" + gossipID
	config.BindAddr = gossipAddr
	config.BindPort = gossipPort

	list, err := memberlist.Create(config)
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}

	_, err = list.Join([]string{bootstrapAddr})

	// Infinite Loop
	// Every 5 seconds Print out all members in cluster
	//for {
	//time.Sleep(time.Second * 5)
	//fmt.Println("Members:")
	//for _, member := range list.Members() {
	//	fmt.Printf("%s %s %d\n", member.Name, member.Addr, member.Port)
	//}
	//fmt.Println()
	//}

}

// ----------------------------------------
// ----------------------------------------

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
	// Parse args.
	usage := fmt.Sprintf("Usage: %s [ReplicationFactor] [NodeID]\n",
		os.Args[0])
	if len(os.Args) != 3 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	replicationFactor := os.Args[1]
	gossipID := os.Args[2]

	nodeId = gossipID
	config, err := ioutil.ReadFile("./config.json")
	checkError(err)
	var nodes map[string]string
	err = json.Unmarshal(config, &nodes)
	checkError(err)

	// Set up gossip protocol libary using known ip:port of node 0 to bootstrap
	bootstrapAddr := nodes["0"]
	nodeIpPort := ""
	if strings.EqualFold(nodeId, "0") {
		nodeIpPort = nodes[nodeId]
	} else {
		nodeIpPort = nodes[nodeId] + ":0"
	}
	gossipAddr, err := net.ResolveUDPAddr("udp", nodeIpPort)
	checkError(err)
	go gossip(gossipID, gossipAddr.IP.String(), gossipAddr.Port, bootstrapAddr)

	nodeIdList := make([]string, 0)
	govecUdpAddrMap = make(map[string]string)
	nodesRpcAddrMap = make(map[string]string)
	rpcAddrRpcCliMap = make(map[string]*rpc.Client)
	for id := range nodes {
		nodeIdList = append(nodeIdList, id)
		govecUdpAddrMap[id] = gossipAddr.IP.String() + ":777" + id
		nodesRpcAddrMap[id] = gossipAddr.IP.String() + ":666" + id
	}
	myLogAddr := govecUdpAddrMap[nodeId]
	myRpcAddr := nodesRpcAddrMap[nodeId]

	// Set up GoVector Logging
	Logger = govec.Initialize(nodeId, nodeId)
	LogMutex = &sync.Mutex{}
	// Used for GoVector Logging
	go receiveMessagesLog(myLogAddr)

	//kvnodeRpcList := make([]string, 0)
	//for id := range nodes {
	//	kvnodeRpcList = append(kvnodeRpcList, nodesRpcAddrMap[id])
	//}
	kvnodeRpcList := []string{"127.0.0.1:6660", "127.0.0.1:6661", "127.0.0.1:6662", "127.0.0.1:6663"}
	numberPortions := 8
	var hashFunction chash.Hash = nil
	consHash = chash.New(numberPortions, kvnodeRpcList, hashFunction)
	fmt.Println(consHash)
	ors = orset.NewORSet()

	repFactor, err = strconv.Atoi(replicationFactor)
	checkError(err)
	kvMutex = &sync.Mutex{}

	// node RPC service setup
	rpc.Register(new(NodeService))
	myRpcListener, err := net.Listen("tcp", myRpcAddr)
	checkError(err)
	defer myRpcListener.Close()
	// infinite loop to serve RPC connections
	for {
		conn, err := myRpcListener.Accept()
		checkError(err)
		go rpc.ServeConn(conn)
	}
}

// If error is non-nil, print it out and halt.
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		os.Exit(1)
	}
}
