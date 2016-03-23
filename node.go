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

	"./node"
	"github.com/hashicorp/memberlist"
)

// args in get(args)
type NodeGetArgs struct {
	Key string // key to look up
}

// args in put(args)
type NodePutArgs struct {
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
var kvmap map[string]string
var nodeRpc *rpc.Client
var nodeId string
var ors *orset.ORSet

//var availableNodes map[string]bool

const rpcTimeout time.Duration = time.Duration(500) * time.Millisecond

// Reserved value in the service that is used to indicate that the kv-node
// has failed. This will be used in the error message
const kvnodeFailure string = "kvnodeFailure"

// Reserved value in the service that is used to indicate that the key
// is unavailable: used in return values to clients and internally.
const unavail string = "unavailable"

//type NodeService int
type NodeService int

func (ns *NodeService) Get(args *NodeGetArgs, reply *ValReply) error {
	// Acquire mutex for exclusive access to kvmap.
	kvMutex.Lock()
	// Defer mutex unlock to (any) function exit.
	defer kvMutex.Unlock()
	//reply.Val = ""
	reply.Val = ors.Get(args.Key)
	fmt.Println(reply.Val)
	return nil
}

func (ns *NodeService) Put(args *NodePutArgs, reply *ValReply) error {
	// Acquire mutex for exclusive access to kvmap.
	kvMutex.Lock()
	// Defer mutex unlock to (any) function exit.
	defer kvMutex.Unlock()
	ors.Add(args.Key, args.Val)
	reply.Val = "Success"
	return nil
}

func (ns *NodeService) Remove(args *NodePutArgs, reply *ValReply) error {
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
	return nil
}

// TESTSET
func (kvs *NodeService) TestSet(args *NodeTestSetArgs, reply *ValReply) error {
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
	return nil
}

// ----------------------------------------
//				GOSSIP PROTOCOL
// ----------------------------------------

// Configuration
// Name - Must be unique
// BindAddr & BindPort - Address and Port to use for Gossip communication
//	var config = memberlist.DefaultLocalConfig()
//	config.Name = "Node" + gossipID
//	config.BindAddr = gossipAddr
//	config.BindPort, err = strconv.Atoi(gossipPort)

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
	for {
		time.Sleep(time.Second * 5)
		fmt.Println("Members:")
		for _, member := range list.Members() {
			fmt.Printf("%s %s %d\n", member.Name, member.Addr, member.Port)
		}
		fmt.Println()
	}

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

	//clientsIpPort := os.Args[1]
	//kvnodesIpPort := os.Args[2]
	replicationFactor := os.Args[1]
	gossipID := os.Args[2]
	//gossipAddr := os.Args[5]
	//gossipPort := os.Args[6]

	nodeId := gossipID
	config, err := ioutil.ReadFile("./config.json")
	checkError(err)
	//fmt.Printf("%s\n", string(config))
	var nodes map[string]string
	err = json.Unmarshal(config, &nodes)
	checkError(err)
	//fmt.Printf("Results: %v\n", nodes)

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
	clientsIpPort := gossipAddr.IP.String() + ":666" + nodeId
	kvnodesIpPort := gossipAddr.IP.String() + ":555" + nodeId
	//nodeKvnodeAddr, err := net.ResolveTCPAddr("tcp", nodeIpPort)
	//checkError(err)
	//nodeClientAddr, err := net.ResolveTCPAddr("tcp", nodeIpPort)
	//checkError(err)

	nodeRpcList := []string{"127.0.0.1:5550", "127.0.0.1:5551", "127.0.0.1:5552", "127.0.0.1:5553"}
	consHash := chash.New(1024, nodeRpcList, nil)
	fmt.Println(consHash.Find("0"))
	fmt.Println(consHash.Find("1"))
	ors = orset.NewORSet()

	repFactor, err = strconv.Atoi(replicationFactor)
	checkError(err)
	kvMutex = &sync.Mutex{}

	kvNodeAddr, err := net.ResolveUDPAddr("udp", kvnodesIpPort)
	checkError(err)
	pingConn, err := net.ListenUDP("udp", kvNodeAddr)
	checkError(err)
	// Clean up the connection when we exit main().
	defer pingConn.Close()

	// Setup key-value store and register service.
	kvservice := new(NodeService)
	rpc.Register(kvservice)
	l, e := net.Listen("tcp", clientsIpPort)
	checkError(e)
	// Clean up the connection when we exit main().
	defer l.Close()
	for {
		nodeConn, err := l.Accept()
		checkError(err)
		go rpc.ServeConn(nodeConn)
	}
}

// If error is non-nil, print it out and halt.
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		os.Exit(1)
	}
}
