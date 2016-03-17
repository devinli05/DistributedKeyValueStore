package main

import (
	//"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

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
var ORset *ORSet

//var availableNodes map[string]bool

const rpcTimeout time.Duration = time.Duration(500) * time.Millisecond

// Reserved value in the service that is used to indicate that the kv-node
// has failed. This will be used in the error message
const kvnodeFailure string = "kvnodeFailure"

// Reserved value in the service that is used to indicate that the key
// is unavailable: used in return values to clients and internally.
const unavail string = "unavailable"

// Registration message of kv-node
type RegMsg struct {
	Id   string //unique Id of kv-node
	Ip   string //ip + port of kv-node
	Keys string //space delimited list of all keys
}

//type NodeService int
type NodeService int

func (ns *NodeService) Get(args *NodeGetArgs, reply *ValReply) error {
	// Acquire mutex for exclusive access to kvmap.
	kvMutex.Lock()
	// Defer mutex unlock to (any) function exit.
	defer kvMutex.Unlock()
	val, contains := kvmap[args.Key]
	if contains {
		reply.Val = val
	} else {
		reply.Val = ""
	}
	fmt.Println("node " + nodeId + " Get key: " + args.Key)
	return nil
}

func (ns *NodeService) Put(args *NodePutArgs, reply *ValReply) error {
	// Acquire mutex for exclusive access to kvmap.
	kvMutex.Lock()
	// Defer mutex unlock to (any) function exit.
	defer kvMutex.Unlock()
	ORset.Add(args.Key, args.Val)
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

// ------------------------------------
//             OR-SET
// ------------------------------------
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

func gossip(gossipID string, gossipAddr string, gossipPort string) {
	var config = memberlist.DefaultLocalConfig()
	config.Name = "Node" + gossipID
	config.BindAddr = gossipAddr
	config.BindPort, _ = strconv.Atoi(gossipPort)

	list, err := memberlist.Create(config)
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}

	_, err = list.Join([]string{"198.162.33.54:4444"})

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
	usage := fmt.Sprintf("Usage: %s [client ip:port] [kv-node ip:port] [r] [ID] [Gossip Addr] [Gossip Port]\n",
		os.Args[0])
	if len(os.Args) != 7 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	clientsIpPort := os.Args[1]
	kvnodesIpPort := os.Args[2]
	replicationFactor := os.Args[3]
	gossipID := os.Args[4]
	gossipAddr := os.Args[5]
	gossipPort := os.Args[6]

	var err error
	ORset = newORSet()

	repFactor, err = strconv.Atoi(replicationFactor)
	checkError(err)
	// TODO: do here the stuff other
	kvMutex = &sync.Mutex{}

	kvNodeAddr, err := net.ResolveUDPAddr("udp", kvnodesIpPort)
	checkError(err)
	pingConn, err := net.ListenUDP("udp", kvNodeAddr)
	checkError(err)
	// Clean up the connection when we exit main().
	defer pingConn.Close()
	// Use channel to block until at least one node joins
	//hasKvNode := make(chan bool, 1)
	//go handleRegistration(pingConn, hasKvNode)
	//<-hasKvNode
	//time.Sleep(time.Second)

	// Setup key-value store and register service.
	kvservice := new(NodeService)
	rpc.Register(kvservice)
	l, e := net.Listen("tcp", clientsIpPort)
	checkError(e)

	go gossip(gossipID, gossipAddr, gossipPort)

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
