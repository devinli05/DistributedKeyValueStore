package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"./chash"
	"./handler"
	"./orset"
	"github.com/arcaneiceman/GoVector/govec"
	"github.com/hashicorp/memberlist"
)

type udpComm struct {
	Type    string
	Key     string
	Val     string
	TestVal string
	NewVal  string
	Status  string
}

type udpOrsetBuild struct {
	Keys   []string
	Values []string
	Status string
}

var udpPortMutex *sync.Mutex
var kvMutex *sync.Mutex
var repFactor int
var nodeId string

// Addr used for Sending UDP packets
var nodeUDPAddr string
var nodeOrsetBuildAddr string

var ors *orset.ORSet
var consHash *chash.Ring

var nodesUDPAddrMap map[string]string

var nodes map[string]string
var nodeIdList []string
var inactiveNodes map[string]bool
var proxyNodes map[string]string
var activeKeys []string

type ActiveList struct{}

func (list ActiveList) NotifyJoin(n *memberlist.Node) {
	fmt.Println(n.Name + " joined")
}

func (list ActiveList) NotifyLeave(n *memberlist.Node) {
	fmt.Println(n.Name + " left")
	s := strings.Split(n.Name, "Node")
	_, ID := s[0], s[1]
	fmt.Println(ID)
	inactiveNodes[ID] = true
	int_ID, _ := strconv.Atoi(ID)
	proxyInt := int_ID + repFactor - 1
	fmt.Println(proxyInt)
	for {
		if proxyInt < len(nodeIdList) {
			proxyCheck := strconv.Itoa(proxyInt)
			if _, ok := inactiveNodes[proxyCheck]; !ok {
				fmt.Println("Proxy for " + ID + " = " + proxyCheck)
				proxyNodes[ID] = proxyCheck
				break
			} else {
				proxyInt++
			}
		} else if proxyInt >= len(nodeIdList) {
			proxyInt = proxyInt % len(nodeIdList)
			fmt.Println("Estimated proxy = ")
			fmt.Println(proxyInt)
		}
	}
	return
}

func (list ActiveList) NotifyUpdate(n *memberlist.Node) {
	fmt.Println(n.Name + " updated")
}

func handleRequestUDP() {
	conn := startListening(nodesUDPAddrMap[nodeId])
	defer conn.Close()

	for {
		packet, retAddr := readMessage(conn)
		fmt.Println(nodeId)
		fmt.Println("Got a packet")
		go handleRequestUDPHelper(packet, retAddr)
	}

}

func handleRequestUDPHelper(packet *udpComm, retAddr *net.UDPAddr) {

	if packet.Type == "Get" {
		fmt.Println("Got a GET packet")
		retVal := Getudp(packet)
		fmt.Println("Return Value: " + retVal.Val)
		udpPortMutex.Lock()
		fmt.Println("Open Connection to: " + retAddr.String() + " from: " + nodeUDPAddr)
		conn := openConnection(nodeUDPAddr, retAddr.String())
		fmt.Println("Prepare to send response, status: " + retVal.Status)
		LogMutex.Lock()
		fmt.Println("Sending : " + retVal.Type)
		outBuf := Logger.PrepareSend("Sending :"+retVal.Type, retVal)
		LogMutex.Unlock()
		fmt.Println("Sending Response to: " + retAddr.String())

		//laddr, err := net.ResolveUDPAddr("udp", retAddr.String())
		//errorCheck(err, "Something is Wrong with the given local address")
		conn.WriteTo(outBuf, retAddr)

		//conn.Write(outBuf)
		conn.Close()
		udpPortMutex.Unlock()

	} else if packet.Type == "Remove" {

		fmt.Println("Got a REMOVE packet")

		retVal := Removeudp(packet)
		fmt.Println("Return Value: " + retVal.Val)
		udpPortMutex.Lock()
		fmt.Println("Open Connection to: " + retAddr.String() + " from: " + nodeUDPAddr)
		conn := openConnection(nodeUDPAddr, retAddr.String())
		fmt.Println("Prepare to send response, status: " + retVal.Status)
		LogMutex.Lock()
		fmt.Println("Sending : " + retVal.Type)
		outBuf := Logger.PrepareSend("Sending :"+retVal.Type, retVal)
		LogMutex.Unlock()
		fmt.Println("Sending Response to: " + retAddr.String())

		//laddr, err := net.ResolveUDPAddr("udp", retAddr.String())
		//errorCheck(err, "Something is Wrong with the given local address")
		conn.WriteTo(outBuf, retAddr)

		//conn.Write(outBuf)
		conn.Close()
		udpPortMutex.Unlock()

	} else if packet.Type == "Put" {
		fmt.Println("Got a PUT packet")
		if strings.EqualFold(packet.Status, "Store") {
			retVal := Putudp(packet)
			fmt.Println("Return Value: " + retVal.Val)
			udpPortMutex.Lock()
			fmt.Println("Open Connection to: " + retAddr.String() + " from: " + nodeUDPAddr)
			conn := openConnection(nodeUDPAddr, retAddr.String())
			fmt.Println("Prepare to send response, status: " + retVal.Status)
			LogMutex.Lock()
			fmt.Println("Sending : " + retVal.Type)
			outBuf := Logger.PrepareSend("Sending : "+retVal.Type, retVal)
			LogMutex.Unlock()
			fmt.Println("Sending Response to: " + retAddr.String())

			conn.WriteTo(outBuf, retAddr)
			conn.Close()
			udpPortMutex.Unlock()
		} else {
			retVal := replicate(packet)
			fmt.Println("Return Value for replicate: " + retVal.Val)
			udpPortMutex.Lock()
			fmt.Println("Open Connection to: " + retAddr.String() + " from: " + nodeUDPAddr)
			conn := openConnection(nodeUDPAddr, retAddr.String())
			fmt.Println("Prepare to send response, status: " + retVal.Status)
			LogMutex.Lock()
			fmt.Println("Sending : " + retVal.Type)
			outBuf := Logger.PrepareSend("Sending : "+retVal.Type, retVal)
			LogMutex.Unlock()
			fmt.Println("Sending Response to: " + retAddr.String())

			conn.WriteTo(outBuf, retAddr)
			conn.Close()
			udpPortMutex.Unlock()
		}
	} else if packet.Type == "Build" {
		fmt.Println("Got a Build Packet from: " + retAddr.String())
		retVal := createNodeOrset(packet.Status)
		fmt.Println("Keys:")
		fmt.Println(retVal.Keys)
		udpPortMutex.Lock()
		fmt.Println("Build packet get udp lock")
		conn := openConnection(nodeUDPAddr, retAddr.String())
		fmt.Println("Connection open to reply to Build packet")
		LogMutex.Lock()
		fmt.Println("Build packet handle just got log lock")
		fmt.Println(retVal)
		outBuf := Logger.PrepareSend("Sending Build Request", retVal)
		fmt.Println("attempt to release govector lock")
		LogMutex.Unlock()
		fmt.Println("sending build packet response")
		conn.WriteTo(outBuf, retAddr)
		conn.Close()
		udpPortMutex.Unlock()
		fmt.Println("finished with build packet request")

	} else {
		//TESTSET
		fmt.Println("Got a TESTSET packet")
		retVal := TestSetudp(packet)
		fmt.Println("Return Value: " + retVal.Val)
		udpPortMutex.Lock()
		fmt.Println("Open Connection to: " + retAddr.String() + " from: " + nodeUDPAddr)
		conn := openConnection(nodeUDPAddr, retAddr.String())
		fmt.Println("Prepare to send response, status: " + retVal.Status)
		LogMutex.Lock()
		fmt.Println("Sending : " + retVal.Type)
		outBuf := Logger.PrepareSend("Sending :"+retVal.Type, retVal)
		LogMutex.Unlock()
		fmt.Println("Sending Response to: " + retAddr.String())

		//laddr, err := net.ResolveUDPAddr("udp", retAddr.String())
		//errorCheck(err, "Something is Wrong with the given local address")
		conn.WriteTo(outBuf, retAddr)

		//conn.Write(outBuf)
		conn.Close()
		udpPortMutex.Unlock()
	}
}

func startListening(localAddr string) *net.UDPConn {

	laddr, err := net.ResolveUDPAddr("udp", localAddr)
	errorCheck(err, "Something is Wrong with the given local address")

	conn, err := net.ListenUDP("udp", laddr)
	errorCheck(err, "Something Went Wrong with Listening for UDP Packets")

	return conn
}

func readMessage(conn *net.UDPConn) (*udpComm, *net.UDPAddr) {

	buffer := make([]byte, 1024)
	bytesRead, retAddr, err := conn.ReadFromUDP(buffer)

	errorCheck(err, "Problem with Reading UDP Packet")
	packet := new(udpComm)
	LogMutex.Lock()
	Logger.UnpackReceive("Receive Message", buffer[:bytesRead], &packet)
	LogMutex.Unlock()

	return packet, retAddr
}

func TestSetudp(packet *udpComm) *udpComm {

	ownerId := consHash.Find(packet.Key)
	for {
		if _, ok := inactiveNodes[ownerId]; ok {
			iterate, _ := strconv.Atoi(ownerId)
			iterate++
			ownerId = strconv.Itoa(iterate)
		} else {
			break
		}
	}
	ownerUDPAddr := nodesUDPAddrMap[ownerId]

	if strings.EqualFold(ownerUDPAddr, nodesUDPAddrMap[nodeId]) {
		fmt.Println("I " + nodeId + " have the value")
		kvMutex.Lock()
		defer kvMutex.Unlock()

		var retVal = ors.Get(packet.Key)
		fmt.Println("Got value: " + retVal)

		if retVal == packet.TestVal {
			LogLocalEvent("Local TESTSET TestVal matches current Val")
			ors.Add(packet.Key, packet.NewVal)

			if !sliceContains(activeKeys, packet.Key) {
				activeKeys = append(activeKeys, packet.Key)
			}

			testset := &udpComm{
				Type:    "TestSet",
				Key:     packet.Key,
				Val:     packet.NewVal,
				TestVal: "",
				NewVal:  "",
				Status:  "Success",
			}
			fmt.Println("Returned from TestSetudp")
			return testset

		} else {
			LogLocalEvent("Local TESTSET TestVal did not Match current Val")

			testset := &udpComm{
				Type:    "TestSet",
				Key:     packet.Key,
				Val:     retVal,
				TestVal: "",
				NewVal:  "",
				Status:  "Failure",
			}
			fmt.Println("Returned from TestSetudp")
			return testset
		}

	} else {
		fmt.Println("Got request for Packet I (" + nodeId + ") dont have")
		testset := &udpComm{
			Type:    "TestSet",
			Key:     packet.Key,
			Val:     packet.Val,
			TestVal: packet.TestVal,
			NewVal:  packet.NewVal,
			Status:  "Request",
		}
		LogMutex.Lock()
		msg := Logger.PrepareSend("Sending Message", testset)
		LogMutex.Unlock()
		udpPortMutex.Lock()
		conn := openConnection(nodeUDPAddr, ownerUDPAddr)

		laddr, err := net.ResolveUDPAddr("udp", ownerUDPAddr)
		errorCheck(err, "Something is Wrong with the given local address")
		fmt.Println("Send request to " + ownerUDPAddr + " From " + nodeUDPAddr)

		conn.WriteTo(msg, laddr)
		//conn.Write(msg)

		conn.Close()
		conn = openConnection(nodeUDPAddr, ownerUDPAddr)

		fmt.Println("Wait for response")
		packet, _ := readMessage(conn)
		fmt.Println("Returned to TestSetUDP function after got a packet")
		conn.Close()
		udpPortMutex.Unlock()
		fmt.Println("Got a response")
		//incomingMessage := new(udpComm)
		//Logger.UnpackReceive("Received Message", buf, &incomingMessage)

		return packet
	}
}

func Getudp(packet *udpComm) *udpComm {

	ownerId := consHash.Find(packet.Key)
	for {
		if _, ok := inactiveNodes[ownerId]; ok {
			iterate, _ := strconv.Atoi(ownerId)
			iterate++
			ownerId = strconv.Itoa(iterate)
		} else {
			break
		}
	}
	ownerUDPAddr := nodesUDPAddrMap[ownerId]

	if strings.EqualFold(ownerUDPAddr, nodesUDPAddrMap[nodeId]) {
		fmt.Println("I " + nodeId + " have the value")
		kvMutex.Lock()
		defer kvMutex.Unlock()
		var retVal = ors.Get(packet.Key)
		fmt.Println("Got value: " + retVal)

		LogLocalEvent("Local GET returned: " + retVal)

		fmt.Println("Released Log Lock")
		get := &udpComm{
			Type:    "Get",
			Key:     packet.Key,
			Val:     retVal,
			TestVal: "",
			NewVal:  "",
			Status:  "Success",
		}
		fmt.Println("Returned from Getudp")
		return get

	} else {
		fmt.Println("Got requets for Packet I (" + nodeId + ") dont have")
		get := &udpComm{
			Type:    "Get",
			Key:     packet.Key,
			Val:     "",
			TestVal: "",
			NewVal:  "",
			Status:  "Request",
		}
		LogMutex.Lock()
		msg := Logger.PrepareSend("Sending Message", get)
		LogMutex.Unlock()
		udpPortMutex.Lock()
		conn := openConnection(nodeUDPAddr, ownerUDPAddr)

		laddr, err := net.ResolveUDPAddr("udp", ownerUDPAddr)
		errorCheck(err, "Something is Wrong with the given local address")
		fmt.Println("Send request to " + ownerUDPAddr + " From " + nodeUDPAddr)

		conn.WriteTo(msg, laddr)
		//conn.Write(msg)

		conn.Close()
		conn = openConnection(nodeUDPAddr, ownerUDPAddr)

		fmt.Println("Wait for response")
		packet, _ := readMessage(conn)
		fmt.Println("Returnd to getUDP function after got a packet")
		conn.Close()
		udpPortMutex.Unlock()
		fmt.Println("Got a response")
		//incomingMessage := new(udpComm)
		//Logger.UnpackReceive("Received Message", buf, &incomingMessage)

		return packet
	}
}

func Removeudp(packet *udpComm) *udpComm {

	ownerId := consHash.Find(packet.Key)
	for {
		if _, ok := inactiveNodes[ownerId]; ok {
			iterate, _ := strconv.Atoi(ownerId)
			iterate++
			ownerId = strconv.Itoa(iterate)
		} else {
			break
		}
	}
	ownerUDPAddr := nodesUDPAddrMap[ownerId]

	if strings.EqualFold(ownerUDPAddr, nodesUDPAddrMap[nodeId]) {
		fmt.Println("I " + nodeId + " have the value")
		kvMutex.Lock()
		defer kvMutex.Unlock()
		ors.Remove(packet.Key, packet.Val)
		fmt.Println("Remove value: " + packet.Val + " Key: " + packet.Key)

		LogLocalEvent("Local REMOVE " + packet.Key)

		fmt.Println("Released Log Lock")
		remove := &udpComm{
			Type:    "Remove",
			Key:     packet.Key,
			Val:     "",
			TestVal: "",
			NewVal:  "",
			Status:  "Success",
		}
		fmt.Println("Returned from Removeudp")
		return remove

	} else {
		fmt.Println("Remove request for Packet I (" + nodeId + ") dont have")
		get := &udpComm{
			Type:    "Remove",
			Key:     packet.Key,
			Val:     packet.Val,
			TestVal: "",
			NewVal:  "",
			Status:  "Request",
		}
		LogMutex.Lock()
		msg := Logger.PrepareSend("Sending Message", get)
		LogMutex.Unlock()
		udpPortMutex.Lock()
		conn := openConnection(nodeUDPAddr, ownerUDPAddr)

		laddr, err := net.ResolveUDPAddr("udp", ownerUDPAddr)
		errorCheck(err, "Something is Wrong with the given local address")
		fmt.Println("Send request to " + ownerUDPAddr + " From " + nodeUDPAddr)

		conn.WriteTo(msg, laddr)
		//conn.Write(msg)

		conn.Close()
		conn = openConnection(nodeUDPAddr, ownerUDPAddr)

		fmt.Println("Wait for response")
		packet, _ := readMessage(conn)
		fmt.Println("Returnd to RemoveUDP function after got a packet")
		conn.Close()
		udpPortMutex.Unlock()
		fmt.Println("Got a response")
		//incomingMessage := new(udpComm)
		//Logger.UnpackReceive("Received Message", buf, &incomingMessage)

		return packet
	}
}

func replicate(packet *udpComm) *udpComm {
	fmt.Println("Replicating key and value")
	ownerId := consHash.Find(packet.Key)
	//ownerUDPAddr := nodesUDPAddrMap[ownerId]
	ID, _ := strconv.Atoi(ownerId)
	var counter = 0
	var i = 0
	var altID = ""
	var altIDInt = 0
	var ownerUDPAddr = ""
	fmt.Println(nodeIdList)
	put := &udpComm{
		Type:    "Put",
		Key:     packet.Key,
		Val:     packet.Val,
		TestVal: "",
		NewVal:  "",
		Status:  "Store",
	}
	var response *udpComm
	fmt.Println("Before loop")
	for counter < repFactor+1 {
		fmt.Println("Current counter: ")
		fmt.Println(counter)
		if counter == repFactor {
			break
		}
		var useAlt = false
		iter_Check := ID + i
		convert := strconv.Itoa(iter_Check)
		if _, ok := inactiveNodes[convert]; ok {
			fmt.Println("Owner node is inactive")
			useAlt = true
			altID = proxyNodes[convert]
			altIDInt, _ = strconv.Atoi(altID)
		}
		//HELLO := nodeList[fixed]
		//fmt.Println(HELLO)
		//fmt.Println(nodeList)
		if ID+i < len(nodeIdList) {
			if useAlt {
				fmt.Println("Storing in alternate:")
				fmt.Println(altIDInt)
				if strings.EqualFold(altID, nodeId) {
					fmt.Println("Storing to self")
					response = Putudp(packet)
					i++
					counter++
					if counter == repFactor-1 {
						break
					}
					continue
				} else {
					ownerUDPAddr = nodesUDPAddrMap[nodeIdList[altIDInt]]
				}
			} else {
				fmt.Println("Storing in:")
				fmt.Println(iter_Check)
				if strings.EqualFold(strconv.Itoa(iter_Check), nodeId) {
					fmt.Println("Storing to self")
					response = Putudp(packet)
					i++
					counter++
					if counter == repFactor-1 {
						break
					}
					continue
				} else {
					ownerUDPAddr = nodesUDPAddrMap[nodeIdList[iter_Check]]
				}
			}
			LogMutex.Lock()
			msg := Logger.PrepareSend("Sending Message", put)
			LogMutex.Unlock()
			udpPortMutex.Lock()
			conn := openConnection(nodeUDPAddr, ownerUDPAddr)

			laddr, err := net.ResolveUDPAddr("udp", ownerUDPAddr)
			errorCheck(err, "Something is Wrong with the given local address")
			fmt.Println("Send request to " + ownerUDPAddr + " From " + nodeUDPAddr)

			conn.WriteTo(msg, laddr)
			//conn.Write(msg)

			conn.Close()
			conn = openConnection(nodeUDPAddr, ownerUDPAddr)

			fmt.Println("Wait for response")
			packet, _ := readMessage(conn)
			response = packet
			fmt.Println("Returnd to putUDP function after got a packet")
			conn.Close()
			udpPortMutex.Unlock()
			fmt.Println("Got a response")
			//incomingMessage := new(udpComm)
			//Logger.UnpackReceive("Received Message", buf, &incomingMessage)
			i++
			counter++
		} else {
			ID = 0
			i = 0
		}
	}
	fmt.Println("Done replicating")
	fmt.Println(response)
	return response
}

func sliceContains(slice []string, str string) bool {
	for _, v := range slice {

		if v == str {
			return true
		}
	}
	return false
}

func Putudp(packet *udpComm) *udpComm {

	//	if strings.EqualFold(ownerUDPAddr, nodesUDPAddrMap[nodeId]) {
	//		fmt.Println("I " + nodeId + " can store the value")
	kvMutex.Lock()
	defer kvMutex.Unlock()
	var retVal = ors.Add(packet.Key, packet.Val)

	fmt.Println("Put contain test")
	fmt.Println(sliceContains(activeKeys, packet.Key))

	if !sliceContains(activeKeys, packet.Key) {
		fmt.Println("Add to activeKeys " + packet.Key)
		activeKeys = append(activeKeys, packet.Key)
	}

	fmt.Println("Put value: " + packet.Key)

	LogLocalEvent("Local Put returned: " + retVal)

	fmt.Println("Released Log Lock")
	put := &udpComm{
		Type:    "Put",
		Key:     packet.Key,
		Val:     packet.Val,
		TestVal: "",
		NewVal:  "",
		Status:  "Success",
	}
	fmt.Println("Returned from Putudp")
	return put
}

//	} // else {
//		fmt.Println("Got requets for Packet I (" + nodeId + ") can't have")
//		put := &udpComm{
//			Type:    "Put",
//			Key:     packet.Key,
//			Val:     packet.Val,
//			TestVal: "",
//			NewVal:  "",
//			Status:  "Storing",
//		}
//		LogMutex.Lock()
//		msg := Logger.PrepareSend("Sending Message", put)
//		LogMutex.Unlock()
//		udpPortMutex.Lock()
//		conn := openConnection(nodeUDPAddr, ownerUDPAddr)

//		laddr, err := net.ResolveUDPAddr("udp", ownerUDPAddr)
//		errorCheck(err, "Something is Wrong with the given local address")
//		fmt.Println("Send request to " + ownerUDPAddr + " From " + nodeUDPAddr)

//		conn.WriteTo(msg, laddr)
//		//conn.Write(msg)

//		conn.Close()
//		conn = openConnection(nodeUDPAddr, ownerUDPAddr)

//		fmt.Println("Wait for response")
//		packet, _ := readMessage(conn)
//		fmt.Println("Returnd to putUDP function after got a packet")
//		conn.Close()
//		udpPortMutex.Unlock()
//		fmt.Println("Got a response")
//		//incomingMessage := new(udpComm)
//		//Logger.UnpackReceive("Received Message", buf, &incomingMessage)

//		return packet
//	}
//}

//func Putudp(packet udpComm) {

//	ownerId := consHash.Find(packet.Key)
//	ownerUDPAddr := nodesUDPAddrMap[ownerId]

//	if strings.EqualFold(ownerUDPAddr, nodesUDPAddrMap[nodeId]) {
//		kvMutex.Lock()
//		defer kvMutex.Unlock()
//		ors.Add(packet.Key, packet.Val)
//		LogLocalEvent("Local Put returned: " + retVal)
//		return retVal

//	} else {

//		put := &udpComm{
//			Type:    "Put",
//			Key:     key,
//			Val:     "",
//			TestVal: "",
//			NewVal:  "",
//			Status:  "Request",
//		}

//		msg := Logger.PrepareSend("Sending Message", put)

//		LogMutex.Lock()
//		conn := openConnection(nodeUDPAddr, ownerUDPAddr)
//		conn.Write(msg)

//		var buf [512]byte
//		n, err := conn.Read(buf[0:])

//		conn.Close()
//		LogMutex.Unlock()

//		incomingMessage := new(udpComm)
//		Logger.UnpackReceive("Received Message", buf[0:n], &incomingMessage)
//		retVal := incomingMessage.Status
//		return retVal
//	}
//}

// ----------------------------------------
// GOSSIP PROTOCOL
// ----------------------------------------

// Configuration
// Name - Must be unique
// BindAddr & BindPort - Address and Port to use for Gossip communication
// BootstrapAddr - Address of seed node known to all nodes

func gossip(gossipID string, gossipAddr string, gossipPort int, bootstrapAddr string) {
	var config = memberlist.DefaultLocalConfig()
	config.Name = "Node" + gossipID
	config.BindAddr = gossipAddr
	config.BindPort = gossipPort
	config.Events = &ActiveList{}
	f, err := os.Create("gossipNode" + gossipID + ".log")
	checkError(err)
	config.LogOutput = f
	list, err := memberlist.Create(config)
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}

	_, err = list.Join([]string{bootstrapAddr})
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

	//raddr, err := net.ResolveUDPAddr("udp", remoteAddr)
	//errorCheck(err, "Something is Wrong with the given remote address")

	conn, _ := net.ListenUDP("udp", laddr)
	//conn, err := net.DialUDP("udp", laddr, raddr)
	//errorCheck(err, "Something has gone wrong in the initial connection")

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

	//laddr, err := net.ResolveUDPAddr("udp", listeningAddr)
	//errorCheck(err, "Something is Wrong with the given local address")
	//conn.WriteToUDP(outBuf, laddr)

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
	Logger.LogLocalEvent(msg)
	LogMutex.Unlock()
}

// goes through all keys in local node
// if key belongs to callingNode then place
// key in retKeys and the corresponding value in retValues
// return all keys and values that belong to callingNode
func createNodeOrset(callingNode string) *udpOrsetBuild {
	kvMutex.Lock()

	var retKeys []string
	var retValues []string

	fmt.Println("activekeys")
	fmt.Println(activeKeys)

	for _, v := range activeKeys {
		fmt.Println("Building Orset")
		fmt.Println("key: " + v)
		fmt.Println(consHash.Find(v))
		fmt.Println(callingNode)

		if consHash.Find(v) == callingNode {
			retKeys = append(retKeys, v)
			retValues = append(retValues, ors.Get(v))
		}
	}
	kvMutex.Unlock()
	orset := &udpOrsetBuild{
		Keys:   retKeys,
		Values: retValues,
		Status: "Reply" + nodeId,
	}

	return orset
}

func buildORSET() {

	contactMyReplicas()
	//contactNodes()
	fmt.Println("Finished building orset")
}

func contactMyReplicas() {
	fmt.Println("ContactMyReplicas")
	totalNodes := len(nodeIdList)

	val, _ := strconv.Atoi(nodeId)
	currentNode := val + 1

	for i := 1; i <= repFactor-1; i++ {
		fmt.Println("contacting Replicas")
		if currentNode >= totalNodes {
			currentNode = 0
		}

		build := &udpComm{
			Type:    "Build",
			Key:     "",
			Val:     "",
			TestVal: "",
			NewVal:  "",
			Status:  nodeId,
		}

		LogMutex.Lock()
		msg := Logger.PrepareSend("Sending Build Request to Replica", build)
		LogMutex.Unlock()

		fmt.Println("Prepped message")

		fmt.Println("got udp lock")
		fmt.Println("Contacting Replica: " + nodesUDPAddrMap[strconv.Itoa(currentNode)])
		conn := openConnection(nodeOrsetBuildAddr, nodesUDPAddrMap[strconv.Itoa(currentNode)])

		fmt.Println("Opened connection to Replica")

		laddr, err := net.ResolveUDPAddr("udp", nodesUDPAddrMap[strconv.Itoa(currentNode)])
		errorCheck(err, "Something is Wrong with the given local address")
		fmt.Println("Sending Build Request")

		conn.WriteTo(msg, laddr)
		conn.Close()

		conn = openConnection(nodeOrsetBuildAddr, nodesUDPAddrMap[strconv.Itoa(currentNode)])
		fmt.Println("Open connection and set timeout")
		conn.SetReadDeadline(time.Now().Add(time.Second * 5))

		fmt.Println("Waiting for response to Build Request")
		buffer := make([]byte, 1024)
		bytesRead, _, err := conn.ReadFromUDP(buffer)
		fmt.Println("No longer waiting")
		conn.Close()

		fmt.Println("Error")
		fmt.Println(err)
		if err == nil {
			fmt.Println("I got a response")
			packet := new(udpOrsetBuild)

			fmt.Println("Keys")
			fmt.Println(packet.Keys)
			fmt.Println("Values")
			fmt.Println(packet.Values)

			LogMutex.Lock()
			Logger.UnpackReceive("Receive Message", buffer[:bytesRead], &packet)
			LogMutex.Unlock()
			fmt.Println(packet)
			kvMutex.Lock()

			receivedKeys := packet.Keys
			receivedValues := packet.Values

			newORset := orset.NewORSet()
			for i, v := range receivedKeys {
				fmt.Println("Adding: " + v)
				newORset.Add(v, receivedValues[i])

				if !sliceContains(activeKeys, v) {
					fmt.Println("Add to activeKeys " + v)
					activeKeys = append(activeKeys, v)
				}
			}

			ors.Merge(newORset)
			kvMutex.Unlock()
		}

		currentNode++

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

	replicationFactor := os.Args[1]
	gossipID := os.Args[2]

	nodeId = gossipID
	config, err := ioutil.ReadFile("config.json")
	checkError(err)
	err = json.Unmarshal(config, &nodes)
	checkError(err)

	// Set up gossip protocol libary using known ip:port of node 0 to bootstrap
	bootstrapAddr := nodes["0"]
	nodeIpPort := nodes[nodeId]
	host, _, err := net.SplitHostPort(nodes[nodeId])
	checkError(err)
	gossipAddr, err := net.ResolveUDPAddr("udp", nodeIpPort)
	fmt.Println(gossipAddr.String())
	checkError(err)
	go gossip(gossipID, gossipAddr.IP.String(), gossipAddr.Port, bootstrapAddr)

	nodeIdList = strings.Split(nodes["list"], " ")
	inactiveNodes = make(map[string]bool)
	proxyNodes = make(map[string]string)
	nodesUDPAddrMap = make(map[string]string)
	for _, id := range nodeIdList {
		peer, _, err := net.SplitHostPort(nodes[id])
		checkError(err)
		nodesUDPAddrMap[id] = peer + ":444" + id
	}
	nodeUDPAddr = host + ":333" + nodeId
	nodeOrsetBuildAddr = host + ":555" + nodeId

	// Set up GoVector Logging
	Logger = govec.Initialize(nodeId, nodeId)
	LogMutex = &sync.Mutex{}
	udpPortMutex = &sync.Mutex{}

	numberPortions := 1024
	var hashFunction chash.Hash = nil
	consHash = chash.New(numberPortions, nodeIdList, hashFunction)
	ors = orset.NewORSet()

	repFactor, err = strconv.Atoi(replicationFactor)
	checkError(err)
	kvMutex = &sync.Mutex{}

	// used for handling UDP communication
	go handleRequestUDP()

	// Checks with other nodes to see if any key value pairs need to be added
	// to this node's orset
	buildORSET()

	//blocking call to serve http requests
	log.Fatal(http.ListenAndServe(host+":888"+nodeId, handler.NewRouter(nodeId, nodesUDPAddrMap[nodeId])))
}

// If error is non-nil, print it out and halt.
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		os.Exit(1)
	}
}
