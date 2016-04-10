package handler

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/arcaneiceman/GoVector/govec"
	"github.com/gorilla/mux"
)

// TODO Object
type Todo struct {
	Task        string `json:"task"`
	Description string `json:"description"`
}

var todos map[string]string

type udpComm struct {
	Type    string
	Key     string
	Val     string
	TestVal string
	NewVal  string
	Status  string
}

var nodeUdpAddr string

var Logger *govec.GoLog

//var LogMutex *sync.Mutex

func NewRouter(nodeId string, udpAddr string) http.Handler {
	nodeUdpAddr = udpAddr
	Logger = govec.Initialize("HttpNode"+nodeId, "HttpNodeLog"+nodeId)
	//LogMutex = &sync.Mutex{}
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", Index)
	router.HandleFunc("/add", Add).Methods("POST")
	return router
}

func Index(w http.ResponseWriter, r *http.Request) {
	//http.ServeFile(w, r, "index.html")
	fmt.Fprintln(w, "index")
}

func Add(w http.ResponseWriter, r *http.Request) {
	// read the json from the client at "/add"

	var todo Todo

	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&todo)
	if err != nil {
		panic("error in ADD decoding JSON")
	}

	fmt.Println(todo)

	// Put Request
	putArgs := udpComm{
		Type:    "Put",
		Key:     todo.Task,
		Val:     todo.Description,
		TestVal: "",
		NewVal:  "",
		Status:  "Request",
	}
	rAddr, err := net.ResolveUDPAddr("udp", nodeUdpAddr)
	checkError(err)
	lAddr, err := net.ResolveUDPAddr("udp", rAddr.IP.String()+":0")
	checkError(err)
	udpConn, err := net.ListenUDP("udp", lAddr)
	checkError(err)
	requestUdp := Logger.PrepareSend("request put "+todo.Task+":"+todo.Description, putArgs)
	udpConn.WriteToUDP(requestUdp, rAddr)
	fmt.Println("Wait for response")
	responseUdp, _ := readMessage("response put "+todo.Task+":"+todo.Description, udpConn)
	fmt.Println(responseUdp)
	udpConn.Close()
}

func readMessage(govecMsg string, conn *net.UDPConn) (*udpComm, net.Addr) {

	buffer := make([]byte, 1024)

	bytesRead, retAddr, err := conn.ReadFrom(buffer)
	checkError(err)
	//errorCheck(err, "Problem with Reading UDP Packet")
	packet := new(udpComm)

	Logger.UnpackReceive(govecMsg, buffer[:bytesRead], &packet)

	return packet, retAddr
}

// If error is non-nil, print it out and halt.
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		os.Exit(1)
	}
}
