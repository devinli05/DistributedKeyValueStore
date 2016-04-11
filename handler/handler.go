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
	router.HandleFunc("/remove", Remove).Methods("DELETE")
	router.HandleFunc("/{key}", GetTodo).Methods("GET")
	return router
}

func Index(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "index.html")
}

// Function to handle all Add requests (at URI:  /add)
func Add(w http.ResponseWriter, r *http.Request) {
	// read the json from the client at "/add"
	var todo Todo
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&todo)
	if err != nil {
		panic("error in ADD decoding JSON")
	}

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
<<<<<<< HEAD

	// IF RECEIVED SUCCESS MESSAGE
	// if responseUdp.Status == "Success" {
	// send back success ack
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	// }
=======
	// IF RECEIVED SUCCESS MESSAGE
	if responseUdp.Status == "Success" {
		// send back success ack
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(todo); err != nil {
			panic(err)
		}
	}
>>>>>>> 4c7d6a194fe6946985233bf6639c9ecf554faf9a
}

func Remove(w http.ResponseWriter, r *http.Request) {

	var todo Todo
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&todo)
	if err != nil {
		panic("error in REMOVE decoding JSON")
	}

	// Remove Request
	removeArgs := udpComm{
		Type:    "Remove",
		Key:     todo.Task,
		Val:     "",
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
	requestUdp := Logger.PrepareSend("request remove "+todo.Task+":"+todo.Description, removeArgs)
	udpConn.WriteToUDP(requestUdp, rAddr)
	fmt.Println("Wait for response")
	responseUdp, _ := readMessage("response remove "+todo.Task+":"+todo.Description, udpConn)
	fmt.Println(responseUdp)
	udpConn.Close()

<<<<<<< HEAD
	// IF REMOVE RECEIVED SUCCESS MESSAGE
	if responseUdp.Status == "Success" {
		// send back success ack
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
	}
	// fmt.Println("Connect to: " + nodeIP)
	// packet := Logger.PrepareSend("Send Remove Request", removeArgs)
	// conn := openConnection("localhost:9999", nodeIP)
	// conn.Write(packet)
	// conn.Close()
	//
	// laddr, _ := net.ResolveUDPAddr("udp", "localhost:9999")
	// conn, _ = net.ListenUDP("udp", laddr)
	// fmt.Println("Wait for response")
	// pack, _ := readMessage(conn)
	// fmt.Println(pack)
	// conn.Close()

=======
	// IF RECEIVED SUCCESS MESSAGE
>>>>>>> 4c7d6a194fe6946985233bf6639c9ecf554faf9a
	if responseUdp.Status == "Success" {
		// send back success ack
		// w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		// if err := json.NewEncoder(w).Encode(todo); err != nil {
		// 	panic(err)
		// }
	}
}

func GetTodo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	task := vars["key"]

	// Get Request
	getArgs := udpComm{
		Type:    "Get",
		Key:     task,
		Val:     "",
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
	requestUdp := Logger.PrepareSend("request get "+task+",", getArgs)
	udpConn.WriteToUDP(requestUdp, rAddr)
	fmt.Println("Wait for response")
	responseUdp, _ := readMessage("response get "+task+":", udpConn)
	fmt.Println(responseUdp)
	udpConn.Close()

	if responseUdp.Status == "Success" {
		// send back json of key/value of Task
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(200)
		todo := Todo{Task: responseUdp.Key, Description: responseUdp.Val}
		if err := json.NewEncoder(w).Encode(todo); err != nil {
			panic("error in encoding json to send to client")
		}
	}
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
