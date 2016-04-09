


package main

import (
    "encoding/json"
    "fmt"
    // "io/ioutil"
    // "io"
    "log"
    // "html/template"
    "net"
    "net/http"
    // "os"
    // "regexp"
    // "strings"
    "sync"
    // "time"
    "github.com/arcaneiceman/GoVector/govec"
    "github.com/gorilla/mux"

)

var nodeIP = "localhost:4441"  // HARDCODED NODE IP

// TODO Object
type Todo struct {
    Task         string `json:"task"`
    Description  string `json:"description"`
}

var todos []Todo

var todolist map[string]string

type udpComm struct {
    Type    string
    Key     string
    Val     string
    TestVal string
    NewVal  string
    Status  string
}
// ----------------------------------------
// GoVector Logging
// ----------------------------------------
var Logger *govec.GoLog
var LogMutex *sync.Mutex
// ----------------------------------------
// ---------------------------------------


// ROUTER FUNCTIONS

func Index(w http.ResponseWriter, r *http.Request) {

    http.ServeFile(w, r, "index.html")
}

func Add(w http.ResponseWriter, r *http.Request) {
    // read the json from the client at "/add"

    var todo Todo

    // body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))  // limit amount of bytes
    // if err != nil {
    //     panic(err)
    // }

    // if err := r.Body.Close(); err != nil {
    //     panic(err)
    // }
    // if err := json.Unmarshal(body, &todo); err != nil {
    //     w.Header().Set("Content-Type", "application/json; charset=UTF-8")
    //     w.WriteHeader(422) // unprocessable entity
    //     if err := json.NewEncoder(w).Encode(err); err != nil {
    //         panic(err)
    //     }
    // }
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

    fmt.Println("Connect to: " + nodeIP)
    packet := Logger.PrepareSend("Send Get Request", &putArgs)
    conn := openConnection("localhost:9999", nodeIP)
    conn.Write(packet)
    conn.Close()

    laddr, _ := net.ResolveUDPAddr("udp", "localhost:9999")
    conn, _ = net.ListenUDP("udp", laddr)
    fmt.Println("Wait for response")
    pack, _ := readMessage(conn)
    fmt.Println(pack)
    conn.Close()
    // if successfull, append the list
    if (pack.Status == "Success") {
 // send back success ack
         // add to client side list:
        todolist[todo.Task] = todo.Description

        w.Header().Set("Content-Type", "application/json; charset=UTF-8")
        w.WriteHeader(http.StatusCreated)
        if err := json.NewEncoder(w).Encode(todo); err != nil {
            panic(err)
        }    
    }
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

    fmt.Println("Connect to: " + nodeIP)
    packet := Logger.PrepareSend("Send Remove Request", removeArgs)
    conn := openConnection("localhost:9999", nodeIP)
    conn.Write(packet)
    conn.Close()

    laddr, _ := net.ResolveUDPAddr("udp", "localhost:9999")
    conn, _ = net.ListenUDP("udp", laddr)
    fmt.Println("Wait for response")
    pack, _ := readMessage(conn)
    fmt.Println(pack)
    conn.Close()

    // IF RECEIVED SUCCESS MESSAGE
    if (pack.Status == "Success") {
     // send back success ack
        w.Header().Set("Content-Type", "application/json; charset=UTF-8")
        w.WriteHeader(http.StatusCreated)
        if err := json.NewEncoder(w).Encode(todo); err != nil {
            panic(err)
        }    
    }
}

// page to list the todos
func Todos(w http.ResponseWriter, r *http.Request) {
    // mock items
    todos = append(todos, Todo{Task: "Write presentation", Description: "DO A GOOD JOB"})
    todos = append(todos, Todo{Task: "Dance", Description: "LIKE WHEN NO ONE IS WATCHING"})
    // for k, _ := todolist {
    //     // get values
    //     // SEND Get Request
    //         getArgs := udpComm{
    //             Type:    "Get",
    //             Key:     k,
    //             Val:     "",
    //             TestVal: "",
    //             NewVal:  "",
    //             Status:  "Request",
    //         }

    //         fmt.Println("Connect to: " + nodeIP)
    //         packet := Logger.PrepareSend("Send Get Request", getArgs)
    //         conn := openConnection("localhost:9999", nodeIP)
    //         conn.Write(packet)
    //         conn.Close()

    //         laddr, _ := net.ResolveUDPAddr("udp", "localhost:9999")
    //         conn, _ = net.ListenUDP("udp", laddr)
    //         fmt.Println("Wait for response")
    //         pack, _ := readMessage(conn)
    //         fmt.Println(pack)
    //         conn.Close()
    //     todo = 
    // }
    w.Header().Set("Content-Type", "application/json")
    // j, _ := json.Marshal(tasks)
    // w.Write(j)

    if err := json.NewEncoder(w).Encode(todos); err != nil {
        panic(err)
    }
}

func TodoShow(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    todoId := vars["todoId"]
    fmt.Fprintln(w, "Todo show:", todoId)
}

// Function to build current map of key/values

func main() {
    // http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
    //     fmt.Fprintf(w, "Testing, %q", html.EscapeString(r.URL.Path))
    // })
    // HTTP SERVER AND ROUTER 
    // cssHandler := http.FileServer(http.Dir("./css/"))
    // jsHandler := http.FileServer(http.Dir("./js/"))

    // Set up GoVector Logging
    Logger = govec.Initialize("DummyClient", "DummyClient")
    LogMutex = &sync.Mutex{}
    go receiveMessagesLog("localhost:8888")

    router := mux.NewRouter().StrictSlash(true)
    router.HandleFunc("/", Index)
    router.HandleFunc("/add", Add).Methods("POST")
    router.HandleFunc("/remove", Remove)
    router.HandleFunc("/todos", Todos)
    router.HandleFunc("/todos/{todoId}", TodoShow)
   
    log.Fatal(http.ListenAndServe(":8080", router))
}



// METHODS TO SERVE BACKEND

//Msg is the message sent over the network
//Msg is capitalized so GoVecs encoder can acess it
//Furthermore its variables are capitalized to make them public
type Msg struct {
    Content, RealTimestamp string
}

func (m Msg) String() string {
    return "content: " + m.Content + "\ntime: " + m.RealTimestamp
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

    fmt.Println("Local addr: " + laddr.String() + " remote: " + raddr.String())
    conn, err := net.DialUDP("udp", laddr, raddr)
    errorCheck(err, "Something has gone wrong in the initial connection")

    return conn
}


func LogLocalEvent(msg string) {
    LogMutex.Lock()
    Logger.LogLocalEvent("Message Says: " + msg)
    LogMutex.Unlock()
}

func errorCheck(err error, message string) {
    if err != nil {
        fmt.Println(message)
        fmt.Printf("%s\n", err)
    }
}

func readMessage(conn *net.UDPConn) (*udpComm, net.Addr) {

    buffer := make([]byte, 1024)

    bytesRead, retAddr, err := conn.ReadFrom(buffer)
    errorCheck(err, "Problem with Reading UDP Packet")
    packet := new(udpComm)

    Logger.UnpackReceive("Receive Message", buffer[:bytesRead], &packet)

    return packet, retAddr
}

func sendToBackEnd(putArgs *udpComm, req string) {

    fmt.Println("Connect to: " + nodeIP)
    packet := Logger.PrepareSend("Send " + req + " Request", putArgs)
    conn := openConnection("localhost:9999", nodeIP)
    conn.Write(packet)
    conn.Close()

    laddr, _ := net.ResolveUDPAddr("udp", "localhost:9999")
    conn, _ = net.ListenUDP("udp", laddr)
    fmt.Println("Wait for response")
    pack, _ := readMessage(conn)
    fmt.Println(pack)
    conn.Close()

}
    