package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	zmq4 "github.com/go-zeromq/zmq4"
	msgpack "github.com/vmihailenco/msgpack/v5"
	_ "modernc.org/sqlite"
)

type InMsg struct {
	Type        string  `msgpack:"type"`
	Username    string  `msgpack:"username"`
	ChannelName string  `msgpack:"channel_name"`
	Message     string  `msgpack:"message"`
	Timestamp   float64 `msgpack:"timestamp"`
	Clock       int64   `msgpack:"clock"`
	Name        string  `msgpack:"name"`
}
type OutMsg struct {
	Status    string   `msgpack:"status"`
	Message   string   `msgpack:"message"`
	Data      []string `msgpack:"data,omitempty"`
	Timestamp float64  `msgpack:"timestamp"`
	Clock     int64    `msgpack:"clock"`
	Rank      int      `msgpack:"rank,omitempty"`
}
type PubPayload struct {
	Channel   string  `msgpack:"channel"`
	Username  string  `msgpack:"username"`
	Message   string  `msgpack:"message"`
	Timestamp float64 `msgpack:"timestamp"`
	Received  float64 `msgpack:"received"`
	Clock     int64   `msgpack:"clock"`
}
type RefResp struct {
	Status    string  `msgpack:"status"`
	Rank      int     `msgpack:"rank"`
	Time      float64 `msgpack:"time"`
	Clock     int64   `msgpack:"clock"`
	Timestamp float64 `msgpack:"timestamp"`
}

var (
	db          *sql.DB
	logicClock  int64
	clockMu     sync.Mutex
	timeOffset  float64
	serverRank  int
	serverName  string
	refHost     string
	refPort     string
)

func nowTS() float64 { return float64(time.Now().UnixNano())/1e9 + timeOffset }

func tickSend() int64 {
	clockMu.Lock(); defer clockMu.Unlock()
	logicClock++; return logicClock
}
func tickRecv(recv int64) {
	clockMu.Lock(); defer clockMu.Unlock()
	if recv > logicClock { logicClock = recv }
}

func initDB() {
	os.MkdirAll("/data", 0755)
	var err error
	db, err = sql.Open("sqlite", "/data/server.db")
	if err != nil { panic(err) }
	db.Exec(`
		CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, created_at REAL NOT NULL);
		CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, timestamp REAL NOT NULL);
		CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at REAL NOT NULL);
		CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, username TEXT NOT NULL, message TEXT NOT NULL, timestamp REAL NOT NULL, clock INTEGER NOT NULL DEFAULT 0);
	`)
}

func callReference(payload map[string]interface{}) (RefResp, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sock := zmq4.NewReq(ctx)
	defer sock.Close()
	if err := sock.Dial(fmt.Sprintf("tcp://%s:%s", refHost, refPort)); err != nil {
		return RefResp{}, err
	}
	raw, _ := msgpack.Marshal(payload)
	sock.Send(zmq4.NewMsg(raw))
	resp, err := sock.Recv()
	if err != nil { return RefResp{}, err }
	var r RefResp
	msgpack.Unmarshal(resp.Frames[0], &r)
	return r, nil
}

func connectToReference() {
	clk := tickSend()
	r, err := callReference(map[string]interface{}{"type": "register", "name": serverName, "clock": clk, "timestamp": nowTS()})
	if err != nil { fmt.Printf("[%s] Reference error: %v\n", serverName, err); return }
	tickRecv(r.Clock)
	serverRank = r.Rank
	timeOffset = r.Timestamp - float64(time.Now().UnixNano())/1e9
	fmt.Printf("[%s] Registered | rank=%d | time_offset=%.3fs\n", serverName, serverRank, timeOffset)
}

func sendHeartbeat(msgCount int64) {
	clk := tickSend()
	r, err := callReference(map[string]interface{}{"type": "heartbeat", "name": serverName, "clock": clk, "timestamp": nowTS(), "msg_count": msgCount})
	if err != nil { fmt.Printf("[%s] Heartbeat error: %v\n", serverName, err); return }
	tickRecv(r.Clock)
	if r.Time > 0 { timeOffset = r.Time - float64(time.Now().UnixNano())/1e9 }
	fmt.Printf("[%s] HEARTBEAT sent | rank=%d | clock=%d | offset=%.3fs\n", serverName, serverRank, logicClock, timeOffset)
}

func makeResp(status, message string) OutMsg {
	return OutMsg{Status: status, Message: message, Clock: tickSend(), Timestamp: nowTS()}
}

func handleLogin(msg InMsg) OutMsg {
	if msg.Username == "" { return makeResp("error", "Username cannot be empty") }
	db.Exec("INSERT OR IGNORE INTO users (username, created_at) VALUES (?, ?)", msg.Username, nowTS())
	db.Exec("INSERT INTO logins (username, timestamp) VALUES (?, ?)", msg.Username, nowTS())
	r := makeResp("ok", fmt.Sprintf("Welcome, %s!", msg.Username))
	r.Rank = serverRank; return r
}

func handleCreateChannel(msg InMsg) OutMsg {
	if msg.ChannelName == "" { return makeResp("error", "Channel name cannot be empty") }
	_, err := db.Exec("INSERT INTO channels (name, created_by, created_at) VALUES (?, ?, ?)", msg.ChannelName, msg.Username, nowTS())
	if err != nil { return makeResp("error", fmt.Sprintf("Channel '%s' already exists", msg.ChannelName)) }
	return makeResp("ok", fmt.Sprintf("Channel '%s' created!", msg.ChannelName))
}

func handleListChannels() OutMsg {
	rows, _ := db.Query("SELECT name FROM channels ORDER BY created_at")
	defer rows.Close()
	var channels []string
	for rows.Next() { var n string; rows.Scan(&n); channels = append(channels, n) }
	if channels == nil { channels = []string{} }
	r := makeResp("ok", "OK"); r.Data = channels; return r
}

func handlePublish(msg InMsg, pubSock zmq4.Socket) OutMsg {
	if msg.ChannelName == "" || msg.Message == "" { return makeResp("error", "Channel and message required") }
	var name string
	if err := db.QueryRow("SELECT name FROM channels WHERE name=?", msg.ChannelName).Scan(&name); err != nil {
		return makeResp("error", fmt.Sprintf("Channel '%s' does not exist", msg.ChannelName))
	}
	db.Exec("INSERT INTO messages (channel,username,message,timestamp,clock) VALUES (?,?,?,?,?)",
		msg.ChannelName, msg.Username, msg.Message, nowTS(), msg.Clock)

	clk := tickSend()
	payload, _ := msgpack.Marshal(PubPayload{
		Channel: msg.ChannelName, Username: msg.Username, Message: msg.Message,
		Timestamp: nowTS(), Received: nowTS(), Clock: clk,
	})
	pubSock.Send(zmq4.NewMsgFrom([]byte(msg.ChannelName), payload))
	fmt.Printf("[%s] PUB  | channel=%-15s | from=%-12s | clock=%d | %s\n", serverName, msg.ChannelName, msg.Username, clk, msg.Message)
	return makeResp("ok", "Published!")
}

func main() {
	port       := os.Getenv("PORT");        if port == "" { port = "5551" }
	proxyHost  := os.Getenv("PROXY_HOST");  if proxyHost == "" { proxyHost = "proxy" }
	xsubPort   := os.Getenv("XSUB_PORT");   if xsubPort == "" { xsubPort = "5557" }
	refHost     = os.Getenv("REF_HOST");    if refHost == "" { refHost = "reference" }
	refPort     = os.Getenv("REF_PORT");    if refPort == "" { refPort = "5559" }
	serverName  = os.Getenv("SERVER_NAME"); if serverName == "" { serverName = "server-go" }

	initDB()
	time.Sleep(2 * time.Second)
	connectToReference()

	ctx := context.Background()
	repSock := zmq4.NewRep(ctx)
	defer repSock.Close()
	repSock.Listen(fmt.Sprintf("tcp://*:%s", port))

	pubSock := zmq4.NewPub(ctx)
	defer pubSock.Close()
	pubSock.Dial(fmt.Sprintf("tcp://%s:%s", proxyHost, xsubPort))
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("[%s] Listening on port %s | rank=%d\n", serverName, port, serverRank)

	var msgCount int64
	for {
		zmqMsg, err := repSock.Recv()
		if err != nil { continue }
		var msg InMsg
		msgpack.Unmarshal(zmqMsg.Frames[0], &msg)
		tickRecv(msg.Clock)
		atomic.AddInt64(&msgCount, 1)

		fmt.Printf("[%s] RECV | type=%-10s | from=%-12s | clock=%d | lc=%d\n", serverName, msg.Type, msg.Username, msg.Clock, logicClock)

		var resp OutMsg
		switch msg.Type {
		case "login":   resp = handleLogin(msg)
		case "channel": resp = handleCreateChannel(msg)
		case "list":    resp = handleListChannels()
		case "publish": resp = handlePublish(msg, pubSock)
		default:        resp = makeResp("error", fmt.Sprintf("Unknown: %s", msg.Type))
		}

		fmt.Printf("[%s] SEND | status=%-8s | clock=%d\n", serverName, resp.Status, resp.Clock)
		raw, _ := msgpack.Marshal(resp)
		repSock.Send(zmq4.NewMsg(raw))

		if atomic.LoadInt64(&msgCount)%10 == 0 {
			go sendHeartbeat(atomic.LoadInt64(&msgCount))
		}
	}
}
