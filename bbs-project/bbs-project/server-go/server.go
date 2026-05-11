package main

import (
	"context"
	"crypto/sha256"
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
	Rank        int     `msgpack:"rank"`
}
type OutMsg struct {
	Status      string        `msgpack:"status"`
	Message     string        `msgpack:"message"`
	Data        interface{}   `msgpack:"data,omitempty"`
	Timestamp   float64       `msgpack:"timestamp"`
	Clock       int64         `msgpack:"clock"`
	Rank        int           `msgpack:"rank,omitempty"`
	Time        float64       `msgpack:"time,omitempty"`
	Coordinator string        `msgpack:"coordinator,omitempty"`
	Servers     []map[string]interface{} `msgpack:"servers,omitempty"`
}
type PubPayload struct {
	Channel   string  `msgpack:"channel"`
	Username  string  `msgpack:"username"`
	Message   string  `msgpack:"message"`
	Timestamp float64 `msgpack:"timestamp"`
	Received  float64 `msgpack:"received"`
	Clock     int64   `msgpack:"clock"`
	Origin    string  `msgpack:"origin"`
}

var (
	db           *sql.DB
	dbMu         sync.Mutex
	logicClock   int64
	clockMu      sync.Mutex
	timeOffset   float64
	offsetMu     sync.Mutex
	serverRank   int
	serverName   string
	coordinator  string
	coordMu      sync.Mutex
	knownServers []map[string]interface{}
	serversMu    sync.Mutex
	pubSock      zmq4.Socket
	refHost, refPort, proxyHost, xsubPort, xpubPort, s2sPort string
)

func nowTS() float64 {
	offsetMu.Lock(); defer offsetMu.Unlock()
	return float64(time.Now().UnixNano())/1e9 + timeOffset
}
func tickSend() int64 {
	clockMu.Lock(); defer clockMu.Unlock(); logicClock++; return logicClock
}
func tickRecv(r int64) {
	clockMu.Lock(); defer clockMu.Unlock()
	if r > logicClock { logicClock = r }
}
func makeMsgID(channel, username, message string, ts float64) string {
	key := fmt.Sprintf("%s|%s|%s|%.3f", channel, username, message, ts)
	h   := sha256.Sum256([]byte(key))
	return fmt.Sprintf("%x", h[:8])
}

func initDB() {
	os.MkdirAll("/data", 0755)
	var err error; db, err = sql.Open("sqlite", "/data/server.db"); if err != nil { panic(err) }
	db.Exec(`
		CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, created_at REAL NOT NULL);
		CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, timestamp REAL NOT NULL);
		CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at REAL NOT NULL);
		CREATE TABLE IF NOT EXISTS messages (
			id        INTEGER PRIMARY KEY AUTOINCREMENT,
			msg_id    TEXT    UNIQUE NOT NULL,
			channel   TEXT    NOT NULL,
			username  TEXT    NOT NULL,
			message   TEXT    NOT NULL,
			timestamp REAL    NOT NULL,
			clock     INTEGER NOT NULL DEFAULT 0,
			origin    TEXT    NOT NULL DEFAULT 'local');
	`)
}

func replicateMessage(data PubPayload) {
	if data.Channel == "" || data.Message == "" { return }
	msgID := makeMsgID(data.Channel, data.Username, data.Message, data.Timestamp)
	dbMu.Lock(); defer dbMu.Unlock()
	db.Exec("INSERT OR IGNORE INTO channels (name,created_by,created_at) VALUES (?,?,?)",
		data.Channel, data.Username, data.Timestamp)
	res, err := db.Exec(
		"INSERT OR IGNORE INTO messages (msg_id,channel,username,message,timestamp,clock,origin) VALUES (?,?,?,?,?,?,?)",
		msgID, data.Channel, data.Username, data.Message, data.Timestamp, data.Clock, data.Origin)
	if err == nil && res != nil {
		if rows, _ := res.RowsAffected(); rows > 0 {
			fmt.Printf("[%s] REPL | channel=%-15s | from=%-12s | origin=%s\n",
				serverName, data.Channel, data.Username, data.Origin)
		}
	}
}

func replicationThread() {
	ctx := context.Background()
	sub := zmq4.NewSub(ctx); defer sub.Close()
	sub.Dial(fmt.Sprintf("tcp://%s:%s", proxyHost, xpubPort))
	time.Sleep(time.Second)
	sub.SetOption(zmq4.OptionSubscribe, "") // todos os tópicos
	fmt.Printf("[%s] REPL SUB | subscribed to all topics on proxy\n", serverName)
	for {
		zmqMsg, err := sub.Recv()
		if err != nil || len(zmqMsg.Frames) < 2 { continue }
		topic := string(zmqMsg.Frames[0])
		if topic == "servers" { continue }
		var p PubPayload
		if err := msgpack.Unmarshal(zmqMsg.Frames[1], &p); err != nil { continue }
		tickRecv(p.Clock)
		replicateMessage(p)
	}
}

func reqCall(addr string, payload interface{}, timeoutMs int) (OutMsg, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()
	sock := zmq4.NewReq(ctx); defer sock.Close()
	if err := sock.Dial(addr); err != nil { return OutMsg{}, err }
	raw, _ := msgpack.Marshal(payload)
	sock.Send(zmq4.NewMsg(raw))
	resp, err := sock.Recv()
	if err != nil { return OutMsg{}, err }
	var out OutMsg; msgpack.Unmarshal(resp.Frames[0], &out)
	return out, nil
}
func callRef(payload interface{}) OutMsg {
	r, _ := reqCall(fmt.Sprintf("tcp://%s:%s", refHost, refPort), payload, 5000)
	return r
}
func s2sPortOf(name string) string {
	m := map[string]string{"server-python":"5560","server-go":"5561","server-csharp":"5562","server-c":"5563","server-lua":"5564"}
	if p, ok := m[name]; ok { return p }; return "5560"
}
func callS2S(name string, payload interface{}) (OutMsg, error) {
	return reqCall(fmt.Sprintf("tcp://%s:%s", name, s2sPortOf(name)), payload, 3000)
}

func connectToReference() {
	r := callRef(map[string]interface{}{"type":"register","name":serverName,"clock":tickSend(),"timestamp":nowTS()})
	tickRecv(r.Clock); serverRank = r.Rank
	fmt.Printf("[%s] Registered | rank=%d\n", serverName, serverRank)
}
func getServerList() {
	r := callRef(map[string]interface{}{"type":"list","name":serverName,"clock":tickSend(),"timestamp":nowTS()})
	tickRecv(r.Clock); serversMu.Lock(); knownServers = r.Servers; serversMu.Unlock()
}
func sendHeartbeat() {
	r := callRef(map[string]interface{}{"type":"heartbeat","name":serverName,"clock":tickSend(),"timestamp":nowTS()})
	tickRecv(r.Clock)
	fmt.Printf("[%s] HEARTBEAT sent | rank=%d | clock=%d\n", serverName, serverRank, atomic.LoadInt64(&logicClock))
}
func syncWithCoordinator() {
	coordMu.Lock(); coord := coordinator; coordMu.Unlock()
	if coord == "" || coord == serverName { return }
	r, err := callS2S(coord, map[string]interface{}{"type":"get_time","name":serverName,"clock":tickSend(),"timestamp":nowTS()})
	if err != nil { go startElection(); return }
	tickRecv(r.Clock)
	if r.Time > 0 {
		newOff := r.Time - float64(time.Now().UnixNano())/1e9
		offsetMu.Lock(); timeOffset = newOff; offsetMu.Unlock()
		fmt.Printf("[%s] CLOCK SYNC | coord=%s | ref_time=%.3f | offset=%.6f\n", serverName, coord, r.Time, newOff)
	}
}
func announceCoordinator() {
	clk := tickSend()
	payload, _ := msgpack.Marshal(map[string]interface{}{"coordinator":serverName,"clock":clk,"timestamp":nowTS()})
	pubSock.Send(zmq4.NewMsgFrom([]byte("servers"), payload))
	fmt.Printf("[%s] ELECTED as coordinator | clock=%d\n", serverName, clk)
}
func startElection() {
	fmt.Printf("[%s] Starting election | rank=%d\n", serverName, serverRank)
	getServerList()
	serversMu.Lock()
	others := make([]map[string]interface{}, 0)
	for _, s := range knownServers { if s["name"] != serverName { others = append(others, s) } }
	serversMu.Unlock()
	type cand struct{ name string; rank int }
	candidates := []cand{{serverName, serverRank}}
	for _, srv := range others {
		name := fmt.Sprintf("%v", srv["name"])
		r, err := callS2S(name, map[string]interface{}{"type":"election","name":serverName,"rank":serverRank,"clock":tickSend()})
		if err == nil { candidates = append(candidates, cand{name, r.Rank}) }
	}
	winner := candidates[0]
	for _, c := range candidates[1:] { if c.rank < winner.rank { winner = c } }
	coordMu.Lock(); coordinator = winner.name; coordMu.Unlock()
	if winner.name == serverName { announceCoordinator() }
	fmt.Printf("[%s] Election result: coordinator='%s'\n", serverName, winner.name)
}

func s2sServerThread() {
	ctx := context.Background(); sock := zmq4.NewRep(ctx); defer sock.Close()
	sock.Listen(fmt.Sprintf("tcp://*:%s", s2sPort))
	for {
		zmqMsg, err := sock.Recv(); if err != nil { continue }
		var msg InMsg; msgpack.Unmarshal(zmqMsg.Frames[0], &msg); tickRecv(msg.Clock)
		var resp OutMsg
		switch msg.Type {
		case "get_time": resp = OutMsg{Status:"ok",Time:nowTS(),Clock:tickSend(),Timestamp:nowTS()}
		case "election": resp = OutMsg{Status:"ok",Rank:serverRank,Clock:tickSend(),Timestamp:nowTS()}
		default:         resp = OutMsg{Status:"error",Message:"Unknown S2S",Clock:tickSend()}
		}
		raw, _ := msgpack.Marshal(resp); sock.Send(zmq4.NewMsg(raw))
	}
}
func serversSubThread() {
	ctx := context.Background(); sub := zmq4.NewSub(ctx); defer sub.Close()
	sub.Dial(fmt.Sprintf("tcp://%s:%s", proxyHost, xpubPort))
	time.Sleep(500 * time.Millisecond); sub.SetOption(zmq4.OptionSubscribe, "servers")
	for {
		zmqMsg, err := sub.Recv(); if err != nil || len(zmqMsg.Frames) < 2 { continue }
		var data map[string]interface{}; msgpack.Unmarshal(zmqMsg.Frames[1], &data)
		if clk, ok := data["clock"].(int64); ok { tickRecv(clk) }
		if coord, ok := data["coordinator"].(string); ok && coord != "" {
			coordMu.Lock(); coordinator = coord; coordMu.Unlock()
			fmt.Printf("[%s] New coordinator: '%s'\n", serverName, coord)
		}
	}
}

func makeResp(status, message string) OutMsg {
	return OutMsg{Status:status,Message:message,Clock:tickSend(),Timestamp:nowTS()}
}
func handleLogin(msg InMsg) OutMsg {
	if msg.Username == "" { return makeResp("error","Username cannot be empty") }
	dbMu.Lock()
	db.Exec("INSERT OR IGNORE INTO users (username,created_at) VALUES (?,?)", msg.Username, nowTS())
	db.Exec("INSERT INTO logins (username,timestamp) VALUES (?,?)", msg.Username, nowTS())
	dbMu.Unlock()
	r := makeResp("ok", fmt.Sprintf("Welcome, %s!", msg.Username)); r.Rank = serverRank; return r
}
func handleCreateChannel(msg InMsg) OutMsg {
	if msg.ChannelName == "" { return makeResp("error","Channel name cannot be empty") }
	dbMu.Lock()
	_, err := db.Exec("INSERT INTO channels (name,created_by,created_at) VALUES (?,?,?)", msg.ChannelName, msg.Username, nowTS())
	dbMu.Unlock()
	if err != nil { return makeResp("error", fmt.Sprintf("Channel '%s' already exists", msg.ChannelName)) }
	return makeResp("ok", fmt.Sprintf("Channel '%s' created!", msg.ChannelName))
}
func handleListChannels() OutMsg {
	dbMu.Lock()
	rows, _ := db.Query("SELECT name FROM channels ORDER BY created_at"); defer rows.Close()
	var channels []string
	for rows.Next() { var n string; rows.Scan(&n); channels = append(channels, n) }
	dbMu.Unlock()
	if channels == nil { channels = []string{} }
	r := makeResp("ok","OK"); r.Data = channels; return r
}
func handlePublish(msg InMsg) OutMsg {
	if msg.ChannelName == "" || msg.Message == "" { return makeResp("error","Channel and message required") }
	dbMu.Lock()
	var name string
	err := db.QueryRow("SELECT name FROM channels WHERE name=?", msg.ChannelName).Scan(&name)
	dbMu.Unlock()
	if err != nil { return makeResp("error", fmt.Sprintf("Channel '%s' does not exist", msg.ChannelName)) }
	clk  := tickSend(); ts := nowTS()
	msgID := makeMsgID(msg.ChannelName, msg.Username, msg.Message, ts)
	dbMu.Lock()
	db.Exec("INSERT OR IGNORE INTO messages (msg_id,channel,username,message,timestamp,clock,origin) VALUES (?,?,?,?,?,?,?)",
		msgID, msg.ChannelName, msg.Username, msg.Message, ts, clk, serverName)
	dbMu.Unlock()
	payload, _ := msgpack.Marshal(PubPayload{Channel:msg.ChannelName,Username:msg.Username,Message:msg.Message,
		Timestamp:ts,Received:ts,Clock:clk,Origin:serverName})
	pubSock.Send(zmq4.NewMsgFrom([]byte(msg.ChannelName), payload))
	fmt.Printf("[%s] PUB  | channel=%-15s | from=%-12s | clock=%d\n", serverName, msg.ChannelName, msg.Username, clk)
	return makeResp("ok","Published!")
}
func handleHistory(msg InMsg) OutMsg {
	if msg.ChannelName == "" { return makeResp("error","Channel required") }
	dbMu.Lock()
	rows, _ := db.Query("SELECT username,message,timestamp,clock,origin FROM messages WHERE channel=? ORDER BY timestamp", msg.ChannelName)
	defer rows.Close()
	var history []map[string]interface{}
	for rows.Next() {
		var u,m,o string; var ts float64; var clk int64
		rows.Scan(&u,&m,&ts,&clk,&o)
		history = append(history, map[string]interface{}{"username":u,"message":m,"timestamp":ts,"clock":clk,"origin":o})
	}
	dbMu.Unlock()
	r := makeResp("ok","OK"); r.Data = history; return r
}

func main() {
	port      := os.Getenv("PORT");        if port == "" { port = "5551" }
	s2sPort    = os.Getenv("S2S_PORT");    if s2sPort == "" { s2sPort = "5561" }
	proxyHost  = os.Getenv("PROXY_HOST");  if proxyHost == "" { proxyHost = "proxy" }
	xsubPort   = os.Getenv("XSUB_PORT");   if xsubPort == "" { xsubPort = "5557" }
	xpubPort   = os.Getenv("XPUB_PORT");   if xpubPort == "" { xpubPort = "5558" }
	refHost    = os.Getenv("REF_HOST");    if refHost == "" { refHost = "reference" }
	refPort    = os.Getenv("REF_PORT");    if refPort == "" { refPort = "5559" }
	serverName = os.Getenv("SERVER_NAME"); if serverName == "" { serverName = "server-go" }

	initDB()
	ctx := context.Background()
	pubSock = zmq4.NewPub(ctx); pubSock.Dial(fmt.Sprintf("tcp://%s:%s", proxyHost, xsubPort))
	time.Sleep(time.Second)

	time.Sleep(2 * time.Second)
	connectToReference(); getServerList()

	go s2sServerThread(); go serversSubThread(); go replicationThread()
	time.Sleep(time.Second); go startElection()

	repSock := zmq4.NewRep(ctx); defer repSock.Close()
	repSock.Listen(fmt.Sprintf("tcp://*:%s", port))
	fmt.Printf("[%s] Listening on port %s | rank=%d\n", serverName, port, serverRank)

	var msgCount int64
	for {
		zmqMsg, err := repSock.Recv(); if err != nil { continue }
		var msg InMsg; msgpack.Unmarshal(zmqMsg.Frames[0], &msg)
		tickRecv(msg.Clock); atomic.AddInt64(&msgCount, 1)
		fmt.Printf("[%s] RECV | type=%-10s | from=%-12s | clock=%d | lc=%d\n",
			serverName, msg.Type, msg.Username, msg.Clock, atomic.LoadInt64(&logicClock))
		var resp OutMsg
		switch msg.Type {
		case "login":   resp = handleLogin(msg)
		case "channel": resp = handleCreateChannel(msg)
		case "list":    resp = handleListChannels()
		case "publish": resp = handlePublish(msg)
		case "history": resp = handleHistory(msg)
		default:        resp = makeResp("error", fmt.Sprintf("Unknown: %s", msg.Type))
		}
		fmt.Printf("[%s] SEND | status=%-8s | clock=%d\n", serverName, resp.Status, resp.Clock)
		raw, _ := msgpack.Marshal(resp); repSock.Send(zmq4.NewMsg(raw))
		if atomic.LoadInt64(&msgCount)%15 == 0 { go sendHeartbeat(); go syncWithCoordinator() }
	}
}