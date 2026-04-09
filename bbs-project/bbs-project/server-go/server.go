package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
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
}

type OutMsg struct {
	Status    string   `msgpack:"status"`
	Message   string   `msgpack:"message"`
	Data      []string `msgpack:"data,omitempty"`
	Timestamp float64  `msgpack:"timestamp"`
}

type PubPayload struct {
	Channel   string  `msgpack:"channel"`
	Username  string  `msgpack:"username"`
	Message   string  `msgpack:"message"`
	Timestamp float64 `msgpack:"timestamp"`
	Received  float64 `msgpack:"received"`
}

var db *sql.DB

func nowTS() float64 { return float64(time.Now().UnixNano()) / 1e9 }
func errResp(msg string) OutMsg { return OutMsg{Status: "error", Message: msg, Timestamp: nowTS()} }
func okResp(msg string) OutMsg  { return OutMsg{Status: "ok", Message: msg, Timestamp: nowTS()} }

func initDB() {
	os.MkdirAll("/data", 0755)
	var err error
	db, err = sql.Open("sqlite", "/data/server.db")
	if err != nil { panic(err) }
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, created_at REAL NOT NULL);
		CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, timestamp REAL NOT NULL);
		CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at REAL NOT NULL);
		CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, username TEXT NOT NULL, message TEXT NOT NULL, timestamp REAL NOT NULL);
	`)
	if err != nil { panic(err) }
}

func handleLogin(msg InMsg) OutMsg {
	if msg.Username == "" { return errResp("Username cannot be empty") }
	db.Exec("INSERT OR IGNORE INTO users (username, created_at) VALUES (?, ?)", msg.Username, msg.Timestamp)
	db.Exec("INSERT INTO logins (username, timestamp) VALUES (?, ?)", msg.Username, msg.Timestamp)
	return okResp(fmt.Sprintf("Welcome, %s!", msg.Username))
}

func handleCreateChannel(msg InMsg) OutMsg {
	if msg.ChannelName == "" { return errResp("Channel name cannot be empty") }
	if len(msg.ChannelName) > 32 { return errResp("Channel name too long") }
	_, err := db.Exec("INSERT INTO channels (name, created_by, created_at) VALUES (?, ?, ?)",
		msg.ChannelName, msg.Username, msg.Timestamp)
	if err != nil { return errResp(fmt.Sprintf("Channel '%s' already exists", msg.ChannelName)) }
	return okResp(fmt.Sprintf("Channel '%s' created!", msg.ChannelName))
}

func handleListChannels() OutMsg {
	rows, _ := db.Query("SELECT name FROM channels ORDER BY created_at")
	defer rows.Close()
	var channels []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		channels = append(channels, name)
	}
	if channels == nil { channels = []string{} }
	return OutMsg{Status: "ok", Message: "OK", Data: channels, Timestamp: nowTS()}
}

func handlePublish(msg InMsg, pubSock zmq4.Socket) OutMsg {
	if msg.ChannelName == "" || msg.Message == "" {
		return errResp("Channel and message are required")
	}
	var name string
	err := db.QueryRow("SELECT name FROM channels WHERE name = ?", msg.ChannelName).Scan(&name)
	if err != nil { return errResp(fmt.Sprintf("Channel '%s' does not exist", msg.ChannelName)) }

	db.Exec("INSERT INTO messages (channel, username, message, timestamp) VALUES (?, ?, ?, ?)",
		msg.ChannelName, msg.Username, msg.Message, msg.Timestamp)

	payload, _ := msgpack.Marshal(PubPayload{
		Channel: msg.ChannelName, Username: msg.Username,
		Message: msg.Message, Timestamp: msg.Timestamp, Received: nowTS(),
	})

	zmqMsg := zmq4.NewMsgFrom([]byte(msg.ChannelName), payload)
	pubSock.Send(zmqMsg)

	fmt.Printf("[SERVER-GO] PUB  | channel=%-15s | from=%-15s | msg=%s\n",
		msg.ChannelName, msg.Username, msg.Message)
	return okResp("Published!")
}

func main() {
	port      := os.Getenv("PORT");      if port == "" { port = "5551" }
	proxyHost := os.Getenv("PROXY_HOST"); if proxyHost == "" { proxyHost = "proxy" }
	xsubPort  := os.Getenv("XSUB_PORT"); if xsubPort == "" { xsubPort = "5557" }

	initDB()

	ctx := context.Background()

	repSock := zmq4.NewRep(ctx)
	defer repSock.Close()
	if err := repSock.Listen(fmt.Sprintf("tcp://*:%s", port)); err != nil { panic(err) }

	pubSock := zmq4.NewPub(ctx)
	defer pubSock.Close()
	if err := pubSock.Dial(fmt.Sprintf("tcp://%s:%s", proxyHost, xsubPort)); err != nil { panic(err) }
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("[SERVER-GO] Listening on port %s\n", port)
	fmt.Printf("[SERVER-GO] Publishing to proxy %s:%s\n", proxyHost, xsubPort)

	for {
		zmqMsg, err := repSock.Recv()
		if err != nil { fmt.Printf("[SERVER-GO] Recv error: %v\n", err); continue }

		var msg InMsg
		if err := msgpack.Unmarshal(zmqMsg.Frames[0], &msg); err != nil { continue }

		fmt.Printf("[SERVER-GO] RECV | type=%-10s | from=%-15s | ts=%.3f\n", msg.Type, msg.Username, msg.Timestamp)

		var resp OutMsg
		switch msg.Type {
		case "login":   resp = handleLogin(msg)
		case "channel": resp = handleCreateChannel(msg)
		case "list":    resp = handleListChannels()
		case "publish": resp = handlePublish(msg, pubSock)
		default:        resp = errResp(fmt.Sprintf("Unknown type: %s", msg.Type))
		}

		fmt.Printf("[SERVER-GO] SEND | status=%-8s | msg=%s\n", resp.Status, resp.Message)
		raw, _ := msgpack.Marshal(resp)
		repSock.Send(zmq4.NewMsg(raw))
	}
}
