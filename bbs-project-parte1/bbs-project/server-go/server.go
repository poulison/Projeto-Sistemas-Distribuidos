package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	zmq4 "github.com/go-zeromq/zmq4"
	_ "modernc.org/sqlite"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

// ── structs ──────────────────────────────────────────────────────────────────

type InMsg struct {
	Type        string  `msgpack:"type"`
	Username    string  `msgpack:"username"`
	ChannelName string  `msgpack:"channel_name"`
	Timestamp   float64 `msgpack:"timestamp"`
}

type OutMsg struct {
	Status    string   `msgpack:"status"`
	Message   string   `msgpack:"message"`
	Data      []string `msgpack:"data,omitempty"`
	Timestamp float64  `msgpack:"timestamp"`
}

// ── helpers ──────────────────────────────────────────────────────────────────

func nowTS() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

func errResp(msg string) OutMsg {
	return OutMsg{Status: "error", Message: msg, Timestamp: nowTS()}
}

func okResp(msg string) OutMsg {
	return OutMsg{Status: "ok", Message: msg, Timestamp: nowTS()}
}

// ── banco de dados ────────────────────────────────────────────────────────────

var db *sql.DB

func initDB() {
	os.MkdirAll("/data", 0755)
	var err error
	db, err = sql.Open("sqlite", "/data/server.db")
	if err != nil {
		panic(fmt.Sprintf("[SERVER-GO] Failed to open DB: %v", err))
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			username   TEXT PRIMARY KEY,
			created_at REAL NOT NULL
		);
		CREATE TABLE IF NOT EXISTS logins (
			id        INTEGER PRIMARY KEY AUTOINCREMENT,
			username  TEXT    NOT NULL,
			timestamp REAL    NOT NULL
		);
		CREATE TABLE IF NOT EXISTS channels (
			name       TEXT PRIMARY KEY,
			created_by TEXT NOT NULL,
			created_at REAL NOT NULL
		);
	`)
	if err != nil {
		panic(fmt.Sprintf("[SERVER-GO] Failed to create tables: %v", err))
	}
}

// ── handlers ─────────────────────────────────────────────────────────────────

func handleLogin(msg InMsg) OutMsg {
	if msg.Username == "" {
		return errResp("Username cannot be empty")
	}
	db.Exec("INSERT OR IGNORE INTO users (username, created_at) VALUES (?, ?)", msg.Username, msg.Timestamp)
	db.Exec("INSERT INTO logins (username, timestamp) VALUES (?, ?)", msg.Username, msg.Timestamp)
	return okResp(fmt.Sprintf("Welcome, %s!", msg.Username))
}

func handleCreateChannel(msg InMsg) OutMsg {
	if msg.ChannelName == "" {
		return errResp("Channel name cannot be empty")
	}
	if len(msg.ChannelName) > 32 {
		return errResp("Channel name too long (max 32 chars)")
	}
	_, err := db.Exec("INSERT INTO channels (name, created_by, created_at) VALUES (?, ?, ?)",
		msg.ChannelName, msg.Username, msg.Timestamp)
	if err != nil {
		return errResp(fmt.Sprintf("Channel '%s' already exists", msg.ChannelName))
	}
	return okResp(fmt.Sprintf("Channel '%s' created!", msg.ChannelName))
}

func handleListChannels() OutMsg {
	rows, err := db.Query("SELECT name FROM channels ORDER BY created_at")
	if err != nil {
		return errResp("Failed to query channels")
	}
	defer rows.Close()

	var channels []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		channels = append(channels, name)
	}
	if channels == nil {
		channels = []string{}
	}
	return OutMsg{Status: "ok", Message: "OK", Data: channels, Timestamp: nowTS()}
}

// ── main ─────────────────────────────────────────────────────────────────────

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "5551"
	}

	initDB()

	ctx  := context.Background()
	sock := zmq4.NewRep(ctx)
	defer sock.Close()

	addr := fmt.Sprintf("tcp://*:%s", port)
	if err := sock.Listen(addr); err != nil {
		panic(fmt.Sprintf("[SERVER-GO] Listen error: %v", err))
	}
	fmt.Printf("[SERVER-GO] Listening on port %s\n", port)

	for {
		zmqMsg, err := sock.Recv()
		if err != nil {
			fmt.Printf("[SERVER-GO] Recv error: %v\n", err)
			continue
		}

		var msg InMsg
		if err := msgpack.Unmarshal(zmqMsg.Frames[0], &msg); err != nil {
			fmt.Printf("[SERVER-GO] Unmarshal error: %v\n", err)
			continue
		}

		fmt.Printf("[SERVER-GO] RECV | type=%-10s | from=%-15s | ts=%.3f\n",
			msg.Type, msg.Username, msg.Timestamp)

		var resp OutMsg
		switch msg.Type {
		case "login":
			resp = handleLogin(msg)
		case "channel":
			resp = handleCreateChannel(msg)
		case "list":
			resp = handleListChannels()
		default:
			resp = errResp(fmt.Sprintf("Unknown type: %s", msg.Type))
		}

		fmt.Printf("[SERVER-GO] SEND | status=%-8s | msg=%s\n", resp.Status, resp.Message)

		raw, _ := msgpack.Marshal(resp)
		sock.Send(zmq4.NewMsg(raw))
	}
}
