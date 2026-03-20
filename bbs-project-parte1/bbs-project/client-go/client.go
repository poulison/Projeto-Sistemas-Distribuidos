package main

import (
	"context"
	"fmt"
	"os"
	"time"

	zmq4 "github.com/go-zeromq/zmq4"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

type OutMsg struct {
	Type        string  `msgpack:"type"`
	Username    string  `msgpack:"username"`
	ChannelName string  `msgpack:"channel_name,omitempty"`
	Timestamp   float64 `msgpack:"timestamp"`
}

type InMsg struct {
	Status    string   `msgpack:"status"`
	Message   string   `msgpack:"message"`
	Data      []string `msgpack:"data"`
	Timestamp float64  `msgpack:"timestamp"`
}

var (
	botName    = os.Getenv("BOT_NAME")
	serverHost = os.Getenv("SERVER_HOST")
	serverPort = os.Getenv("SERVER_PORT")
	sock       zmq4.Socket
)

var channels = []string{"geral", "random", "noticias", "projetos", "go-talk"}

func init() {
	if botName    == "" { botName    = "bot-go-1" }
	if serverHost == "" { serverHost = "server-go" }
	if serverPort == "" { serverPort = "5551" }
}

func sendRecv(payload OutMsg) InMsg {
	raw, _ := msgpack.Marshal(payload)
	fmt.Printf("[%s] SEND | type=%-10s | ts=%.3f\n", botName, payload.Type, payload.Timestamp)

	sock.Send(zmq4.NewMsg(raw))

	zmqMsg, _ := sock.Recv()
	var resp InMsg
	msgpack.Unmarshal(zmqMsg.Frames[0], &resp)

	fmt.Printf("[%s] RECV | status=%-8s | msg=%s\n", botName, resp.Status, resp.Message)
	return resp
}

func login() {
	for {
		resp := sendRecv(OutMsg{Type: "login", Username: botName, Timestamp: float64(time.Now().UnixNano()) / 1e9})
		if resp.Status == "ok" {
			fmt.Printf("[%s] ✔ Login successful!\n", botName)
			return
		}
		fmt.Printf("[%s] ✘ Login failed: %s — retrying in 2s...\n", botName, resp.Message)
		time.Sleep(2 * time.Second)
	}
}

func createChannel(name string) {
	sendRecv(OutMsg{
		Type:        "channel",
		Username:    botName,
		ChannelName: name,
		Timestamp:   float64(time.Now().UnixNano()) / 1e9,
	})
}

func listChannels() {
	resp := sendRecv(OutMsg{Type: "list", Username: botName, Timestamp: float64(time.Now().UnixNano()) / 1e9})
	if resp.Status == "ok" {
		fmt.Printf("[%s] Channels available: %v\n", botName, resp.Data)
	}
}

func main() {
	time.Sleep(3 * time.Second)

	ctx  := context.Background()
	sock  = zmq4.NewReq(ctx)
	defer sock.Close()

	addr := fmt.Sprintf("tcp://%s:%s", serverHost, serverPort)
	if err := sock.Dial(addr); err != nil {
		panic(fmt.Sprintf("[%s] Dial error: %v", botName, err))
	}
	fmt.Printf("[%s] Connected to %s\n", botName, addr)

	login()
	time.Sleep(500 * time.Millisecond)

	listChannels()
	time.Sleep(500 * time.Millisecond)

	for _, ch := range channels {
		createChannel(ch)
		time.Sleep(300 * time.Millisecond)
	}

	listChannels()
	fmt.Printf("[%s] ✔ Part 1 done!\n", botName)
}
