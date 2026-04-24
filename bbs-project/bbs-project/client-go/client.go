package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	zmq4 "github.com/go-zeromq/zmq4"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

type ReqMsg struct {
	Type        string  `msgpack:"type"`
	Username    string  `msgpack:"username"`
	ChannelName string  `msgpack:"channel_name,omitempty"`
	Message     string  `msgpack:"message,omitempty"`
	Timestamp   float64 `msgpack:"timestamp"`
	Clock       int64   `msgpack:"clock"`
}
type RespMsg struct {
	Status    string   `msgpack:"status"`
	Message   string   `msgpack:"message"`
	Data      []string `msgpack:"data"`
	Timestamp float64  `msgpack:"timestamp"`
	Clock     int64    `msgpack:"clock"`
	Rank      int      `msgpack:"rank"`
}
type PubPayload struct {
	Channel   string  `msgpack:"channel"`
	Username  string  `msgpack:"username"`
	Message   string  `msgpack:"message"`
	Timestamp float64 `msgpack:"timestamp"`
	Received  float64 `msgpack:"received"`
	Clock     int64   `msgpack:"clock"`
}

var (
	botName    = getEnv("BOT_NAME",    "bot-go-1")
	serverHost = getEnv("SERVER_HOST", "server-go")
	serverPort = getEnv("SERVER_PORT", "5551")
	proxyHost  = getEnv("PROXY_HOST",  "proxy")
	xpubPort   = getEnv("XPUB_PORT",   "5558")
	reqSock    zmq4.Socket

	logicClock int64
	clockMu    sync.Mutex
)

var words = []string{"ola","mundo","sistema","distribuido","mensagem","canal",
	"teste","golang","zmq","pubsub","broker","topico","servidor","rede"}

func getEnv(k, d string) string { if v := os.Getenv(k); v != "" { return v }; return d }

func tickSend() int64 {
	clockMu.Lock(); defer clockMu.Unlock()
	logicClock++; return logicClock
}
func tickRecv(r int64) {
	clockMu.Lock(); defer clockMu.Unlock()
	if r > logicClock { logicClock = r }
}

func nowTS() float64 { return float64(time.Now().UnixNano()) / 1e9 }

func randomMsg() string {
	n := 3 + rand.Intn(5); parts := make([]string, n)
	for i := range parts { parts[i] = words[rand.Intn(len(words))] }
	result := ""
	for i, p := range parts { if i > 0 { result += " " }; result += p }
	return result
}

func sendRecv(payload ReqMsg) RespMsg {
	payload.Clock = tickSend()
	raw, _ := msgpack.Marshal(payload)
	fmt.Printf("[%s] SEND | type=%-10s | clock=%d | ts=%.3f\n", botName, payload.Type, payload.Clock, payload.Timestamp)
	reqSock.Send(zmq4.NewMsg(raw))
	zmqMsg, _ := reqSock.Recv()
	var resp RespMsg
	msgpack.Unmarshal(zmqMsg.Frames[0], &resp)
	tickRecv(resp.Clock)
	fmt.Printf("[%s] RECV | status=%-8s | clock=%d | msg=%s\n", botName, resp.Status, resp.Clock, resp.Message)
	return resp
}

func subscriberThread(channels []string) {
	ctx := context.Background()
	sub := zmq4.NewSub(ctx)
	defer sub.Close()
	sub.Dial(fmt.Sprintf("tcp://%s:%s", proxyHost, xpubPort))
	time.Sleep(500 * time.Millisecond)
	for _, ch := range channels {
		sub.SetOption(zmq4.OptionSubscribe, ch)
		fmt.Printf("[%s] SUB  | subscribed to '%s'\n", botName, ch)
	}
	for {
		zmqMsg, err := sub.Recv()
		if err != nil || len(zmqMsg.Frames) < 2 { continue }
		var p PubPayload
		msgpack.Unmarshal(zmqMsg.Frames[1], &p)
		tickRecv(p.Clock)
		fmt.Printf("[%s] MSG  | channel=%-12s | from=%-12s | clock=%d | sent=%.3f | recv=%.3f | %s\n",
			botName, p.Channel, p.Username, p.Clock, p.Timestamp, nowTS(), p.Message)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	time.Sleep(4 * time.Second)

	ctx := context.Background()
	reqSock = zmq4.NewReq(ctx)
	defer reqSock.Close()
	reqSock.Dial(fmt.Sprintf("tcp://%s:%s", serverHost, serverPort))
	fmt.Printf("[%s] Connected to %s:%s\n", botName, serverHost, serverPort)

	for {
		resp := sendRecv(ReqMsg{Type: "login", Username: botName, Timestamp: nowTS()})
		if resp.Status == "ok" {
			fmt.Printf("[%s] ✔ Login | server rank=%d\n", botName, resp.Rank)
			break
		}
		time.Sleep(2 * time.Second)
	}

	resp := sendRecv(ReqMsg{Type: "list", Username: botName, Timestamp: nowTS()})
	channels := resp.Data
	if len(channels) < 5 {
		newCh := fmt.Sprintf("ch-go-%d", rand.Intn(900)+100)
		sendRecv(ReqMsg{Type: "channel", Username: botName, ChannelName: newCh, Timestamp: nowTS()})
		resp = sendRecv(ReqMsg{Type: "list", Username: botName, Timestamp: nowTS()})
		channels = resp.Data
	}

	rand.Shuffle(len(channels), func(i, j int) { channels[i], channels[j] = channels[j], channels[i] })
	subChs := channels; if len(subChs) > 3 { subChs = channels[:3] }

	go subscriberThread(subChs)
	time.Sleep(1500 * time.Millisecond)

	fmt.Printf("[%s] Starting publish loop\n", botName)
	for {
		ch := channels[rand.Intn(len(channels))]
		for i := 0; i < 10; i++ {
			sendRecv(ReqMsg{Type: "publish", Username: botName, ChannelName: ch, Message: randomMsg(), Timestamp: nowTS()})
			time.Sleep(time.Second)
		}
		resp = sendRecv(ReqMsg{Type: "list", Username: botName, Timestamp: nowTS()})
		channels = resp.Data
	}
}
