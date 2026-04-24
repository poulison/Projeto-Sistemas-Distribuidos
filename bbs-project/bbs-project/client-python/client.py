import zmq
import msgpack
import time
import os
import random
import threading

BOT_NAME    = os.getenv("BOT_NAME",    "bot-py-1")
SERVER_HOST = os.getenv("SERVER_HOST", "server-python")
SERVER_PORT = os.getenv("SERVER_PORT", "5550")
PROXY_HOST  = os.getenv("PROXY_HOST",  "proxy")
XPUB_PORT   = os.getenv("XPUB_PORT",   "5558")

RANDOM_WORDS = ["ola","mundo","sistema","distribuido","mensagem","canal","teste",
                "python","zmq","pubsub","broker","topico","servidor","cliente","rede"]

# ── relógio lógico ────────────────────────────────────────────────────────────
logical_clock = 0
clock_lock    = threading.Lock()

def tick_send():
    global logical_clock
    with clock_lock:
        logical_clock += 1
        return logical_clock

def tick_recv(received):
    global logical_clock
    with clock_lock:
        logical_clock = max(logical_clock, int(received))

subscribed_channels = []
req_socket = None

def random_message():
    return " ".join(random.choices(RANDOM_WORDS, k=random.randint(3, 7)))

def send_recv(payload):
    payload["clock"] = tick_send()
    raw = msgpack.packb(payload, use_bin_type=True)
    print(f"[{BOT_NAME}] SEND | type={payload['type']:<10} | clock={payload['clock']} | ts={payload['timestamp']:.3f}", flush=True)
    req_socket.send(raw)
    resp = msgpack.unpackb(req_socket.recv(), raw=False)
    tick_recv(resp.get("clock", 0))
    print(f"[{BOT_NAME}] RECV | status={resp['status']:<8} | clock={resp.get('clock',0)} | msg={resp['message']}", flush=True)
    return resp

def login():
    while True:
        resp = send_recv({"type": "login", "username": BOT_NAME, "timestamp": time.time()})
        if resp["status"] == "ok":
            print(f"[{BOT_NAME}] ✔ Login successful! | server rank={resp.get('rank','?')}", flush=True)
            return
        time.sleep(2)

def list_channels():
    resp = send_recv({"type": "list", "username": BOT_NAME, "timestamp": time.time()})
    channels = resp.get("data", []) if resp["status"] == "ok" else []
    print(f"[{BOT_NAME}] Channels: {channels}", flush=True)
    return channels

def create_channel(name):
    return send_recv({"type": "channel", "username": BOT_NAME, "channel_name": name, "timestamp": time.time()})

def publish(channel, message):
    return send_recv({"type": "publish", "username": BOT_NAME, "channel_name": channel,
                      "message": message, "timestamp": time.time()})

def subscriber_thread():
    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{PROXY_HOST}:{XPUB_PORT}")
    time.sleep(1)
    for ch in subscribed_channels:
        sub.setsockopt_string(zmq.SUBSCRIBE, ch)
        print(f"[{BOT_NAME}] SUB  | subscribed to '{ch}'", flush=True)
    while True:
        try:
            topic, raw = sub.recv_multipart()
            data = msgpack.unpackb(raw, raw=False)
            tick_recv(data.get("clock", 0))
            print(f"[{BOT_NAME}] MSG  | channel={data.get('channel','?'):<12} | from={data.get('username','?'):<12} "
                  f"| clock={data.get('clock',0)} | sent={data.get('timestamp',0):.3f} | recv={time.time():.3f} | {data.get('message','?')}", flush=True)
        except Exception as e:
            print(f"[{BOT_NAME}] SUB error: {e}", flush=True)

def main():
    global req_socket, subscribed_channels
    time.sleep(4)

    ctx = zmq.Context()
    req_socket = ctx.socket(zmq.REQ)
    req_socket.connect(f"tcp://{SERVER_HOST}:{SERVER_PORT}")
    print(f"[{BOT_NAME}] Connected to {SERVER_HOST}:{SERVER_PORT}", flush=True)

    login()
    channels = list_channels()
    if len(channels) < 5:
        new_ch = f"ch-{BOT_NAME.replace('-','')}-{random.randint(100,999)}"
        create_channel(new_ch)
        channels = list_channels()

    subscribed_channels = random.sample(channels, min(3, len(channels)))
    threading.Thread(target=subscriber_thread, daemon=True).start()
    time.sleep(1.5)

    print(f"[{BOT_NAME}] Starting publish loop", flush=True)
    while True:
        channel = random.choice(channels)
        for _ in range(10):
            publish(channel, random_message())
            time.sleep(1)
        channels = list_channels()

if __name__ == "__main__":
    main()
