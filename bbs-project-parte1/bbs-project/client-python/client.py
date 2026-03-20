import zmq
import msgpack
import time
import os

BOT_NAME    = os.getenv("BOT_NAME",    "bot-py-1")
SERVER_HOST = os.getenv("SERVER_HOST", "server-python")
SERVER_PORT = os.getenv("SERVER_PORT", "5550")

CHANNELS = ["geral", "random", "noticias", "projetos", "python-talk"]


# ── comunicação ─────────────────────────────────────────────────────────────

def send_recv(socket, payload):
    raw = msgpack.packb(payload, use_bin_type=True)
    print(f"[{BOT_NAME}] SEND | type={payload['type']:<10} | ts={payload['timestamp']:.3f}", flush=True)
    socket.send(raw)

    resp = msgpack.unpackb(socket.recv(), raw=False)
    print(f"[{BOT_NAME}] RECV | status={resp['status']:<8} | msg={resp['message']}", flush=True)
    return resp


# ── ações do bot ─────────────────────────────────────────────────────────────

def login(socket):
    while True:
        resp = send_recv(socket, {"type": "login", "username": BOT_NAME, "timestamp": time.time()})
        if resp["status"] == "ok":
            print(f"[{BOT_NAME}] ✔ Login successful!", flush=True)
            return
        print(f"[{BOT_NAME}] ✘ Login failed: {resp['message']} — retrying in 2s...", flush=True)
        time.sleep(2)


def create_channel(socket, name):
    return send_recv(socket, {
        "type":         "channel",
        "username":     BOT_NAME,
        "channel_name": name,
        "timestamp":    time.time(),
    })


def list_channels(socket):
    resp = send_recv(socket, {"type": "list", "username": BOT_NAME, "timestamp": time.time()})
    if resp["status"] == "ok":
        channels = resp.get("data", [])
        print(f"[{BOT_NAME}] Channels available: {channels}", flush=True)
    return resp


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    time.sleep(3)   # aguarda servidor subir

    ctx    = zmq.Context()
    socket = ctx.socket(zmq.REQ)
    socket.connect(f"tcp://{SERVER_HOST}:{SERVER_PORT}")
    print(f"[{BOT_NAME}] Connected to {SERVER_HOST}:{SERVER_PORT}", flush=True)

    login(socket)
    time.sleep(0.5)

    list_channels(socket)
    time.sleep(0.5)

    for ch in CHANNELS:
        create_channel(socket, ch)
        time.sleep(0.3)

    list_channels(socket)

    print(f"[{BOT_NAME}] ✔ Part 1 done!", flush=True)


if __name__ == "__main__":
    main()
