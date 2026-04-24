import zmq
import msgpack
import time
import threading
import os

PORT    = int(os.getenv("REF_PORT", "5559"))
TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "30"))  # remove servidor após 30s sem heartbeat

# ── estado ────────────────────────────────────────────────────────────────────
servers      = {}   # name -> {"rank": int, "last_beat": float}
rank_counter = 0
lock         = threading.Lock()

# ── relógio lógico ────────────────────────────────────────────────────────────
logical_clock = 0

def tick_send():
    global logical_clock
    logical_clock += 1
    return logical_clock

def tick_recv(received):
    global logical_clock
    logical_clock = max(logical_clock, received)

# ── heartbeat watchdog ────────────────────────────────────────────────────────
def watchdog():
    while True:
        time.sleep(10)
        now = time.time()
        with lock:
            dead = [n for n, v in servers.items() if now - v["last_beat"] > TIMEOUT]
            for name in dead:
                print(f"[REFERENCE] REMOVE server '{name}' (no heartbeat)", flush=True)
                del servers[name]

# ── handlers ──────────────────────────────────────────────────────────────────
def handle_register(data):
    global rank_counter
    name = str(data.get("name", "")).strip()
    if not name:
        return {"status": "error", "message": "Name required",
                "clock": tick_send(), "timestamp": time.time()}
    with lock:
        if name not in servers:
            rank_counter += 1
            servers[name] = {"rank": rank_counter, "last_beat": time.time()}
            print(f"[REFERENCE] REGISTER '{name}' → rank {rank_counter}", flush=True)
        rank = servers[name]["rank"]
    return {"status": "ok", "rank": rank, "clock": tick_send(), "timestamp": time.time()}

def handle_list(data):
    with lock:
        server_list = [{"name": n, "rank": v["rank"]} for n, v in servers.items()]
    return {"status": "ok", "servers": server_list, "clock": tick_send(), "timestamp": time.time()}

def handle_heartbeat(data):
    name = str(data.get("name", "")).strip()
    with lock:
        if name in servers:
            servers[name]["last_beat"] = time.time()
            print(f"[REFERENCE] HEARTBEAT from '{name}' | clock={data.get('clock',0)}", flush=True)
        else:
            print(f"[REFERENCE] HEARTBEAT from unknown '{name}' — ignoring", flush=True)
    # retorna tempo atual para sincronização do relógio físico
    return {"status": "ok", "time": time.time(), "clock": tick_send(), "timestamp": time.time()}

# ── main ──────────────────────────────────────────────────────────────────────
def main():
    threading.Thread(target=watchdog, daemon=True).start()

    ctx    = zmq.Context()
    socket = ctx.socket(zmq.REP)
    socket.bind(f"tcp://*:{PORT}")
    print(f"[REFERENCE] Listening on port {PORT}", flush=True)

    while True:
        raw  = socket.recv()
        data = msgpack.unpackb(raw, raw=False)
        tick_recv(data.get("clock", 0))

        msg_type = data.get("type", "")
        print(f"[REFERENCE] RECV | type={msg_type:<12} | from={data.get('name','?'):<15} | clock={data.get('clock',0)}", flush=True)

        if   msg_type == "register":  resp = handle_register(data)
        elif msg_type == "list":      resp = handle_list(data)
        elif msg_type == "heartbeat": resp = handle_heartbeat(data)
        else: resp = {"status": "error", "message": f"Unknown: {msg_type}", "clock": tick_send(), "timestamp": time.time()}

        print(f"[REFERENCE] SEND | status={resp['status']:<8} | clock={resp['clock']}", flush=True)
        socket.send(msgpack.packb(resp, use_bin_type=True))

if __name__ == "__main__":
    main()
