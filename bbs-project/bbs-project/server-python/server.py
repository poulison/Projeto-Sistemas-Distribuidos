import zmq
import msgpack
import sqlite3
import time
import os
import threading

PORT       = int(os.getenv("PORT", "5550"))
PROXY_HOST = os.getenv("PROXY_HOST", "proxy")
XSUB_PORT  = int(os.getenv("XSUB_PORT", "5557"))
REF_HOST   = os.getenv("REF_HOST", "reference")
REF_PORT   = int(os.getenv("REF_PORT", "5559"))
SERVER_NAME = os.getenv("SERVER_NAME", "server-python")
DB_PATH    = "/data/server.db"

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

# ── relógio físico (ajustado pelo reference) ──────────────────────────────────
time_offset = 0.0

def now_ts():
    return time.time() + time_offset

# ── banco ─────────────────────────────────────────────────────────────────────
def init_db(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, timestamp REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, username TEXT NOT NULL, message TEXT NOT NULL, timestamp REAL NOT NULL, clock INTEGER NOT NULL DEFAULT 0);
    """)
    conn.commit()

# ── referência ────────────────────────────────────────────────────────────────
rank = 0

def connect_to_reference():
    global rank, time_offset
    ctx    = zmq.Context.instance()
    socket = ctx.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 5000)
    socket.connect(f"tcp://{REF_HOST}:{REF_PORT}")

    # registra e obtém rank
    try:
        clk = tick_send()
        socket.send(msgpack.packb({"type": "register", "name": SERVER_NAME, "clock": clk, "timestamp": now_ts()}, use_bin_type=True))
        resp = msgpack.unpackb(socket.recv(), raw=False)
        tick_recv(resp.get("clock", 0))
        rank = resp.get("rank", 0)
        # sincroniza relógio físico
        ref_time = resp.get("timestamp", time.time())
        time_offset = ref_time - time.time()
        print(f"[{SERVER_NAME}] Registered | rank={rank} | time_offset={time_offset:.3f}s", flush=True)
    except Exception as e:
        print(f"[{SERVER_NAME}] Reference error: {e}", flush=True)
    finally:
        socket.close()

def send_heartbeat(msg_count):
    global time_offset
    ctx    = zmq.Context.instance()
    socket = ctx.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 5000)
    socket.connect(f"tcp://{REF_HOST}:{REF_PORT}")
    try:
        clk = tick_send()
        socket.send(msgpack.packb({"type": "heartbeat", "name": SERVER_NAME, "clock": clk, "timestamp": now_ts(), "msg_count": msg_count}, use_bin_type=True))
        resp = msgpack.unpackb(socket.recv(), raw=False)
        tick_recv(resp.get("clock", 0))
        ref_time = resp.get("time", time.time())
        time_offset = ref_time - time.time()
        print(f"[{SERVER_NAME}] HEARTBEAT sent | rank={rank} | clock={logical_clock} | time_offset={time_offset:.3f}s", flush=True)
    except Exception as e:
        print(f"[{SERVER_NAME}] Heartbeat error: {e}", flush=True)
    finally:
        socket.close()

# ── handlers ──────────────────────────────────────────────────────────────────
def make_resp(d):
    d["clock"]     = tick_send()
    d["timestamp"] = now_ts()
    return d

def handle_login(data, conn):
    username = str(data.get("username", "")).strip()
    if not username:
        return make_resp({"status": "error", "message": "Username cannot be empty"})
    conn.execute("INSERT OR IGNORE INTO users (username, created_at) VALUES (?, ?)", (username, now_ts()))
    conn.execute("INSERT INTO logins (username, timestamp) VALUES (?, ?)", (username, now_ts()))
    conn.commit()
    return make_resp({"status": "ok", "message": f"Welcome, {username}!", "rank": rank})

def handle_create_channel(data, conn):
    channel  = str(data.get("channel_name", "")).strip()
    username = str(data.get("username", ""))
    if not channel:
        return make_resp({"status": "error", "message": "Channel name cannot be empty"})
    if len(channel) > 32 or not channel.replace("-","").replace("_","").isalnum():
        return make_resp({"status": "error", "message": "Channel name invalid"})
    try:
        conn.execute("INSERT INTO channels (name, created_by, created_at) VALUES (?, ?, ?)", (channel, username, now_ts()))
        conn.commit()
        return make_resp({"status": "ok", "message": f"Channel '{channel}' created!"})
    except sqlite3.IntegrityError:
        return make_resp({"status": "error", "message": f"Channel '{channel}' already exists"})

def handle_list_channels(conn):
    rows = conn.execute("SELECT name FROM channels ORDER BY created_at").fetchall()
    return make_resp({"status": "ok", "message": "OK", "data": [r[0] for r in rows]})

def handle_publish(data, conn, pub_socket):
    channel  = str(data.get("channel_name", "")).strip()
    username = str(data.get("username", "")).strip()
    message  = str(data.get("message", "")).strip()
    msg_clock = int(data.get("clock", 0))

    if not channel or not message:
        return make_resp({"status": "error", "message": "Channel and message are required"})
    row = conn.execute("SELECT name FROM channels WHERE name = ?", (channel,)).fetchone()
    if not row:
        return make_resp({"status": "error", "message": f"Channel '{channel}' does not exist"})

    conn.execute("INSERT INTO messages (channel, username, message, timestamp, clock) VALUES (?, ?, ?, ?, ?)",
                 (channel, username, message, now_ts(), msg_clock))
    conn.commit()

    send_clk = tick_send()
    payload = msgpack.packb({
        "channel": channel, "username": username, "message": message,
        "timestamp": now_ts(), "received": now_ts(), "clock": send_clk,
    }, use_bin_type=True)
    pub_socket.send_multipart([channel.encode(), payload])

    print(f"[{SERVER_NAME}] PUB  | channel={channel:<15} | from={username:<12} | clock={send_clk} | {message[:30]}", flush=True)
    return make_resp({"status": "ok", "message": "Published!"})

# ── main ──────────────────────────────────────────────────────────────────────
def main():
    os.makedirs("/data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    time.sleep(2)  # aguarda reference subir
    connect_to_reference()

    ctx = zmq.Context()
    rep_socket = ctx.socket(zmq.REP)
    rep_socket.bind(f"tcp://*:{PORT}")

    pub_socket = ctx.socket(zmq.PUB)
    pub_socket.connect(f"tcp://{PROXY_HOST}:{XSUB_PORT}")
    time.sleep(0.5)

    print(f"[{SERVER_NAME}] Listening on port {PORT} | rank={rank}", flush=True)

    msg_count = 0
    while True:
        raw  = rep_socket.recv()
        data = msgpack.unpackb(raw, raw=False)

        recv_clock = int(data.get("clock", 0))
        tick_recv(recv_clock)
        msg_count += 1

        msg_type = data.get("type", "")
        username = data.get("username", "?")
        print(f"[{SERVER_NAME}] RECV | type={msg_type:<10} | from={username:<12} | clock={recv_clock} | lc={logical_clock}", flush=True)

        if   msg_type == "login":   resp = handle_login(data, conn)
        elif msg_type == "channel": resp = handle_create_channel(data, conn)
        elif msg_type == "list":    resp = handle_list_channels(conn)
        elif msg_type == "publish": resp = handle_publish(data, conn, pub_socket)
        else: resp = make_resp({"status": "error", "message": f"Unknown type: {msg_type}"})

        print(f"[{SERVER_NAME}] SEND | status={resp['status']:<8} | clock={resp['clock']}", flush=True)
        rep_socket.send(msgpack.packb(resp, use_bin_type=True))

        # heartbeat a cada 10 mensagens
        if msg_count % 10 == 0:
            threading.Thread(target=send_heartbeat, args=(msg_count,), daemon=True).start()

if __name__ == "__main__":
    main()
