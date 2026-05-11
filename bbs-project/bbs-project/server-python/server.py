import zmq
import msgpack
import sqlite3
import time
import os
import threading
import hashlib

PORT        = int(os.getenv("PORT", "5550"))
S2S_PORT    = int(os.getenv("S2S_PORT", "5560"))
PROXY_HOST  = os.getenv("PROXY_HOST", "proxy")
XSUB_PORT   = int(os.getenv("XSUB_PORT", "5557"))
XPUB_PORT   = int(os.getenv("XPUB_PORT", "5558"))
REF_HOST    = os.getenv("REF_HOST", "reference")
REF_PORT    = int(os.getenv("REF_PORT", "5559"))
SERVER_NAME = os.getenv("SERVER_NAME", "server-python")
DB_PATH     = "/data/server.db"

# ── relógio lógico ────────────────────────────────────────────────────────────
logical_clock = 0
clock_lock    = threading.Lock()

def tick_send():
    global logical_clock
    with clock_lock:
        logical_clock += 1
        return logical_clock

def tick_recv(r):
    global logical_clock
    with clock_lock:
        logical_clock = max(logical_clock, int(r))

# ── relógio físico ────────────────────────────────────────────────────────────
time_offset = 0.0
offset_lock = threading.Lock()

def now_ts():
    with offset_lock:
        return time.time() + time_offset

def set_offset(new_offset):
    with offset_lock:
        global time_offset
        time_offset = new_offset

# ── estado ────────────────────────────────────────────────────────────────────
rank          = 0
coordinator   = None
coord_lock    = threading.Lock()
known_servers = []
servers_lock  = threading.Lock()
ctx_global    = None
pub_socket    = None

# ── banco ─────────────────────────────────────────────────────────────────────
def make_msg_id(channel, username, message, timestamp):
    key = f"{channel}|{username}|{message}|{timestamp:.3f}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]

def init_db(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS logins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL, timestamp REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS channels (
            name TEXT PRIMARY KEY,
            created_by TEXT NOT NULL, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS messages (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            msg_id   TEXT    UNIQUE NOT NULL,
            channel  TEXT    NOT NULL,
            username TEXT    NOT NULL,
            message  TEXT    NOT NULL,
            timestamp REAL   NOT NULL,
            clock    INTEGER NOT NULL DEFAULT 0,
            origin   TEXT    NOT NULL DEFAULT 'local');
    """)
    conn.commit()

# ── replicação: salva mensagem recebida de outro servidor ─────────────────────
conn_lock = threading.Lock()
db_conn   = None

def replicate_message(data):
    channel   = data.get("channel", "")
    username  = data.get("username", "")
    message   = data.get("message", "")
    timestamp = float(data.get("timestamp", now_ts()))
    clk       = int(data.get("clock", 0))
    origin    = data.get("origin", "remote")

    if not channel or not message:
        return

    msg_id = make_msg_id(channel, username, message, timestamp)
    with conn_lock:
        db_conn.execute(
            "INSERT OR IGNORE INTO messages (msg_id,channel,username,message,timestamp,clock,origin) "
            "VALUES (?,?,?,?,?,?,?)",
            (msg_id, channel, username, message, timestamp, clk, origin)
        )
        # garante que o canal existe localmente
        db_conn.execute(
            "INSERT OR IGNORE INTO channels (name, created_by, created_at) VALUES (?,?,?)",
            (channel, username, timestamp)
        )
        db_conn.commit()
    print(f"[{SERVER_NAME}] REPL | channel={channel:<15} | from={username:<12} | origin={origin}", flush=True)

# ── thread de replicação: SUB em TODOS os tópicos ────────────────────────────
def replication_thread():
    ctx = zmq.Context.instance()
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{PROXY_HOST}:{XPUB_PORT}")
    sub.setsockopt_string(zmq.SUBSCRIBE, "")  # inscreve em TUDO
    time.sleep(1)
    print(f"[{SERVER_NAME}] REPL SUB | subscribed to all topics on proxy", flush=True)

    while True:
        try:
            topic_raw, raw = sub.recv_multipart()
            topic = topic_raw.decode()

            # ignora tópico 'servers' (eleição) — não é mensagem de usuário
            if topic == "servers":
                continue

            data = msgpack.unpackb(raw, raw=False)
            tick_recv(data.get("clock", 0))
            replicate_message(data)
        except Exception as e:
            print(f"[{SERVER_NAME}] REPL error: {e}", flush=True)

# ── referência ────────────────────────────────────────────────────────────────
def call_reference(payload, timeout=5000):
    ctx  = zmq.Context.instance()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, timeout)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(f"tcp://{REF_HOST}:{REF_PORT}")
    try:
        sock.send(msgpack.packb(payload, use_bin_type=True))
        return msgpack.unpackb(sock.recv(), raw=False)
    except:
        return {}
    finally:
        sock.close()

def connect_to_reference():
    global rank
    resp = call_reference({"type": "register", "name": SERVER_NAME,
                           "clock": tick_send(), "timestamp": now_ts()})
    tick_recv(resp.get("clock", 0))
    rank = resp.get("rank", 0)
    print(f"[{SERVER_NAME}] Registered | rank={rank}", flush=True)

def get_server_list():
    global known_servers
    resp = call_reference({"type": "list", "name": SERVER_NAME,
                           "clock": tick_send(), "timestamp": now_ts()})
    tick_recv(resp.get("clock", 0))
    with servers_lock:
        known_servers = resp.get("servers", [])

def send_heartbeat():
    resp = call_reference({"type": "heartbeat", "name": SERVER_NAME,
                           "clock": tick_send(), "timestamp": now_ts()})
    tick_recv(resp.get("clock", 0))
    print(f"[{SERVER_NAME}] HEARTBEAT sent | rank={rank} | clock={logical_clock}", flush=True)

# ── sync Berkeley ─────────────────────────────────────────────────────────────
def call_server(host, s2s_port, payload, timeout=3000):
    ctx  = zmq.Context.instance()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, timeout)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(f"tcp://{host}:{s2s_port}")
    try:
        sock.send(msgpack.packb(payload, use_bin_type=True))
        return msgpack.unpackb(sock.recv(), raw=False)
    except:
        return None
    finally:
        sock.close()

def get_s2s_port(srv_name):
    base = {"server-python": 5550, "server-go": 5551,
            "server-csharp": 5552, "server-c": 5553, "server-lua": 5554}
    return base.get(srv_name, 5550) + 10

def sync_with_coordinator():
    with coord_lock:
        coord = coordinator
    if coord is None or coord == SERVER_NAME:
        return
    s2s = get_s2s_port(coord)
    resp = call_server(coord, s2s, {"type": "get_time", "name": SERVER_NAME,
                                     "clock": tick_send(), "timestamp": now_ts()})
    if resp is None:
        start_election()
        return
    tick_recv(resp.get("clock", 0))
    ref_time = resp.get("time", 0)
    if ref_time > 0:
        set_offset(ref_time - time.time())
        print(f"[{SERVER_NAME}] CLOCK SYNC | coord={coord} | ref_time={ref_time:.3f} | offset={time_offset:.6f}", flush=True)

# ── eleição ───────────────────────────────────────────────────────────────────
def start_election():
    global coordinator
    print(f"[{SERVER_NAME}] Starting election | rank={rank}", flush=True)
    get_server_list()
    with servers_lock:
        others = [s for s in known_servers if s["name"] != SERVER_NAME]

    candidates = [{"name": SERVER_NAME, "rank": rank}]
    for srv in others:
        s2s  = get_s2s_port(srv["name"])
        resp = call_server(srv["name"], s2s, {"type": "election", "name": SERVER_NAME,
                                               "rank": rank, "clock": tick_send()})
        if resp is not None:
            candidates.append(srv)

    winner = min(candidates, key=lambda s: s["rank"])
    with coord_lock:
        coordinator = winner["name"]
    if winner["name"] == SERVER_NAME:
        announce_coordinator()
    print(f"[{SERVER_NAME}] Election result: coordinator='{coordinator}'", flush=True)

def announce_coordinator():
    clk     = tick_send()
    payload = msgpack.packb({"coordinator": SERVER_NAME, "clock": clk, "timestamp": now_ts()}, use_bin_type=True)
    pub_socket.send_multipart([b"servers", payload])
    print(f"[{SERVER_NAME}] ELECTED as coordinator | clock={clk}", flush=True)

# ── thread S2S REP ────────────────────────────────────────────────────────────
def s2s_server_thread():
    ctx  = zmq.Context.instance()
    sock = ctx.socket(zmq.REP)
    sock.bind(f"tcp://*:{S2S_PORT}")
    while True:
        try:
            raw  = sock.recv()
            data = msgpack.unpackb(raw, raw=False)
            tick_recv(data.get("clock", 0))
            msg_type = data.get("type", "")
            if msg_type == "get_time":
                resp = {"status": "ok", "time": now_ts(), "clock": tick_send(), "timestamp": now_ts()}
            elif msg_type == "election":
                resp = {"status": "ok", "name": SERVER_NAME, "rank": rank, "clock": tick_send()}
            else:
                resp = {"status": "error", "message": f"Unknown: {msg_type}", "clock": tick_send()}
            sock.send(msgpack.packb(resp, use_bin_type=True))
        except Exception as e:
            print(f"[{SERVER_NAME}] S2S error: {e}", flush=True)

# ── thread SUB 'servers' ──────────────────────────────────────────────────────
def servers_subscriber_thread():
    global coordinator
    ctx = zmq.Context.instance()
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{PROXY_HOST}:{XPUB_PORT}")
    sub.setsockopt_string(zmq.SUBSCRIBE, "servers")
    while True:
        try:
            _, raw = sub.recv_multipart()
            data   = msgpack.unpackb(raw, raw=False)
            tick_recv(data.get("clock", 0))
            new_coord = data.get("coordinator", "")
            if new_coord:
                with coord_lock:
                    coordinator = new_coord
                print(f"[{SERVER_NAME}] New coordinator: '{new_coord}'", flush=True)
        except Exception as e:
            print(f"[{SERVER_NAME}] servers SUB error: {e}", flush=True)

# ── handlers clientes ─────────────────────────────────────────────────────────
def make_resp(d):
    d["clock"]     = tick_send()
    d["timestamp"] = now_ts()
    return d

def handle_login(data):
    username = str(data.get("username", "")).strip()
    if not username:
        return make_resp({"status": "error", "message": "Username cannot be empty"})
    with conn_lock:
        db_conn.execute("INSERT OR IGNORE INTO users (username,created_at) VALUES (?,?)", (username, now_ts()))
        db_conn.execute("INSERT INTO logins (username,timestamp) VALUES (?,?)", (username, now_ts()))
        db_conn.commit()
    return make_resp({"status": "ok", "message": f"Welcome, {username}!", "rank": rank})

def handle_create_channel(data):
    channel  = str(data.get("channel_name", "")).strip()
    username = str(data.get("username", ""))
    if not channel:
        return make_resp({"status": "error", "message": "Channel name cannot be empty"})
    if len(channel) > 32 or not channel.replace("-","").replace("_","").isalnum():
        return make_resp({"status": "error", "message": "Channel name invalid"})
    try:
        with conn_lock:
            db_conn.execute("INSERT INTO channels (name,created_by,created_at) VALUES (?,?,?)",
                            (channel, username, now_ts()))
            db_conn.commit()
        return make_resp({"status": "ok", "message": f"Channel '{channel}' created!"})
    except sqlite3.IntegrityError:
        return make_resp({"status": "error", "message": f"Channel '{channel}' already exists"})

def handle_list_channels():
    with conn_lock:
        rows = db_conn.execute("SELECT name FROM channels ORDER BY created_at").fetchall()
    return make_resp({"status": "ok", "message": "OK", "data": [r[0] for r in rows]})

def handle_publish(data):
    channel  = str(data.get("channel_name", "")).strip()
    username = str(data.get("username", "")).strip()
    message  = str(data.get("message", "")).strip()
    clk_in   = int(data.get("clock", 0))

    if not channel or not message:
        return make_resp({"status": "error", "message": "Channel and message required"})

    with conn_lock:
        row = db_conn.execute("SELECT name FROM channels WHERE name=?", (channel,)).fetchone()
    if not row:
        return make_resp({"status": "error", "message": f"Channel '{channel}' does not exist"})

    clk    = tick_send()
    ts     = now_ts()
    msg_id = make_msg_id(channel, username, message, ts)

    with conn_lock:
        db_conn.execute(
            "INSERT OR IGNORE INTO messages (msg_id,channel,username,message,timestamp,clock,origin) VALUES (?,?,?,?,?,?,?)",
            (msg_id, channel, username, message, ts, clk, SERVER_NAME)
        )
        db_conn.commit()

    payload = msgpack.packb({
        "channel": channel, "username": username, "message": message,
        "timestamp": ts, "received": ts, "clock": clk,
        "origin": SERVER_NAME,
    }, use_bin_type=True)
    pub_socket.send_multipart([channel.encode(), payload])
    print(f"[{SERVER_NAME}] PUB  | channel={channel:<15} | from={username:<12} | clock={clk} | {message[:30]}", flush=True)
    return make_resp({"status": "ok", "message": "Published!"})

def handle_history(data):
    channel = str(data.get("channel_name", "")).strip()
    if not channel:
        return make_resp({"status": "error", "message": "Channel required"})
    with conn_lock:
        rows = db_conn.execute(
            "SELECT username, message, timestamp, clock, origin FROM messages "
            "WHERE channel=? ORDER BY timestamp", (channel,)
        ).fetchall()
    history = [{"username": r[0], "message": r[1],
                "timestamp": r[2], "clock": r[3], "origin": r[4]} for r in rows]
    return make_resp({"status": "ok", "message": "OK", "data": history})

# ── main ──────────────────────────────────────────────────────────────────────
def main():
    global ctx_global, pub_socket, db_conn

    os.makedirs("/data", exist_ok=True)
    db_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    init_db(db_conn)

    ctx_global = zmq.Context()
    pub_socket = ctx_global.socket(zmq.PUB)
    pub_socket.connect(f"tcp://{PROXY_HOST}:{XSUB_PORT}")
    time.sleep(1)

    time.sleep(2)
    connect_to_reference()
    get_server_list()

    threading.Thread(target=s2s_server_thread,        daemon=True).start()
    threading.Thread(target=servers_subscriber_thread, daemon=True).start()
    threading.Thread(target=replication_thread,        daemon=True).start()
    time.sleep(1.5)

    start_election()

    rep_socket = ctx_global.socket(zmq.REP)
    rep_socket.bind(f"tcp://*:{PORT}")
    print(f"[{SERVER_NAME}] Listening on port {PORT} | rank={rank}", flush=True)

    msg_count = 0
    while True:
        raw  = rep_socket.recv()
        data = msgpack.unpackb(raw, raw=False)
        tick_recv(data.get("clock", 0))
        msg_count += 1

        msg_type = data.get("type", "")
        username = data.get("username", "?")
        print(f"[{SERVER_NAME}] RECV | type={msg_type:<10} | from={username:<12} | clock={data.get('clock',0)} | lc={logical_clock}", flush=True)

        if   msg_type == "login":   resp = handle_login(data)
        elif msg_type == "channel": resp = handle_create_channel(data)
        elif msg_type == "list":    resp = handle_list_channels()
        elif msg_type == "publish": resp = handle_publish(data)
        elif msg_type == "history": resp = handle_history(data)
        else: resp = make_resp({"status": "error", "message": f"Unknown: {msg_type}"})

        print(f"[{SERVER_NAME}] SEND | status={resp['status']:<8} | clock={resp['clock']}", flush=True)
        rep_socket.send(msgpack.packb(resp, use_bin_type=True))

        if msg_count % 15 == 0:
            threading.Thread(target=send_heartbeat,        daemon=True).start()
            threading.Thread(target=sync_with_coordinator, daemon=True).start()

if __name__ == "__main__":
    main()