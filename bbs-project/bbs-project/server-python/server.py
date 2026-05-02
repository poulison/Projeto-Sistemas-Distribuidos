import zmq
import msgpack
import sqlite3
import time
import os
import threading

PORT        = int(os.getenv("PORT", "5550"))
S2S_PORT    = int(os.getenv("S2S_PORT", "5560"))   # porta servidor-a-servidor
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
time_offset  = 0.0
offset_lock  = threading.Lock()

def now_ts():
    with offset_lock:
        return time.time() + time_offset

def set_offset(new_offset):
    with offset_lock:
        global time_offset
        time_offset = new_offset

# ── estado de eleição / coordenador ──────────────────────────────────────────
rank         = 0
coordinator  = None          # nome do coordenador atual
coord_lock   = threading.Lock()
known_servers = []           # lista de {"name":..., "rank":...}
servers_lock  = threading.Lock()

ctx_global   = None
pub_socket   = None          # socket PUB global

# ── banco ─────────────────────────────────────────────────────────────────────
def init_db(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, timestamp REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, username TEXT NOT NULL, message TEXT NOT NULL, timestamp REAL NOT NULL, clock INTEGER NOT NULL DEFAULT 0);
    """)
    conn.commit()

# ── chamada ao serviço de referência ─────────────────────────────────────────
def call_reference(payload, timeout=5000):
    ctx  = zmq.Context.instance()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, timeout)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(f"tcp://{REF_HOST}:{REF_PORT}")
    try:
        sock.send(msgpack.packb(payload, use_bin_type=True))
        return msgpack.unpackb(sock.recv(), raw=False)
    except Exception as e:
        print(f"[{SERVER_NAME}] Reference error: {e}", flush=True)
        return {}
    finally:
        sock.close()

def connect_to_reference():
    global rank
    clk  = tick_send()
    resp = call_reference({"type": "register", "name": SERVER_NAME,
                           "clock": clk, "timestamp": time.time()})
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
    return known_servers

def send_heartbeat():
    resp = call_reference({"type": "heartbeat", "name": SERVER_NAME,
                           "clock": tick_send(), "timestamp": now_ts()})
    tick_recv(resp.get("clock", 0))
    # sem sync de tempo aqui (parte 4: tempo vem do coordenador)
    print(f"[{SERVER_NAME}] HEARTBEAT sent | rank={rank} | clock={logical_clock}", flush=True)

# ── chamada servidor-a-servidor ───────────────────────────────────────────────
def call_server(host, s2s_port, payload, timeout=3000):
    ctx  = zmq.Context.instance()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, timeout)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(f"tcp://{host}:{s2s_port}")
    try:
        sock.send(msgpack.packb(payload, use_bin_type=True))
        return msgpack.unpackb(sock.recv(), raw=False)
    except Exception:
        return None
    finally:
        sock.close()

# ── Berkeley: sincroniza com coordenador ─────────────────────────────────────
def sync_with_coordinator():
    global coordinator
    with coord_lock:
        coord = coordinator

    if coord is None or coord == SERVER_NAME:
        return  # sou o coordenador ou não há um ainda

    # descobre a porta S2S do coordenador
    with servers_lock:
        coord_info = next((s for s in known_servers if s["name"] == coord), None)

    if coord_info is None:
        print(f"[{SERVER_NAME}] Coordinator '{coord}' not in server list, triggering election", flush=True)
        start_election()
        return

    s2s = get_s2s_port(coord)
    resp = call_server(coord, s2s, {"type": "get_time", "name": SERVER_NAME,
                                     "clock": tick_send(), "timestamp": now_ts()})
    if resp is None:
        print(f"[{SERVER_NAME}] Coordinator '{coord}' unreachable, triggering election", flush=True)
        start_election()
        return

    tick_recv(resp.get("clock", 0))
    ref_time = resp.get("time", 0)
    if ref_time > 0:
        new_offset = ref_time - time.time()
        set_offset(new_offset)
        print(f"[{SERVER_NAME}] CLOCK SYNC | coord={coord} | ref_time={ref_time:.3f} | offset={new_offset:.6f}", flush=True)

# ── Eleição (Bully simplificado: menor rank ganha) ────────────────────────────
def get_s2s_port(srv_name):
    # convenção: S2S_PORT = PORT + 10
    base = {"server-python": 5550, "server-go": 5551,
            "server-csharp": 5552, "server-c": 5553, "server-lua": 5554}
    client_port = base.get(srv_name, 5550)
    return client_port + 10

def start_election():
    global coordinator
    print(f"[{SERVER_NAME}] Starting election | my rank={rank}", flush=True)
    get_server_list()

    with servers_lock:
        others = [s for s in known_servers if s["name"] != SERVER_NAME]

    # envia REQ eleição para todos
    responded = []
    for srv in others:
        s2s = get_s2s_port(srv["name"])
        resp = call_server(srv["name"], s2s,
                           {"type": "election", "name": SERVER_NAME,
                            "rank": rank, "clock": tick_send()})
        if resp is not None:
            responded.append(srv)

    # o servidor com menor rank entre os que responderam + eu mesmo é o coordenador
    candidates = [{"name": SERVER_NAME, "rank": rank}] + responded
    winner = min(candidates, key=lambda s: s["rank"])

    with coord_lock:
        coordinator = winner["name"]

    if winner["name"] == SERVER_NAME:
        # anuncio que sou o coordenador
        announce_coordinator()
    print(f"[{SERVER_NAME}] Election result: coordinator='{coordinator}'", flush=True)

def announce_coordinator():
    clk = tick_send()
    payload = msgpack.packb({
        "coordinator": SERVER_NAME,
        "clock": clk,
        "timestamp": now_ts()
    }, use_bin_type=True)
    # publica no tópico "servers"
    pub_socket.send_multipart([b"servers", payload])
    print(f"[{SERVER_NAME}] ELECTED as coordinator | clock={clk}", flush=True)

# ── thread: SUB no tópico 'servers' ──────────────────────────────────────────
def servers_subscriber_thread():
    global coordinator
    ctx = zmq.Context.instance()
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{PROXY_HOST}:{XPUB_PORT}")
    sub.setsockopt_string(zmq.SUBSCRIBE, "servers")
    print(f"[{SERVER_NAME}] SUB listening on 'servers' topic", flush=True)
    while True:
        try:
            _, raw = sub.recv_multipart()
            data   = msgpack.unpackb(raw, raw=False)
            tick_recv(data.get("clock", 0))
            new_coord = data.get("coordinator", "")
            if new_coord:
                with coord_lock:
                    coordinator = new_coord
                print(f"[{SERVER_NAME}] New coordinator announced: '{new_coord}'", flush=True)
        except Exception as e:
            print(f"[{SERVER_NAME}] servers SUB error: {e}", flush=True)

# ── thread: REP servidor-a-servidor ──────────────────────────────────────────
def s2s_server_thread():
    ctx  = zmq.Context.instance()
    sock = ctx.socket(zmq.REP)
    sock.bind(f"tcp://*:{S2S_PORT}")
    print(f"[{SERVER_NAME}] S2S listening on port {S2S_PORT}", flush=True)
    while True:
        try:
            raw  = sock.recv()
            data = msgpack.unpackb(raw, raw=False)
            tick_recv(data.get("clock", 0))
            msg_type = data.get("type", "")

            if msg_type == "get_time":
                # responde com a hora atual (sou coordenador)
                clk  = tick_send()
                resp = {"status": "ok", "time": now_ts(),
                        "clock": clk, "timestamp": now_ts()}
                print(f"[{SERVER_NAME}] S2S get_time | from={data.get('name','?')} | time={now_ts():.3f}", flush=True)

            elif msg_type == "election":
                clk  = tick_send()
                resp = {"status": "ok", "name": SERVER_NAME,
                        "rank": rank, "clock": clk}
                print(f"[{SERVER_NAME}] S2S election | from={data.get('name','?')} | my rank={rank}", flush=True)

            else:
                resp = {"status": "error", "message": f"Unknown S2S: {msg_type}", "clock": tick_send()}

            sock.send(msgpack.packb(resp, use_bin_type=True))
        except Exception as e:
            print(f"[{SERVER_NAME}] S2S error: {e}", flush=True)

# ── handlers clientes ─────────────────────────────────────────────────────────
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
        conn.execute("INSERT INTO channels (name, created_by, created_at) VALUES (?, ?, ?)",
                     (channel, username, now_ts()))
        conn.commit()
        return make_resp({"status": "ok", "message": f"Channel '{channel}' created!"})
    except sqlite3.IntegrityError:
        return make_resp({"status": "error", "message": f"Channel '{channel}' already exists"})

def handle_list_channels(conn):
    rows = conn.execute("SELECT name FROM channels ORDER BY created_at").fetchall()
    return make_resp({"status": "ok", "message": "OK", "data": [r[0] for r in rows]})

def handle_publish(data, conn):
    channel   = str(data.get("channel_name", "")).strip()
    username  = str(data.get("username", "")).strip()
    message   = str(data.get("message", "")).strip()
    msg_clock = int(data.get("clock", 0))

    if not channel or not message:
        return make_resp({"status": "error", "message": "Channel and message required"})
    row = conn.execute("SELECT name FROM channels WHERE name = ?", (channel,)).fetchone()
    if not row:
        return make_resp({"status": "error", "message": f"Channel '{channel}' does not exist"})

    conn.execute("INSERT INTO messages (channel, username, message, timestamp, clock) VALUES (?,?,?,?,?)",
                 (channel, username, message, now_ts(), msg_clock))
    conn.commit()

    clk = tick_send()
    payload = msgpack.packb({
        "channel": channel, "username": username, "message": message,
        "timestamp": now_ts(), "received": now_ts(), "clock": clk,
    }, use_bin_type=True)
    pub_socket.send_multipart([channel.encode(), payload])
    print(f"[{SERVER_NAME}] PUB  | channel={channel:<15} | from={username:<12} | clock={clk} | {message[:30]}", flush=True)
    return make_resp({"status": "ok", "message": "Published!"})

# ── main ──────────────────────────────────────────────────────────────────────
def main():
    global ctx_global, pub_socket

    os.makedirs("/data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    ctx_global = zmq.Context()

    pub_socket = ctx_global.socket(zmq.PUB)
    pub_socket.connect(f"tcp://{PROXY_HOST}:{XSUB_PORT}")
    time.sleep(1)

    time.sleep(2)
    connect_to_reference()
    get_server_list()

    # inicia threads
    threading.Thread(target=s2s_server_thread,       daemon=True).start()
    threading.Thread(target=servers_subscriber_thread, daemon=True).start()
    time.sleep(1)

    # eleição inicial
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

        if   msg_type == "login":   resp = handle_login(data, conn)
        elif msg_type == "channel": resp = handle_create_channel(data, conn)
        elif msg_type == "list":    resp = handle_list_channels(conn)
        elif msg_type == "publish": resp = handle_publish(data, conn)
        else: resp = make_resp({"status": "error", "message": f"Unknown: {msg_type}"})

        print(f"[{SERVER_NAME}] SEND | status={resp['status']:<8} | clock={resp['clock']}", flush=True)
        rep_socket.send(msgpack.packb(resp, use_bin_type=True))

        # a cada 15 mensagens: heartbeat + sync de relógio
        if msg_count % 15 == 0:
            threading.Thread(target=send_heartbeat,       daemon=True).start()
            threading.Thread(target=sync_with_coordinator, daemon=True).start()

if __name__ == "__main__":
    main()
