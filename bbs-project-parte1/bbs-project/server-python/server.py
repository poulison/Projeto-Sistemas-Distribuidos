import zmq
import msgpack
import sqlite3
import time
import os

PORT   = int(os.getenv("PORT", "5550"))
DB_PATH = "/data/server.db"


# ── banco de dados ──────────────────────────────────────────────────────────

def init_db(conn):
    c = conn.cursor()
    c.executescript("""
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
    """)
    conn.commit()


# ── handlers ────────────────────────────────────────────────────────────────

def handle_login(data, conn):
    username  = str(data.get("username", "")).strip()
    timestamp = float(data.get("timestamp", time.time()))

    if not username:
        return {"status": "error", "message": "Username cannot be empty", "timestamp": time.time()}

    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO users (username, created_at) VALUES (?, ?)", (username, timestamp))
    c.execute("INSERT INTO logins (username, timestamp) VALUES (?, ?)", (username, timestamp))
    conn.commit()

    return {"status": "ok", "message": f"Welcome, {username}!", "timestamp": time.time()}


def handle_create_channel(data, conn):
    channel  = str(data.get("channel_name", "")).strip()
    username = str(data.get("username", ""))
    ts       = float(data.get("timestamp", time.time()))

    if not channel:
        return {"status": "error", "message": "Channel name cannot be empty", "timestamp": time.time()}
    if len(channel) > 32 or not channel.replace("-", "").replace("_", "").isalnum():
        return {"status": "error", "message": "Channel name invalid (max 32 chars, alphanumeric/-/_)", "timestamp": time.time()}

    try:
        conn.execute("INSERT INTO channels (name, created_by, created_at) VALUES (?, ?, ?)", (channel, username, ts))
        conn.commit()
        return {"status": "ok", "message": f"Channel '{channel}' created!", "timestamp": time.time()}
    except sqlite3.IntegrityError:
        return {"status": "error", "message": f"Channel '{channel}' already exists", "timestamp": time.time()}


def handle_list_channels(conn):
    rows = conn.execute("SELECT name FROM channels ORDER BY created_at").fetchall()
    channels = [r[0] for r in rows]
    return {"status": "ok", "message": "OK", "data": channels, "timestamp": time.time()}


# ── main ────────────────────────────────────────────────────────────────────

def main():
    os.makedirs("/data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    ctx    = zmq.Context()
    socket = ctx.socket(zmq.REP)
    socket.bind(f"tcp://*:{PORT}")

    print(f"[SERVER-PYTHON] Listening on port {PORT}", flush=True)

    while True:
        raw  = socket.recv()
        data = msgpack.unpackb(raw, raw=False)

        msg_type = data.get("type", "")
        username = data.get("username", "?")
        ts       = data.get("timestamp", 0)

        print(f"[SERVER-PYTHON] RECV | type={msg_type:<10} | from={username:<15} | ts={ts:.3f}", flush=True)

        if   msg_type == "login":   resp = handle_login(data, conn)
        elif msg_type == "channel": resp = handle_create_channel(data, conn)
        elif msg_type == "list":    resp = handle_list_channels(conn)
        else:
            resp = {"status": "error", "message": f"Unknown type: {msg_type}", "timestamp": time.time()}

        print(f"[SERVER-PYTHON] SEND | status={resp['status']:<8} | msg={resp['message']}", flush=True)

        socket.send(msgpack.packb(resp, use_bin_type=True))


if __name__ == "__main__":
    main()
