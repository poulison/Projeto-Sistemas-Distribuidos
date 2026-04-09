import zmq
import msgpack
import sqlite3
import time
import os

PORT       = int(os.getenv("PORT", "5550"))
PROXY_HOST = os.getenv("PROXY_HOST", "proxy")
XSUB_PORT  = int(os.getenv("XSUB_PORT", "5557"))
DB_PATH    = "/data/server.db"


def init_db(conn):
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS logins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL, timestamp REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS channels (
            name TEXT PRIMARY KEY,
            created_by TEXT NOT NULL, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel TEXT NOT NULL, username TEXT NOT NULL,
            message TEXT NOT NULL, timestamp REAL NOT NULL);
    """)
    conn.commit()


def handle_login(data, conn):
    username  = str(data.get("username", "")).strip()
    timestamp = float(data.get("timestamp", time.time()))
    if not username:
        return {"status": "error", "message": "Username cannot be empty", "timestamp": time.time()}
    conn.execute("INSERT OR IGNORE INTO users (username, created_at) VALUES (?, ?)", (username, timestamp))
    conn.execute("INSERT INTO logins (username, timestamp) VALUES (?, ?)", (username, timestamp))
    conn.commit()
    return {"status": "ok", "message": f"Welcome, {username}!", "timestamp": time.time()}


def handle_create_channel(data, conn):
    channel  = str(data.get("channel_name", "")).strip()
    username = str(data.get("username", ""))
    ts       = float(data.get("timestamp", time.time()))
    if not channel:
        return {"status": "error", "message": "Channel name cannot be empty", "timestamp": time.time()}
    if len(channel) > 32 or not channel.replace("-", "").replace("_", "").isalnum():
        return {"status": "error", "message": "Channel name invalid", "timestamp": time.time()}
    try:
        conn.execute("INSERT INTO channels (name, created_by, created_at) VALUES (?, ?, ?)", (channel, username, ts))
        conn.commit()
        return {"status": "ok", "message": f"Channel '{channel}' created!", "timestamp": time.time()}
    except sqlite3.IntegrityError:
        return {"status": "error", "message": f"Channel '{channel}' already exists", "timestamp": time.time()}


def handle_list_channels(conn):
    rows = conn.execute("SELECT name FROM channels ORDER BY created_at").fetchall()
    return {"status": "ok", "message": "OK", "data": [r[0] for r in rows], "timestamp": time.time()}


def handle_publish(data, conn, pub_socket):
    channel  = str(data.get("channel_name", "")).strip()
    username = str(data.get("username", "")).strip()
    message  = str(data.get("message", "")).strip()
    ts       = float(data.get("timestamp", time.time()))

    if not channel or not message:
        return {"status": "error", "message": "Channel and message are required", "timestamp": time.time()}

    row = conn.execute("SELECT name FROM channels WHERE name = ?", (channel,)).fetchone()
    if not row:
        return {"status": "error", "message": f"Channel '{channel}' does not exist", "timestamp": time.time()}

    conn.execute("INSERT INTO messages (channel, username, message, timestamp) VALUES (?, ?, ?, ?)",
                 (channel, username, message, ts))
    conn.commit()

    payload = msgpack.packb({
        "channel": channel, "username": username,
        "message": message, "timestamp": ts, "received": time.time(),
    }, use_bin_type=True)
    pub_socket.send_multipart([channel.encode(), payload])

    print(f"[SERVER-PYTHON] PUB  | channel={channel:<15} | from={username:<15} | msg={message[:40]}", flush=True)
    return {"status": "ok", "message": "Published!", "timestamp": time.time()}


def main():
    os.makedirs("/data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    ctx = zmq.Context()

    rep_socket = ctx.socket(zmq.REP)
    rep_socket.bind(f"tcp://*:{PORT}")

    pub_socket = ctx.socket(zmq.PUB)
    pub_socket.connect(f"tcp://{PROXY_HOST}:{XSUB_PORT}")
    time.sleep(0.5)

    print(f"[SERVER-PYTHON] Listening on port {PORT}", flush=True)
    print(f"[SERVER-PYTHON] Publishing to proxy {PROXY_HOST}:{XSUB_PORT}", flush=True)

    while True:
        raw  = rep_socket.recv()
        data = msgpack.unpackb(raw, raw=False)
        msg_type = data.get("type", "")
        username = data.get("username", "?")
        ts       = data.get("timestamp", 0)

        print(f"[SERVER-PYTHON] RECV | type={msg_type:<10} | from={username:<15} | ts={ts:.3f}", flush=True)

        if   msg_type == "login":   resp = handle_login(data, conn)
        elif msg_type == "channel": resp = handle_create_channel(data, conn)
        elif msg_type == "list":    resp = handle_list_channels(conn)
        elif msg_type == "publish": resp = handle_publish(data, conn, pub_socket)
        else: resp = {"status": "error", "message": f"Unknown type: {msg_type}", "timestamp": time.time()}

        print(f"[SERVER-PYTHON] SEND | status={resp['status']:<8} | msg={resp['message']}", flush=True)
        rep_socket.send(msgpack.packb(resp, use_bin_type=True))


if __name__ == "__main__":
    main()
