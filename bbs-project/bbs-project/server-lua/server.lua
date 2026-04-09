local zmq     = require "lzmq"
local mp      = require "MessagePack"
local sqlite3 = require "lsqlite3"
local os      = require "os"

local PORT       = tonumber(os.getenv("PORT") or "5554")
local PROXY_HOST = os.getenv("PROXY_HOST") or "proxy"
local XSUB_PORT  = os.getenv("XSUB_PORT")  or "5557"
local DB_PATH    = "/data/server.db"

local db
local pub_socket

local function now_ts() return os.time() + 0.0 end

local function init_db()
    os.execute("mkdir -p /data")
    db = sqlite3.open(DB_PATH)
    db:exec([[
        CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, timestamp REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, username TEXT NOT NULL, message TEXT NOT NULL, timestamp REAL NOT NULL);
    ]])
end

local function make_ok(message, data)
    local t = { status="ok", message=message, timestamp=now_ts() }
    if data then t.data = data end
    return mp.pack(t)
end

local function make_err(message)
    return mp.pack({ status="error", message=message, timestamp=now_ts() })
end

local function handle_login(username, ts)
    if not username or username=="" then return make_err("Username cannot be empty") end
    local s = db:prepare("INSERT OR IGNORE INTO users (username, created_at) VALUES (?, ?)")
    s:bind_values(username, ts); s:step(); s:finalize()
    s = db:prepare("INSERT INTO logins (username, timestamp) VALUES (?, ?)")
    s:bind_values(username, ts); s:step(); s:finalize()
    return make_ok("Welcome, " .. username .. "!")
end

local function handle_create_channel(name, created_by, ts)
    if not name or name=="" then return make_err("Channel name cannot be empty") end
    local s = db:prepare("INSERT INTO channels (name, created_by, created_at) VALUES (?, ?, ?)")
    s:bind_values(name, created_by or "", ts)
    local rc = s:step(); s:finalize()
    if rc == sqlite3.CONSTRAINT then return make_err("Channel '" .. name .. "' already exists") end
    return make_ok("Channel '" .. name .. "' created!")
end

local function handle_list_channels()
    local channels = {}
    for row in db:nrows("SELECT name FROM channels ORDER BY created_at") do
        channels[#channels+1] = row.name
    end
    return make_ok("OK", channels)
end

local function handle_publish(channel, username, message, ts)
    if not channel or channel=="" or not message or message=="" then
        return make_err("Channel and message are required")
    end
    -- verifica canal
    local found = false
    for row in db:nrows("SELECT name FROM channels WHERE name='" .. channel .. "'") do found=true end
    if not found then return make_err("Channel '" .. channel .. "' does not exist") end

    -- persiste
    local s = db:prepare("INSERT INTO messages (channel,username,message,timestamp) VALUES (?,?,?,?)")
    s:bind_values(channel, username, message, ts); s:step(); s:finalize()

    -- publica via PUB socket
    local payload = mp.pack({ channel=channel, username=username, message=message, timestamp=ts, received=now_ts() })
    pub_socket:send({ channel, payload })

    io.write(string.format("[SERVER-LUA] PUB  | channel=%-15s | from=%-15s | msg=%s\n", channel, username, message))
    io.flush()
    return make_ok("Published!")
end

-- ── main ──────────────────────────────────────────────────────────────────────

init_db()

local ctx = zmq.context()

local rep_socket = ctx:socket(zmq.REP)
rep_socket:bind("tcp://*:" .. PORT)

pub_socket = ctx:socket(zmq.PUB)
pub_socket:connect("tcp://" .. PROXY_HOST .. ":" .. XSUB_PORT)
os.execute("sleep 1")

print(string.format("[SERVER-LUA] Listening on port %d", PORT))
print(string.format("[SERVER-LUA] Publishing to proxy %s:%s", PROXY_HOST, XSUB_PORT))
io.flush()

while true do
    local raw, err = rep_socket:recv()
    if not raw then
        print("[SERVER-LUA] Recv error: " .. tostring(err))
    else
        local ok_u, data = pcall(mp.unpack, raw)
        if not ok_u or type(data) ~= "table" then
            rep_socket:send(make_err("Invalid message format"))
        else
            local msg_type     = data.type         or ""
            local username     = data.username     or ""
            local channel_name = data.channel_name or ""
            local message      = data.message      or ""
            local ts           = data.timestamp    or now_ts()

            io.write(string.format("[SERVER-LUA] RECV | type=%-10s | from=%-15s | ts=%.3f\n", msg_type, username, ts))
            io.flush()

            local resp
            if     msg_type=="login"   then resp = handle_login(username, ts)
            elseif msg_type=="channel" then resp = handle_create_channel(channel_name, username, ts)
            elseif msg_type=="list"    then resp = handle_list_channels()
            elseif msg_type=="publish" then resp = handle_publish(channel_name, username, message, ts)
            else   resp = make_err("Unknown type: " .. msg_type)
            end
            rep_socket:send(resp)
        end
    end
end
