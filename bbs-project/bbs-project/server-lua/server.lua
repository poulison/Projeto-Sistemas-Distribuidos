local zmq     = require "lzmq"
local mp      = require "MessagePack"
local sqlite3 = require "lsqlite3"
local socket  = require "socket"    -- para gettime() com precisão de microssegundos
local os      = require "os"

local PORT        = tonumber(os.getenv("PORT") or "5554")
local PROXY_HOST  = os.getenv("PROXY_HOST")  or "proxy"
local XSUB_PORT   = os.getenv("XSUB_PORT")   or "5557"
local REF_HOST    = os.getenv("REF_HOST")     or "reference"
local REF_PORT    = os.getenv("REF_PORT")     or "5559"
local SERVER_NAME = os.getenv("SERVER_NAME")  or "server-lua"
local DB_PATH     = "/data/server.db"

local db, pub_socket
local logical_clock = 0
local time_offset   = 0
local server_rank   = 0
local msg_count     = 0

local function tick_send()
    logical_clock = logical_clock + 1
    return logical_clock
end
local function tick_recv(r)
    r = tonumber(r) or 0
    if r > logical_clock then logical_clock = r end
end
-- relógio físico com precisão de microssegundos (ajustado pelo reference)
local function local_time() return socket.gettime() end
local function now_ts()     return socket.gettime() + time_offset end

local function init_db()
    os.execute("mkdir -p /data")
    db = sqlite3.open(DB_PATH)
    db:exec([[
        CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, timestamp REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, username TEXT NOT NULL, message TEXT NOT NULL, timestamp REAL NOT NULL, clock INTEGER NOT NULL DEFAULT 0);
    ]])
end

local function call_reference(type_str, extra)
    local ctx  = zmq.context()
    local sock = ctx:socket(zmq.REQ)
    sock:set_rcvtimeo(5000)
    sock:connect("tcp://" .. REF_HOST .. ":" .. REF_PORT)
    local payload = { type=type_str, name=SERVER_NAME, clock=tick_send(), timestamp=now_ts() }
    if extra then for k,v in pairs(extra) do payload[k]=v end end
    local ok, err = sock:send(mp.pack(payload))
    if not ok then sock:close(); return end
    local raw = sock:recv()
    if raw then
        local ok2, resp = pcall(mp.unpack, raw)
        if ok2 and resp then
            tick_recv(resp.clock or 0)
            if resp.rank and resp.rank > 0 then server_rank = resp.rank end
            if resp.time and resp.time > 1e9 then time_offset = resp.time - local_time() end
            if resp.timestamp and resp.timestamp > 1e9 and type_str=="register" then
                time_offset = resp.timestamp - local_time()
                print(string.format("[%s] Registered | rank=%d | offset=%.3fs", SERVER_NAME, server_rank, time_offset))
                io.flush()
            end
            if type_str=="heartbeat" then
                print(string.format("[%s] HEARTBEAT sent | rank=%d | clock=%d | offset=%.3fs", SERVER_NAME, server_rank, logical_clock, time_offset))
                io.flush()
            end
        end
    end
    sock:close()
end

local function make_ok(message, data)
    local t = { status="ok", message=message, clock=tick_send(), timestamp=now_ts() }
    if data then t.data = data end
    return mp.pack(t)
end
local function make_err(message)
    return mp.pack({ status="error", message=message, clock=tick_send(), timestamp=now_ts() })
end

local function handle_login(username, ts)
    if not username or username=="" then return make_err("Username cannot be empty") end
    local s=db:prepare("INSERT OR IGNORE INTO users (username,created_at) VALUES (?,?)")
    s:bind_values(username,now_ts()); s:step(); s:finalize()
    s=db:prepare("INSERT INTO logins (username,timestamp) VALUES (?,?)")
    s:bind_values(username,now_ts()); s:step(); s:finalize()
    return make_ok("Welcome, "..username.."!")
end

local function handle_create_channel(name, created_by, ts)
    if not name or name=="" then return make_err("Channel name cannot be empty") end
    local s=db:prepare("INSERT INTO channels (name,created_by,created_at) VALUES (?,?,?)")
    s:bind_values(name,created_by or "",now_ts())
    local rc=s:step(); s:finalize()
    if rc==sqlite3.CONSTRAINT then return make_err("Channel '"..name.."' already exists") end
    return make_ok("Channel '"..name.."' created!")
end

local function handle_list_channels()
    local channels={}
    for row in db:nrows("SELECT name FROM channels ORDER BY created_at") do
        channels[#channels+1]=row.name
    end
    return make_ok("OK",channels)
end

local function handle_publish(channel, username, message, ts, recv_clk)
    if not channel or channel=="" or not message or message=="" then
        return make_err("Channel and message are required")
    end
    local found=false
    local s_chk = db:prepare("SELECT name FROM channels WHERE name = ?")
    s_chk:bind_values(channel)
    if s_chk:step() == sqlite3.ROW then found = true end
    s_chk:finalize()
    if not found then return make_err("Channel '"..channel.."' does not exist") end

    local s=db:prepare("INSERT INTO messages (channel,username,message,timestamp,clock) VALUES (?,?,?,?,?)")
    s:bind_values(channel,username,message,now_ts(),recv_clk); s:step(); s:finalize()

    local pub_clk=tick_send()
    local payload=mp.pack({channel=channel,username=username,message=message,
                            timestamp=now_ts(),received=now_ts(),clock=pub_clk})
    pub_socket:send({channel,payload})
    io.write(string.format("[%s] PUB  | channel=%-15s | from=%-12s | clock=%d | %s\n",
        SERVER_NAME,channel,username,pub_clk,message))
    io.flush()
    return make_ok("Published!")
end

-- main
init_db()
os.execute("sleep 2")
call_reference("register", nil)

local ctx = zmq.context()
local rep_socket = ctx:socket(zmq.REP)
rep_socket:bind("tcp://*:"..PORT)
pub_socket = ctx:socket(zmq.PUB)
pub_socket:connect("tcp://"..PROXY_HOST..":"..XSUB_PORT)
os.execute("sleep 1")

print(string.format("[%s] Listening on port %d | rank=%d", SERVER_NAME, PORT, server_rank))
io.flush()

while true do
    local raw, err = rep_socket:recv()
    if not raw then
        print("[" .. SERVER_NAME .. "] Recv error: " .. tostring(err))
    else
        local ok_u, data = pcall(mp.unpack, raw)
        if not ok_u or type(data)~="table" then
            rep_socket:send(make_err("Invalid message format"))
        else
            local msg_type     = data.type         or ""
            local username     = data.username     or ""
            local channel_name = data.channel_name or ""
            local message      = data.message      or ""
            local ts           = data.timestamp    or now_ts()
            local recv_clk     = data.clock        or 0
            tick_recv(recv_clk)
            msg_count = msg_count + 1

            io.write(string.format("[%s] RECV | type=%-10s | from=%-12s | clock=%d | lc=%d\n",
                SERVER_NAME, msg_type, username, recv_clk, logical_clock))
            io.flush()

            local resp
            if     msg_type=="login"   then resp=handle_login(username,ts)
            elseif msg_type=="channel" then resp=handle_create_channel(channel_name,username,ts)
            elseif msg_type=="list"    then resp=handle_list_channels()
            elseif msg_type=="publish" then resp=handle_publish(channel_name,username,message,ts,recv_clk)
            else   resp=make_err("Unknown type: "..msg_type)
            end
            rep_socket:send(resp)

            if msg_count % 10 == 0 then
                call_reference("heartbeat", {msg_count=msg_count})
            end
        end
    end
end
