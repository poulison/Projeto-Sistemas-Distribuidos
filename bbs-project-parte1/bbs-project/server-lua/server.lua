local zmq     = require "lzmq"
local mp      = require "MessagePack"
local sqlite3 = require "lsqlite3"
local os      = require "os"

local PORT    = tonumber(os.getenv("PORT") or "5554")
local DB_PATH = "/data/server.db"

--  banco de dados 

local db

local function init_db()
    os.execute("mkdir -p /data")
    db = sqlite3.open(DB_PATH)
    db:exec([[
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS logins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL, timestamp REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS channels (
            name TEXT PRIMARY KEY,
            created_by TEXT NOT NULL, created_at REAL NOT NULL);
    ]])
end

--  helpers

local function now_ts()
    -- lua 5.3+ tem math.type, usamos socket.gettime se disponível
    return os.time() + 0.0
end

local function make_ok(message, data)
    local t = { status = "ok", message = message, timestamp = now_ts() }
    if data then t.data = data end
    return mp.pack(t)
end

local function make_err(message)
    return mp.pack({ status = "error", message = message, timestamp = now_ts() })
end

--  handlers 

local function handle_login(username, ts)
    if not username or username == "" then
        return make_err("Username cannot be empty")
    end

    local stmt = db:prepare("INSERT OR IGNORE INTO users (username, created_at) VALUES (?, ?)")
    stmt:bind_values(username, ts)
    stmt:step()
    stmt:finalize()

    stmt = db:prepare("INSERT INTO logins (username, timestamp) VALUES (?, ?)")
    stmt:bind_values(username, ts)
    stmt:step()
    stmt:finalize()

    return make_ok("Welcome, " .. username .. "!")
end

local function handle_create_channel(name, created_by, ts)
    if not name or name == "" then
        return make_err("Channel name cannot be empty")
    end
    if #name > 32 then
        return make_err("Channel name too long (max 32 chars)")
    end

    local stmt = db:prepare(
        "INSERT INTO channels (name, created_by, created_at) VALUES (?, ?, ?)")
    stmt:bind_values(name, created_by or "", ts)
    local rc = stmt:step()
    stmt:finalize()

    if rc == sqlite3.CONSTRAINT then
        return make_err("Channel '" .. name .. "' already exists")
    end
    return make_ok("Channel '" .. name .. "' created!")
end

local function handle_list_channels()
    local channels = {}
    for row in db:nrows("SELECT name FROM channels ORDER BY created_at") do
        channels[#channels + 1] = row.name
    end
    return make_ok("OK", channels)
end

-- main 

init_db()

local ctx    = zmq.context()
local socket = ctx:socket(zmq.REP)
socket:bind("tcp://*:" .. PORT)

print(string.format("[SERVER-LUA] Listening on port %d", PORT))
io.flush()

while true do
    local raw, err = socket:recv()
    if not raw then
        print("[SERVER-LUA] Recv error: " .. tostring(err))
    else
        local ok_unpack, data = pcall(mp.unpack, raw)
        if not ok_unpack or type(data) ~= "table" then
            socket:send(make_err("Invalid message format"))
        else
            local msg_type    = data.type        or ""
            local username    = data.username    or ""
            local channel_name = data.channel_name or ""
            local ts          = data.timestamp   or now_ts()

            io.write(string.format("[SERVER-LUA] RECV | type=%-10s | from=%-15s | ts=%.3f\n",
                msg_type, username, ts))
            io.flush()

            local resp
            if     msg_type == "login"   then resp = handle_login(username, ts)
            elseif msg_type == "channel" then resp = handle_create_channel(channel_name, username, ts)
            elseif msg_type == "list"    then resp = handle_list_channels()
            else
                resp = make_err("Unknown type: " .. msg_type)
            end

            print("[SERVER-LUA] SEND | " .. #resp .. " bytes")
            io.flush()
            socket:send(resp)
        end
    end
end
