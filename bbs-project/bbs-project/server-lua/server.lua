local zmq     = require "lzmq"
local mp      = require "MessagePack"
local sqlite3 = require "lsqlite3"

local PORT        = tonumber(os.getenv("PORT") or "5554")
local S2S_PORT    = tonumber(os.getenv("S2S_PORT") or "5564")
local PROXY_HOST  = os.getenv("PROXY_HOST")  or "proxy"
local XSUB_PORT   = os.getenv("XSUB_PORT")   or "5557"
local XPUB_PORT   = os.getenv("XPUB_PORT")   or "5558"
local REF_HOST    = os.getenv("REF_HOST")     or "reference"
local REF_PORT    = os.getenv("REF_PORT")     or "5559"
local SERVER_NAME = os.getenv("SERVER_NAME")  or "server-lua"
local DB_PATH     = "/data/server.db"

local db
local logical_clock = 0
local time_offset   = 0.0
local server_rank   = 0
local coordinator   = ""
local known_servers = {}
local pub_socket
local ctx

local function now_ts() return os.time() + time_offset end
local function tick_send() logical_clock=logical_clock+1; return logical_clock end
local function tick_recv(r)
    r=tonumber(r) or 0; if r>logical_clock then logical_clock=r end
end

local function s2s_port_of(name)
    local m={["server-python"]=5560,["server-go"]=5561,
             ["server-csharp"]=5562,["server-c"]=5563,["server-lua"]=5564}
    return m[name] or 5560
end

local function init_db()
    os.execute("mkdir -p /data")
    db=sqlite3.open(DB_PATH)
    db:exec([[
        CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, timestamp REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, username TEXT NOT NULL, message TEXT NOT NULL, timestamp REAL NOT NULL, clock INTEGER NOT NULL DEFAULT 0);
    ]])
end

-- ── REQ-REP genérico com timeout ─────────────────────────────────────────────
local function req_call(addr, payload, timeout_ms)
    timeout_ms = timeout_ms or 5000
    local sock = ctx:socket(zmq.REQ)
    sock:set_rcvtimeo(timeout_ms)
    sock:connect(addr)
    local raw = mp.pack(payload)
    sock:send(raw)
    local resp_raw, err = sock:recv()
    sock:close()
    if not resp_raw then return nil end
    local ok, resp = pcall(mp.unpack, resp_raw)
    return ok and resp or nil
end

local function call_ref(type_str)
    local clk = tick_send()
    local addr = "tcp://" .. REF_HOST .. ":" .. REF_PORT
    local resp = req_call(addr, {type=type_str, name=SERVER_NAME, clock=clk, timestamp=now_ts()})
    if resp then tick_recv(resp.clock or 0) end
    return resp or {}
end

local function call_s2s(srv_name, type_str)
    local clk = tick_send()
    local port = s2s_port_of(srv_name)
    local addr = "tcp://" .. srv_name .. ":" .. port
    local resp = req_call(addr, {type=type_str, name=SERVER_NAME, rank=server_rank, clock=clk}, 3000)
    if resp then tick_recv(resp.clock or 0) end
    return resp
end

local function connect_to_reference()
    local resp = call_ref("register")
    server_rank = resp.rank or 0
    print(string.format("[%s] Registered | rank=%d", SERVER_NAME, server_rank)); io.flush()
end

local function get_server_list()
    local resp = call_ref("list")
    known_servers = resp.servers or {}
end

local function send_heartbeat()
    call_ref("heartbeat")
    print(string.format("[%s] HEARTBEAT sent | rank=%d | clock=%d", SERVER_NAME, server_rank, logical_clock)); io.flush()
end

local function sync_with_coordinator()
    if coordinator=="" or coordinator==SERVER_NAME then return end
    local resp = call_s2s(coordinator, "get_time")
    if not resp then
        print(string.format("[%s] Coordinator '%s' unreachable, election", SERVER_NAME, coordinator)); io.flush()
        return
    end
    local ref_time = resp.time or 0
    if ref_time > 0 then
        time_offset = ref_time - os.time()
        print(string.format("[%s] CLOCK SYNC | coord=%s | ref_time=%.3f | offset=%.6f",
            SERVER_NAME, coordinator, ref_time, time_offset)); io.flush()
    end
end

local function announce_coordinator()
    local clk = tick_send()
    local payload = mp.pack({coordinator=SERVER_NAME, clock=clk, timestamp=now_ts()})
    pub_socket:send({"servers", payload})
    print(string.format("[%s] ELECTED as coordinator | clock=%d", SERVER_NAME, clk)); io.flush()
end

local function start_election()
    print(string.format("[%s] Starting election | rank=%d", SERVER_NAME, server_rank)); io.flush()
    get_server_list()

    local candidates = {{name=SERVER_NAME, rank=server_rank}}
    for _, srv in ipairs(known_servers) do
        if srv.name ~= SERVER_NAME then
            local resp = call_s2s(srv.name, "election")
            if resp and resp.status == "ok" then
                table.insert(candidates, {name=srv.name, rank=resp.rank or 99})
            end
        end
    end

    -- menor rank ganha
    local winner = candidates[1]
    for _, c in ipairs(candidates) do
        if c.rank < winner.rank then winner = c end
    end

    coordinator = winner.name
    if winner.name == SERVER_NAME then announce_coordinator() end
    print(string.format("[%s] Election result: coordinator='%s'", SERVER_NAME, coordinator)); io.flush()
end

-- ── S2S server (socket REP separado) ─────────────────────────────────────────
local function make_ok(d) d.status="ok"; d.clock=tick_send(); d.timestamp=now_ts(); return mp.pack(d) end
local function make_err(m) return mp.pack({status="error",message=m,clock=tick_send(),timestamp=now_ts()}) end

local function make_resp_client(d) d.clock=tick_send(); d.timestamp=now_ts(); return d end

local function handle_login(username)
    if not username or username=="" then return make_resp_client({status="error",message="Username cannot be empty"}) end
    local s=db:prepare("INSERT OR IGNORE INTO users (username,created_at) VALUES(?,?)"); s:bind_values(username,now_ts()); s:step(); s:finalize()
    s=db:prepare("INSERT INTO logins (username,timestamp) VALUES(?,?)"); s:bind_values(username,now_ts()); s:step(); s:finalize()
    return make_resp_client({status="ok",message="Welcome, " .. username .. "!"})
end

local function handle_create_channel(name, by)
    if not name or name=="" then return make_resp_client({status="error",message="Channel name cannot be empty"}) end
    local s=db:prepare("INSERT INTO channels (name,created_by,created_at) VALUES(?,?,?)")
    s:bind_values(name,by or "",now_ts())
    local rc=s:step(); s:finalize()
    if rc==sqlite3.CONSTRAINT then return make_resp_client({status="error",message="Channel '" .. name .. "' already exists"}) end
    return make_resp_client({status="ok",message="Channel '" .. name .. "' created!"})
end

local function handle_list_channels()
    local channels={}
    for row in db:nrows("SELECT name FROM channels ORDER BY created_at") do channels[#channels+1]=row.name end
    return make_resp_client({status="ok",message="OK",data=channels})
end

local function handle_publish(channel, username, message, msg_clock)
    if not channel or channel=="" or not message or message=="" then
        return make_resp_client({status="error",message="Channel and message required"})
    end
    local found=false
    for _ in db:nrows("SELECT name FROM channels WHERE name='" .. channel .. "'") do found=true end
    if not found then return make_resp_client({status="error",message="Channel '" .. channel .. "' does not exist"}) end
    local s=db:prepare("INSERT INTO messages (channel,username,message,timestamp,clock) VALUES(?,?,?,?,?)")
    s:bind_values(channel,username,message,now_ts(),msg_clock); s:step(); s:finalize()
    local clk=tick_send()
    local payload=mp.pack({channel=channel,username=username,message=message,timestamp=now_ts(),received=now_ts(),clock=clk})
    pub_socket:send({channel,payload})
    print(string.format("[%s] PUB  | channel=%-15s | from=%-12s | clock=%d | msg=%s",
        SERVER_NAME,channel,username,clk,message)); io.flush()
    return make_resp_client({status="ok",message="Published!"})
end

-- ── main ─────────────────────────────────────────────────────────────────────
init_db()
ctx = zmq.context()

pub_socket = ctx:socket(zmq.PUB)
pub_socket:connect("tcp://" .. PROXY_HOST .. ":" .. XSUB_PORT)
os.execute("sleep 1")

os.execute("sleep 2")
connect_to_reference()
get_server_list()

-- socket S2S REP
local s2s_sock = ctx:socket(zmq.REP)
s2s_sock:bind("tcp://*:" .. S2S_PORT)
print(string.format("[%s] S2S listening on port %d", SERVER_NAME, S2S_PORT)); io.flush()

-- socket SUB 'servers'
local sub_servers = ctx:socket(zmq.SUB)
sub_servers:connect("tcp://" .. PROXY_HOST .. ":" .. XPUB_PORT)
sub_servers:subscribe("servers")
print(string.format("[%s] SUB listening on 'servers' topic", SERVER_NAME)); io.flush()

os.execute("sleep 1")
start_election()

-- socket REP clientes
local rep_socket = ctx:socket(zmq.REP)
rep_socket:bind("tcp://*:" .. PORT)
print(string.format("[%s] Listening on port %d | rank=%d", SERVER_NAME, PORT, server_rank)); io.flush()

-- poller para S2S + SUB + REP clientes
local poller = zmq.poller(3)

poller:add(s2s_sock, zmq.POLLIN, function()
    local raw = s2s_sock:recv()
    if not raw then return end
    local ok_u, data = pcall(mp.unpack, raw)
    if not ok_u then s2s_sock:send(make_err("parse error")); return end
    tick_recv(data.clock or 0)
    local msg_type = data.type or ""
    if msg_type == "get_time" then
        s2s_sock:send(make_ok({time=now_ts()}))
        print(string.format("[%s] S2S get_time | from=%s", SERVER_NAME, data.name or "?")); io.flush()
    elseif msg_type == "election" then
        s2s_sock:send(make_ok({rank=server_rank}))
        print(string.format("[%s] S2S election | from=%s | rank=%d", SERVER_NAME, data.name or "?", server_rank)); io.flush()
    else
        s2s_sock:send(make_err("Unknown S2S: " .. msg_type))
    end
end)

poller:add(sub_servers, zmq.POLLIN, function()
    local frames = sub_servers:recv_multipart()
    if not frames or #frames < 2 then return end
    local ok_u, data = pcall(mp.unpack, frames[2])
    if ok_u and data then
        tick_recv(data.clock or 0)
        if data.coordinator and data.coordinator ~= "" then
            coordinator = data.coordinator
            print(string.format("[%s] New coordinator: '%s'", SERVER_NAME, coordinator)); io.flush()
        end
    end
end)

local msg_count = 0
poller:add(rep_socket, zmq.POLLIN, function()
    local raw = rep_socket:recv()
    if not raw then return end
    local ok_u, data = pcall(mp.unpack, raw)
    if not ok_u then rep_socket:send(make_err("parse error")); return end
    tick_recv(data.clock or 0); msg_count=msg_count+1

    local msg_type     = data.type         or ""
    local username     = data.username     or ""
    local channel_name = data.channel_name or ""
    local message      = data.message      or ""
    local msg_clock    = data.clock        or 0

    print(string.format("[%s] RECV | type=%-10s | from=%-12s | clock=%d | lc=%d",
        SERVER_NAME, msg_type, username, msg_clock, logical_clock)); io.flush()

    local resp
    if     msg_type=="login"   then resp=handle_login(username)
    elseif msg_type=="channel" then resp=handle_create_channel(channel_name,username)
    elseif msg_type=="list"    then resp=handle_list_channels()
    elseif msg_type=="publish" then resp=handle_publish(channel_name,username,message,msg_clock)
    else   resp=make_resp_client({status="error",message="Unknown: " .. msg_type})
    end

    print(string.format("[%s] SEND | status=%-8s | clock=%d", SERVER_NAME, resp.status, resp.clock)); io.flush()
    rep_socket:send(mp.pack(resp))

    if msg_count % 15 == 0 then
        send_heartbeat()
        sync_with_coordinator()
    end
end)

while true do
    poller:poll(100)
end
