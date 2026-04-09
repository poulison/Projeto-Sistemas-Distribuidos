local zmq = require "lzmq"
local mp  = require "MessagePack"
local os  = require "os"

local BOT_NAME    = os.getenv("BOT_NAME")    or "bot-lua-1"
local SERVER_HOST = os.getenv("SERVER_HOST") or "server-lua"
local SERVER_PORT = os.getenv("SERVER_PORT") or "5554"
local PROXY_HOST  = os.getenv("PROXY_HOST")  or "proxy"
local XPUB_PORT   = os.getenv("XPUB_PORT")   or "5558"

local words = {"ola","mundo","sistema","distribuido","mensagem","canal",
               "teste","lua","zmq","pubsub","broker","topico","servidor","rede"}

local function now_ts() return os.time() + 0.0 end
local function sleep(s) os.execute("sleep " .. s) end

local function random_msg()
    local n = 3 + math.random(5)
    local parts = {}
    for i = 1, n do parts[i] = words[math.random(#words)] end
    return table.concat(parts, " ")
end

local req_socket

local function send_recv(payload)
    payload.timestamp = now_ts()
    local raw = mp.pack(payload)
    io.write(string.format("[%s] SEND | type=%-10s | ts=%.3f\n", BOT_NAME, payload.type or "?", payload.timestamp))
    io.flush()
    req_socket:send(raw)
    local resp_raw = req_socket:recv()
    local ok_u, resp = pcall(mp.unpack, resp_raw)
    if not ok_u then return {} end
    io.write(string.format("[%s] RECV | status=%-8s | msg=%s\n", BOT_NAME, resp.status or "?", resp.message or "?"))
    io.flush()
    return resp
end

-- thread SUB via coroutine + socket não bloqueante
local function start_subscriber(channels)
    local ctx = zmq.context()
    local sub = ctx:socket(zmq.SUB)
    sub:connect("tcp://" .. PROXY_HOST .. ":" .. XPUB_PORT)
    sleep(1)
    for _, ch in ipairs(channels) do
        sub:subscribe(ch)
        print("[" .. BOT_NAME .. "] SUB  | subscribed to '" .. ch .. "'")
        io.flush()
    end

    -- poll non-blocking em loop separado via zmq poller
    local poller = zmq.poller(1)
    poller:add(sub, zmq.POLLIN, function()
        local frames, err = sub:recv_multipart()
        if frames and #frames >= 2 then
            local ok_u, p = pcall(mp.unpack, frames[2])
            if ok_u and p then
                io.write(string.format("[%s] MSG  | channel=%-12s | from=%-15s | sent=%.3f | recv=%.3f | %s\n",
                    BOT_NAME, p.channel or "?", p.username or "?",
                    p.timestamp or 0, now_ts(), p.message or "?"))
                io.flush()
            end
        end
    end)
    return poller
end

-- main
math.randomseed(os.time() + os.clock() * 1000)
sleep(3)

local ctx = zmq.context()
req_socket = ctx:socket(zmq.REQ)
req_socket:connect("tcp://" .. SERVER_HOST .. ":" .. SERVER_PORT)
print("[" .. BOT_NAME .. "] Connected to " .. SERVER_HOST .. ":" .. SERVER_PORT)
io.flush()

-- login
while true do
    local resp = send_recv({ type="login", username=BOT_NAME })
    if resp.status == "ok" then print("[" .. BOT_NAME .. "] Login successful!"); io.flush(); break end
    sleep(2)
end

-- lista canais
local resp = send_recv({ type="list", username=BOT_NAME })
local channels = resp.data or {}
io.write("[" .. BOT_NAME .. "] Channels: [")
for i,v in ipairs(channels) do io.write(v .. (i<#channels and ", " or "")) end
io.write("]\n"); io.flush()

-- cria canal se < 5
if #channels < 5 then
    local new_ch = "ch-lua-" .. math.random(100, 999)
    send_recv({ type="channel", username=BOT_NAME, channel_name=new_ch })
    resp = send_recv({ type="list", username=BOT_NAME })
    channels = resp.data or channels
end

-- inscreve em até 3 canais
local sub_chs = {}
for i = 1, math.min(3, #channels) do sub_chs[i] = channels[i] end

local poller = start_subscriber(sub_chs)
sleep(1)

-- loop infinito com poll intercalado
print("[" .. BOT_NAME .. "] Starting publish loop"); io.flush()
while true do
    local ch = channels[math.random(#channels)]
    for i = 1, 10 do
        send_recv({ type="publish", username=BOT_NAME, channel_name=ch, message=random_msg() })
        -- poll para receber msgs enquanto espera
        poller:poll(900)
    end
    resp = send_recv({ type="list", username=BOT_NAME })
    channels = resp.data or channels
end
