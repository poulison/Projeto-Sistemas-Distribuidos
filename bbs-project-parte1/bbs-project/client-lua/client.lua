local zmq = require "lzmq"
local mp  = require "MessagePack"
local os  = require "os"

local BOT_NAME    = os.getenv("BOT_NAME")    or "bot-lua-1"
local SERVER_HOST = os.getenv("SERVER_HOST") or "server-lua"
local SERVER_PORT = os.getenv("SERVER_PORT") or "5554"

local channels = { "geral", "random", "noticias", "projetos", "lua-talk" }

-- helpers 

local function now_ts()
    return os.time() + 0.0
end

local function sleep(s)
    os.execute("sleep " .. s)
end

--  comunicação 

local socket

local function send_recv(payload)
    payload.timestamp = now_ts()
    local raw = mp.pack(payload)

    io.write(string.format("[%s] SEND | type=%-10s | ts=%.3f\n",
        BOT_NAME, payload.type or "?", payload.timestamp))
    io.flush()

    socket:send(raw)

    local resp_raw = socket:recv()
    local ok_unpack, resp = pcall(mp.unpack, resp_raw)
    if not ok_unpack then
        print("[" .. BOT_NAME .. "] ERROR: failed to unpack response")
        return {}
    end

    io.write(string.format("[%s] RECV | status=%-8s | msg=%s\n",
        BOT_NAME, resp.status or "?", resp.message or "?"))
    io.flush()
    return resp
end

local function login()
    while true do
        local resp = send_recv({ type = "login", username = BOT_NAME })
        if resp.status == "ok" then
            print("[" .. BOT_NAME .. "] ✔ Login successful!")
            io.flush()
            return
        end
        print("[" .. BOT_NAME .. "] ✘ Login failed: " .. (resp.message or "?") .. " — retrying...")
        io.flush()
        sleep(2)
    end
end

local function create_channel(name)
    send_recv({ type = "channel", username = BOT_NAME, channel_name = name })
end

local function list_channels()
    local resp = send_recv({ type = "list", username = BOT_NAME })
    if resp.status == "ok" and resp.data then
        io.write("[" .. BOT_NAME .. "] Channels available: [")
        for i, ch in ipairs(resp.data) do
            io.write(ch .. (i < #resp.data and ", " or ""))
        end
        io.write("]\n")
        io.flush()
    end
end

-- main 

sleep(3)

local ctx = zmq.context()
socket    = ctx:socket(zmq.REQ)
socket:connect("tcp://" .. SERVER_HOST .. ":" .. SERVER_PORT)

print("[" .. BOT_NAME .. "] Connected to " .. SERVER_HOST .. ":" .. SERVER_PORT)
io.flush()

login()
sleep(1)

list_channels()
sleep(1)

for _, ch in ipairs(channels) do
    create_channel(ch)
    sleep(0)
end

list_channels()
print("[" .. BOT_NAME .. "] ✔ Part 1 done!")
io.flush()
