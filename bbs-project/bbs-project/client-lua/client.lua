local zmq    = require "lzmq"
local mp     = require "MessagePack"
local socket = require "socket"
local os     = require "os"

local BOT_NAME    = os.getenv("BOT_NAME")    or "bot-lua-1"
local SERVER_HOST = os.getenv("SERVER_HOST") or "server-lua"
local SERVER_PORT = os.getenv("SERVER_PORT") or "5554"
local PROXY_HOST  = os.getenv("PROXY_HOST")  or "proxy"
local XPUB_PORT   = os.getenv("XPUB_PORT")   or "5558"

local words = {"ola","mundo","sistema","distribuido","mensagem","canal",
               "teste","lua","zmq","pubsub","broker","topico","servidor","rede"}

local logical_clock = 0
local function tick_send() logical_clock=logical_clock+1; return logical_clock end
local function tick_recv(r) r=tonumber(r) or 0; if r>logical_clock then logical_clock=r end end
local function now_ts() return socket.gettime() end
local function sleep(s) os.execute("sleep "..s) end

local function random_msg()
    local n=3+math.random(5); local p={}
    for i=1,n do p[i]=words[math.random(#words)] end
    return table.concat(p," ")
end

local req_socket

local function send_recv(payload)
    payload.timestamp = now_ts()
    payload.clock     = tick_send()
    local raw = mp.pack(payload)
    io.write(string.format("[%s] SEND | type=%-10s | clock=%d | ts=%.3f\n",
        BOT_NAME, payload.type or "?", payload.clock, payload.timestamp))
    io.flush()
    req_socket:send(raw)
    local resp_raw = req_socket:recv()
    local ok_u, resp = pcall(mp.unpack, resp_raw)
    if not ok_u then return {} end
    tick_recv(resp.clock or 0)
    io.write(string.format("[%s] RECV | status=%-8s | clock=%d | msg=%s\n",
        BOT_NAME, resp.status or "?", resp.clock or 0, resp.message or "?"))
    io.flush()
    return resp
end

local function start_subscriber(channels)
    local ctx = zmq.context()
    local sub = ctx:socket(zmq.SUB)
    sub:connect("tcp://"..PROXY_HOST..":"..XPUB_PORT)
    sleep(1)
    for _,ch in ipairs(channels) do
        sub:subscribe(ch)
        print("["..BOT_NAME.."] SUB  | subscribed to '"..ch.."'"); io.flush()
    end
    local poller = zmq.poller(1)
    poller:add(sub, zmq.POLLIN, function()
        local frames = sub:recv_multipart()
        if frames and #frames>=2 then
            local ok_u,p=pcall(mp.unpack,frames[2])
            if ok_u and p then
                tick_recv(p.clock or 0)
                io.write(string.format("[%s] MSG  | channel=%-12s | from=%-12s | clock=%d | sent=%.3f | recv=%.3f | %s\n",
                    BOT_NAME, p.channel or "?", p.username or "?",
                    p.clock or 0, p.timestamp or 0, now_ts(), p.message or "?"))
                io.flush()
            end
        end
    end)
    return poller
end

math.randomseed(os.time()+os.clock()*1000)
sleep(4)

local ctx=zmq.context()
req_socket=ctx:socket(zmq.REQ)
req_socket:connect("tcp://"..SERVER_HOST..":"..SERVER_PORT)
print("["..BOT_NAME.."] Connected to "..SERVER_HOST..":"..SERVER_PORT); io.flush()

while true do
    local resp=send_recv({type="login",username=BOT_NAME})
    if resp.status=="ok" then
        print("["..BOT_NAME.."] Login successful! | server rank="..(resp.rank or "?"))
        io.flush(); break
    end
    sleep(2)
end

local resp=send_recv({type="list",username=BOT_NAME})
local channels=resp.data or {}
if #channels<5 then
    local nc="ch-lua-"..math.random(100,999)
    send_recv({type="channel",username=BOT_NAME,channel_name=nc})
    resp=send_recv({type="list",username=BOT_NAME})
    channels=resp.data or channels
end

local sub_chs={}
for i=1,math.min(3,#channels) do sub_chs[i]=channels[i] end

local poller=start_subscriber(sub_chs)
sleep(1)

print("["..BOT_NAME.."] Starting publish loop"); io.flush()
while true do
    local ch=channels[math.random(#channels)]
    for i=1,10 do
        send_recv({type="publish",username=BOT_NAME,channel_name=ch,message=random_msg()})
        poller:poll(900)
    end
    resp=send_recv({type="list",username=BOT_NAME})
    channels=resp.data or channels
end
