using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using MessagePack;
using Microsoft.Data.Sqlite;
using NetMQ;
using NetMQ.Sockets;

[MessagePackObject] public class InMsg {
    [Key("type")]         public string Type        { get; set; } = "";
    [Key("username")]     public string Username    { get; set; } = "";
    [Key("channel_name")] public string ChannelName { get; set; } = "";
    [Key("message")]      public string Message     { get; set; } = "";
    [Key("timestamp")]    public double Timestamp   { get; set; }
    [Key("clock")]        public long   Clock       { get; set; }
    [Key("name")]         public string Name        { get; set; } = "";
    [Key("rank")]         public int    Rank        { get; set; }
}
[MessagePackObject] public class OutMsg {
    [Key("status")]      public string        Status      { get; set; } = "";
    [Key("message")]     public string        Message     { get; set; } = "";
    [Key("data")]        public List<string>? Data        { get; set; }
    [Key("timestamp")]   public double        Timestamp   { get; set; }
    [Key("clock")]       public long          Clock       { get; set; }
    [Key("rank")]        public int           Rank        { get; set; }
    [Key("time")]        public double        Time        { get; set; }
    [Key("coordinator")] public string?       Coordinator { get; set; }
    [Key("servers")]     public List<Dictionary<string,object>>? Servers { get; set; }
}
[MessagePackObject] public class PubPayload {
    [Key("channel")]   public string Channel   { get; set; } = "";
    [Key("username")]  public string Username  { get; set; } = "";
    [Key("message")]   public string Message   { get; set; } = "";
    [Key("timestamp")] public double Timestamp { get; set; }
    [Key("received")]  public double Received  { get; set; }
    [Key("clock")]     public long   Clock     { get; set; }
}

class Server {
    static long   _lc = 0;
    static object _lcLock = new();
    static double _offset = 0;
    static object _offLock = new();
    static int    _rank = 0;
    static string _serverName = "server-csharp";
    static string _coordinator = "";
    static object _coordLock = new();
    static List<Dictionary<string,object>> _knownServers = new();
    static object _srvLock = new();
    static PublisherSocket? _pub;
    static readonly MessagePackSerializerOptions opts = MessagePackSerializerOptions.Standard;

    static string refHost="reference", refPort="5559", proxyHost="proxy", xsubPort="5557", xpubPort="5558", s2sPort="5562";

    static double NowTS() { lock(_offLock) { return (double)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()/1000.0 + _offset; } }
    static long TickSend() { lock(_lcLock) { _lc++; return _lc; } }
    static void TickRecv(long r) { lock(_lcLock) { if(r>_lc) _lc=r; } }

    static string S2SPortOf(string name) => name switch {
        "server-python" => "5560", "server-go" => "5561",
        "server-csharp" => "5562", "server-c" => "5563", "server-lua" => "5564", _ => "5560" };

    static SqliteConnection? db;
    static void InitDB() {
        Directory.CreateDirectory("/data");
        db = new SqliteConnection("Data Source=/data/server.db"); db.Open();
        new SqliteCommand(@"CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, created_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, timestamp REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, username TEXT NOT NULL, message TEXT NOT NULL, timestamp REAL NOT NULL, clock INTEGER NOT NULL DEFAULT 0);", db).ExecuteNonQuery();
    }

    static OutMsg ReqCall(string addr, object payload, int timeoutMs=5000) {
        try {
            using var sock = new RequestSocket();
            sock.Connect(addr);
            sock.SendFrame(MessagePackSerializer.Serialize(payload, opts));
            byte[]? raw;
            if (!sock.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(timeoutMs), out raw) || raw == null)
                return new OutMsg();
            return MessagePackSerializer.Deserialize<OutMsg>(raw, opts);
        } catch { return new OutMsg(); }
    }
    static OutMsg CallRef(object payload) => ReqCall($"tcp://{refHost}:{refPort}", payload);
    static OutMsg CallS2S(string name, object payload) => ReqCall($"tcp://{name}:{S2SPortOf(name)}", payload, 3000);

    static void ConnectToReference() {
        var r = CallRef(new { type="register", name=_serverName, clock=TickSend(), timestamp=NowTS() });
        TickRecv(r.Clock); _rank = r.Rank;
        Console.WriteLine($"[{_serverName}] Registered | rank={_rank}");
    }
    static void GetServerList() {
        var r = CallRef(new { type="list", name=_serverName, clock=TickSend(), timestamp=NowTS() });
        TickRecv(r.Clock);
        lock(_srvLock) { _knownServers = r.Servers ?? new(); }
    }
    static void SendHeartbeat() {
        var r = CallRef(new { type="heartbeat", name=_serverName, clock=TickSend(), timestamp=NowTS() });
        TickRecv(r.Clock);
        Console.WriteLine($"[{_serverName}] HEARTBEAT sent | rank={_rank} | clock={_lc}");
    }
    static void SyncWithCoordinator() {
        string coord; lock(_coordLock) { coord = _coordinator; }
        if (string.IsNullOrEmpty(coord) || coord == _serverName) return;
        var r = CallS2S(coord, new { type="get_time", name=_serverName, clock=TickSend(), timestamp=NowTS() });
        if (r.Status != "ok") { Console.WriteLine($"[{_serverName}] Coordinator '{coord}' unreachable, election"); new Thread(StartElection){IsBackground=true}.Start(); return; }
        TickRecv(r.Clock);
        if (r.Time > 0) {
            var newOff = r.Time - (double)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()/1000.0;
            lock(_offLock) { _offset = newOff; }
            Console.WriteLine($"[{_serverName}] CLOCK SYNC | coord={coord} | ref_time={r.Time:F3} | offset={newOff:F6}");
        }
    }
    static void StartElection() {
        Console.WriteLine($"[{_serverName}] Starting election | rank={_rank}");
        GetServerList();
        List<Dictionary<string,object>> others;
        lock(_srvLock) { others = _knownServers.FindAll(s => s.ContainsKey("name") && s["name"].ToString() != _serverName); }

        var candidates = new List<(string name, int rank)> { (_serverName, _rank) };
        foreach (var srv in others) {
            string name = srv["name"].ToString()!;
            var r = CallS2S(name, new { type="election", name=_serverName, rank=_rank, clock=TickSend() });
            if (r.Status == "ok") candidates.Add((name, r.Rank));
        }
        var winner = candidates[0];
        foreach (var c in candidates) { if (c.rank < winner.rank) winner = c; }
        lock(_coordLock) { _coordinator = winner.name; }
        if (winner.name == _serverName) AnnounceCoordinator();
        Console.WriteLine($"[{_serverName}] Election result: coordinator='{winner.name}'");
    }
    static void AnnounceCoordinator() {
        var clk = TickSend();
        var payload = MessagePackSerializer.Serialize(new { coordinator=_serverName, clock=clk, timestamp=NowTS() }, opts);
        _pub!.SendMoreFrame("servers").SendFrame(payload);
        Console.WriteLine($"[{_serverName}] ELECTED as coordinator | clock={clk}");
    }
    static void S2SServerThread() {
        using var sock = new ResponseSocket();
        sock.Bind($"tcp://*:{s2sPort}");
        Console.WriteLine($"[{_serverName}] S2S listening on port {s2sPort}");
        while (true) {
            var raw = sock.ReceiveFrameBytes();
            var msg = MessagePackSerializer.Deserialize<InMsg>(raw, opts);
            TickRecv(msg.Clock);
            OutMsg resp;
            if (msg.Type == "get_time") {
                resp = new OutMsg { Status="ok", Time=NowTS(), Clock=TickSend(), Timestamp=NowTS() };
                Console.WriteLine($"[{_serverName}] S2S get_time | from={msg.Name} | time={resp.Time:F3}");
            } else if (msg.Type == "election") {
                resp = new OutMsg { Status="ok", Rank=_rank, Clock=TickSend(), Timestamp=NowTS() };
                Console.WriteLine($"[{_serverName}] S2S election | from={msg.Name} | rank={_rank}");
            } else {
                resp = new OutMsg { Status="error", Message="Unknown S2S", Clock=TickSend() };
            }
            sock.SendFrame(MessagePackSerializer.Serialize(resp, opts));
        }
    }
    static void ServersSubThread() {
        using var sub = new SubscriberSocket();
        sub.Connect($"tcp://{proxyHost}:{xpubPort}");
        sub.Subscribe("servers");
        Console.WriteLine($"[{_serverName}] SUB listening on 'servers' topic");
        while (true) {
            sub.ReceiveFrameBytes();
            var raw = sub.ReceiveFrameBytes();
            var data = MessagePackSerializer.Deserialize<OutMsg>(raw, opts);
            TickRecv(data.Clock);
            if (!string.IsNullOrEmpty(data.Coordinator)) {
                lock(_coordLock) { _coordinator = data.Coordinator; }
                Console.WriteLine($"[{_serverName}] New coordinator: '{data.Coordinator}'");
            }
        }
    }

    static OutMsg MakeResp(string status, string msg, List<string>? data=null) =>
        new OutMsg { Status=status, Message=msg, Data=data, Clock=TickSend(), Timestamp=NowTS() };

    static OutMsg HandleLogin(InMsg msg) {
        if (string.IsNullOrWhiteSpace(msg.Username)) return MakeResp("error","Username cannot be empty");
        var c1=new SqliteCommand("INSERT OR IGNORE INTO users (username,created_at) VALUES(@u,@t)",db); c1.Parameters.AddWithValue("@u",msg.Username); c1.Parameters.AddWithValue("@t",NowTS()); c1.ExecuteNonQuery();
        var c2=new SqliteCommand("INSERT INTO logins (username,timestamp) VALUES(@u,@t)",db); c2.Parameters.AddWithValue("@u",msg.Username); c2.Parameters.AddWithValue("@t",NowTS()); c2.ExecuteNonQuery();
        var r=MakeResp("ok",$"Welcome, {msg.Username}!"); r.Rank=_rank; return r;
    }
    static OutMsg HandleCreateChannel(InMsg msg) {
        if (string.IsNullOrWhiteSpace(msg.ChannelName)) return MakeResp("error","Channel name cannot be empty");
        try { var c=new SqliteCommand("INSERT INTO channels (name,created_by,created_at) VALUES(@n,@u,@t)",db); c.Parameters.AddWithValue("@n",msg.ChannelName); c.Parameters.AddWithValue("@u",msg.Username); c.Parameters.AddWithValue("@t",NowTS()); c.ExecuteNonQuery(); return MakeResp("ok",$"Channel '{msg.ChannelName}' created!"); }
        catch (SqliteException) { return MakeResp("error",$"Channel '{msg.ChannelName}' already exists"); }
    }
    static OutMsg HandleListChannels() {
        var r=new SqliteCommand("SELECT name FROM channels ORDER BY created_at",db).ExecuteReader();
        var list=new List<string>(); while(r.Read()) list.Add(r.GetString(0));
        return MakeResp("ok","OK",list);
    }
    static OutMsg HandlePublish(InMsg msg) {
        if (string.IsNullOrWhiteSpace(msg.ChannelName)||string.IsNullOrWhiteSpace(msg.Message)) return MakeResp("error","Channel and message required");
        var chk=new SqliteCommand("SELECT name FROM channels WHERE name=@n",db); chk.Parameters.AddWithValue("@n",msg.ChannelName);
        if (chk.ExecuteScalar()==null) return MakeResp("error",$"Channel '{msg.ChannelName}' does not exist");
        var ins=new SqliteCommand("INSERT INTO messages (channel,username,message,timestamp,clock) VALUES(@c,@u,@m,@t,@lc)",db);
        ins.Parameters.AddWithValue("@c",msg.ChannelName); ins.Parameters.AddWithValue("@u",msg.Username);
        ins.Parameters.AddWithValue("@m",msg.Message); ins.Parameters.AddWithValue("@t",NowTS()); ins.Parameters.AddWithValue("@lc",msg.Clock); ins.ExecuteNonQuery();
        var clk=TickSend();
        var payload=MessagePackSerializer.Serialize(new PubPayload{Channel=msg.ChannelName,Username=msg.Username,Message=msg.Message,Timestamp=NowTS(),Received=NowTS(),Clock=clk},opts);
        _pub!.SendMoreFrame(msg.ChannelName).SendFrame(payload);
        Console.WriteLine($"[{_serverName}] PUB  | channel={msg.ChannelName,-15} | from={msg.Username,-12} | clock={clk}");
        return MakeResp("ok","Published!");
    }

    static void Main() {
        string port = Environment.GetEnvironmentVariable("PORT") ?? "5552";
        s2sPort     = Environment.GetEnvironmentVariable("S2S_PORT") ?? "5562";
        proxyHost   = Environment.GetEnvironmentVariable("PROXY_HOST") ?? "proxy";
        xsubPort    = Environment.GetEnvironmentVariable("XSUB_PORT") ?? "5557";
        xpubPort    = Environment.GetEnvironmentVariable("XPUB_PORT") ?? "5558";
        refHost     = Environment.GetEnvironmentVariable("REF_HOST") ?? "reference";
        refPort     = Environment.GetEnvironmentVariable("REF_PORT") ?? "5559";
        _serverName = Environment.GetEnvironmentVariable("SERVER_NAME") ?? "server-csharp";
        InitDB();

        _pub = new PublisherSocket(); _pub.Connect($"tcp://{proxyHost}:{xsubPort}");
        Thread.Sleep(1000);
        Thread.Sleep(2000);
        ConnectToReference(); GetServerList();

        new Thread(S2SServerThread){IsBackground=true}.Start();
        new Thread(ServersSubThread){IsBackground=true}.Start();
        Thread.Sleep(1000);
        new Thread(StartElection){IsBackground=true}.Start();

        using var server = new ResponseSocket(); server.Bind($"tcp://*:{port}");
        Console.WriteLine($"[{_serverName}] Listening on port {port} | rank={_rank}");

        long msgCount=0;
        while (true) {
            var raw=server.ReceiveFrameBytes();
            var msg=MessagePackSerializer.Deserialize<InMsg>(raw,opts);
            TickRecv(msg.Clock); msgCount++;
            Console.WriteLine($"[{_serverName}] RECV | type={msg.Type,-10} | from={msg.Username,-12} | clock={msg.Clock} | lc={_lc}");
            OutMsg resp = msg.Type switch {
                "login"   => HandleLogin(msg),
                "channel" => HandleCreateChannel(msg),
                "list"    => HandleListChannels(),
                "publish" => HandlePublish(msg),
                _         => MakeResp("error",$"Unknown: {msg.Type}")
            };
            Console.WriteLine($"[{_serverName}] SEND | status={resp.Status,-8} | clock={resp.Clock}");
            server.SendFrame(MessagePackSerializer.Serialize(resp,opts));
            if (msgCount%15==0) { new Thread(SendHeartbeat){IsBackground=true}.Start(); new Thread(SyncWithCoordinator){IsBackground=true}.Start(); }
        }
    }
}
