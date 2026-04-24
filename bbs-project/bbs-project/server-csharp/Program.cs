using System;
using System.Collections.Generic;
using System.IO;
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
}
[MessagePackObject] public class OutMsg {
    [Key("status")]    public string        Status    { get; set; } = "";
    [Key("message")]   public string        Message   { get; set; } = "";
    [Key("data")]      public List<string>? Data      { get; set; }
    [Key("timestamp")] public double        Timestamp { get; set; }
    [Key("clock")]     public long          Clock     { get; set; }
    [Key("rank")]      public int           Rank      { get; set; }
}
[MessagePackObject] public class PubPayload {
    [Key("channel")]   public string Channel   { get; set; } = "";
    [Key("username")]  public string Username  { get; set; } = "";
    [Key("message")]   public string Message   { get; set; } = "";
    [Key("timestamp")] public double Timestamp { get; set; }
    [Key("received")]  public double Received  { get; set; }
    [Key("clock")]     public long   Clock     { get; set; }
}
[MessagePackObject] public class RefResp {
    [Key("status")]    public string Status    { get; set; } = "";
    [Key("rank")]      public int    Rank      { get; set; }
    [Key("time")]      public double Time      { get; set; }
    [Key("clock")]     public long   Clock     { get; set; }
    [Key("timestamp")] public double Timestamp { get; set; }
}

class Server {
    static SqliteConnection? db;
    static readonly MessagePackSerializerOptions opts = MessagePackSerializerOptions.Standard;
    static long   logicClock = 0;
    static object clockLock  = new object();
    static double timeOffset = 0;
    static int    serverRank = 0;
    static string serverName = Environment.GetEnvironmentVariable("SERVER_NAME") ?? "server-csharp";
    static string refHost    = Environment.GetEnvironmentVariable("REF_HOST")    ?? "reference";
    static string refPort    = Environment.GetEnvironmentVariable("REF_PORT")    ?? "5559";

    static long TickSend() { lock(clockLock) { return ++logicClock; } }
    static void TickRecv(long r) { lock(clockLock) { if (r > logicClock) logicClock = r; } }
    static double NowTS() => (double)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()/1000.0 + timeOffset;

    static void InitDB() {
        Directory.CreateDirectory("/data");
        db = new SqliteConnection("Data Source=/data/server.db");
        db.Open();
        new SqliteCommand(@"
            CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, created_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, timestamp REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, username TEXT NOT NULL, message TEXT NOT NULL, timestamp REAL NOT NULL, clock INTEGER NOT NULL DEFAULT 0);
        ", db).ExecuteNonQuery();
    }

    static RefResp CallReference(Dictionary<string,object> payload) {
        using var sock = new RequestSocket();
        sock.Connect($"tcp://{refHost}:{refPort}");
        sock.SendFrame(MessagePackSerializer.Serialize(payload, opts));
        byte[] raw = sock.ReceiveFrameBytes();
        return MessagePackSerializer.Deserialize<RefResp>(raw, opts);
    }

    static void ConnectToReference() {
        try {
            var r = CallReference(new() { ["type"]="register", ["name"]=serverName, ["clock"]=TickSend(), ["timestamp"]=NowTS() });
            TickRecv(r.Clock);
            serverRank = r.Rank;
            timeOffset = r.Timestamp - (double)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()/1000.0;
            Console.WriteLine($"[{serverName}] Registered | rank={serverRank} | offset={timeOffset:F3}s");
        } catch(Exception e) { Console.WriteLine($"[{serverName}] Reference error: {e.Message}"); }
    }

    static void SendHeartbeat(long msgCount) {
        try {
            var r = CallReference(new() { ["type"]="heartbeat", ["name"]=serverName, ["clock"]=TickSend(), ["timestamp"]=NowTS(), ["msg_count"]=msgCount });
            TickRecv(r.Clock);
            if (r.Time > 0) timeOffset = r.Time - (double)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()/1000.0;
            Console.WriteLine($"[{serverName}] HEARTBEAT sent | rank={serverRank} | clock={logicClock} | offset={timeOffset:F3}s");
        } catch(Exception e) { Console.WriteLine($"[{serverName}] Heartbeat error: {e.Message}"); }
    }

    static OutMsg MakeResp(string status, string message) =>
        new() { Status=status, Message=message, Clock=TickSend(), Timestamp=NowTS() };

    static OutMsg HandleLogin(InMsg msg) {
        if (string.IsNullOrWhiteSpace(msg.Username)) return MakeResp("error","Username cannot be empty");
        var c1 = new SqliteCommand("INSERT OR IGNORE INTO users (username,created_at) VALUES (@u,@t)", db);
        c1.Parameters.AddWithValue("@u",msg.Username); c1.Parameters.AddWithValue("@t",NowTS()); c1.ExecuteNonQuery();
        var c2 = new SqliteCommand("INSERT INTO logins (username,timestamp) VALUES (@u,@t)", db);
        c2.Parameters.AddWithValue("@u",msg.Username); c2.Parameters.AddWithValue("@t",NowTS()); c2.ExecuteNonQuery();
        var r = MakeResp("ok",$"Welcome, {msg.Username}!"); r.Rank=serverRank; return r;
    }
    static OutMsg HandleCreateChannel(InMsg msg) {
        if (string.IsNullOrWhiteSpace(msg.ChannelName)) return MakeResp("error","Channel name cannot be empty");
        try {
            var c = new SqliteCommand("INSERT INTO channels (name,created_by,created_at) VALUES (@n,@u,@t)", db);
            c.Parameters.AddWithValue("@n",msg.ChannelName); c.Parameters.AddWithValue("@u",msg.Username); c.Parameters.AddWithValue("@t",NowTS());
            c.ExecuteNonQuery(); return MakeResp("ok",$"Channel '{msg.ChannelName}' created!");
        } catch { return MakeResp("error",$"Channel '{msg.ChannelName}' already exists"); }
    }
    static OutMsg HandleListChannels() {
        var r = new SqliteCommand("SELECT name FROM channels ORDER BY created_at", db).ExecuteReader();
        var list = new List<string>(); while(r.Read()) list.Add(r.GetString(0));
        var resp = MakeResp("ok","OK"); resp.Data=list; return resp;
    }
    static OutMsg HandlePublish(InMsg msg, PublisherSocket pub) {
        if (string.IsNullOrWhiteSpace(msg.ChannelName)||string.IsNullOrWhiteSpace(msg.Message))
            return MakeResp("error","Channel and message required");
        var chk = new SqliteCommand("SELECT name FROM channels WHERE name=@n", db);
        chk.Parameters.AddWithValue("@n",msg.ChannelName);
        if (chk.ExecuteScalar()==null) return MakeResp("error",$"Channel '{msg.ChannelName}' does not exist");
        var ins = new SqliteCommand("INSERT INTO messages (channel,username,message,timestamp,clock) VALUES (@c,@u,@m,@t,@k)", db);
        ins.Parameters.AddWithValue("@c",msg.ChannelName); ins.Parameters.AddWithValue("@u",msg.Username);
        ins.Parameters.AddWithValue("@m",msg.Message);     ins.Parameters.AddWithValue("@t",NowTS());
        ins.Parameters.AddWithValue("@k",msg.Clock);       ins.ExecuteNonQuery();
        long clk = TickSend();
        pub.SendMoreFrame(msg.ChannelName).SendFrame(MessagePackSerializer.Serialize(new PubPayload {
            Channel=msg.ChannelName, Username=msg.Username, Message=msg.Message,
            Timestamp=NowTS(), Received=NowTS(), Clock=clk }, opts));
        Console.WriteLine($"[{serverName}] PUB  | channel={msg.ChannelName,-15} | from={msg.Username,-12} | clock={clk} | {msg.Message}");
        return MakeResp("ok","Published!");
    }

    static void Main() {
        string port     = Environment.GetEnvironmentVariable("PORT")       ?? "5552";
        string proxy    = Environment.GetEnvironmentVariable("PROXY_HOST") ?? "proxy";
        string xsub     = Environment.GetEnvironmentVariable("XSUB_PORT") ?? "5557";
        InitDB();
        Thread.Sleep(2000);
        ConnectToReference();

        using var server = new ResponseSocket(); server.Bind($"tcp://*:{port}");
        using var pub    = new PublisherSocket(); pub.Connect($"tcp://{proxy}:{xsub}");
        Thread.Sleep(500);
        Console.WriteLine($"[{serverName}] Listening on port {port} | rank={serverRank}");

        long msgCount = 0;
        while (true) {
            byte[] raw = server.ReceiveFrameBytes();
            var msg = MessagePackSerializer.Deserialize<InMsg>(raw, opts);
            TickRecv(msg.Clock); msgCount++;
            Console.WriteLine($"[{serverName}] RECV | type={msg.Type,-10} | from={msg.Username,-12} | clock={msg.Clock} | lc={logicClock}");
            OutMsg resp = msg.Type switch {
                "login"   => HandleLogin(msg),
                "channel" => HandleCreateChannel(msg),
                "list"    => HandleListChannels(),
                "publish" => HandlePublish(msg, pub),
                _         => MakeResp("error",$"Unknown: {msg.Type}")
            };
            Console.WriteLine($"[{serverName}] SEND | status={resp.Status,-8} | clock={resp.Clock}");
            server.SendFrame(MessagePackSerializer.Serialize(resp, opts));
            if (msgCount % 10 == 0) new Thread(() => SendHeartbeat(msgCount)) { IsBackground=true }.Start();
        }
    }
}
