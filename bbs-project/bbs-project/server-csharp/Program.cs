using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
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
    [Key("status")]      public string              Status      { get; set; } = "";
    [Key("message")]     public string              Message     { get; set; } = "";
    [Key("data")]        public object?             Data        { get; set; }
    [Key("timestamp")]   public double              Timestamp   { get; set; }
    [Key("clock")]       public long                Clock       { get; set; }
    [Key("rank")]        public int                 Rank        { get; set; }
    [Key("time")]        public double              Time        { get; set; }
    [Key("coordinator")] public string?             Coordinator { get; set; }
    [Key("servers")]     public List<Dictionary<string,object>>? Servers { get; set; }
}
[MessagePackObject] public class PubPayload {
    [Key("channel")]   public string Channel   { get; set; } = "";
    [Key("username")]  public string Username  { get; set; } = "";
    [Key("message")]   public string Message   { get; set; } = "";
    [Key("timestamp")] public double Timestamp { get; set; }
    [Key("received")]  public double Received  { get; set; }
    [Key("clock")]     public long   Clock     { get; set; }
    [Key("origin")]    public string Origin    { get; set; } = "";
}

class Server {
    static long   _lc = 0; static object _lcLock = new();
    static double _offset = 0; static object _offLock = new();
    static int    _rank = 0;
    static string _name = "server-csharp";
    static string _coordinator = ""; static object _coordLock = new();
    static List<Dictionary<string,object>> _knownServers = new(); static object _srvLock = new();
    static PublisherSocket? _pub;
    static SqliteConnection? db; static object _dbLock = new();
    static readonly MessagePackSerializerOptions opts = MessagePackSerializerOptions.Standard;
    static string refHost="reference",refPort="5559",proxyHost="proxy",xsubPort="5557",xpubPort="5558",s2sPort="5562";

    static double NowTS() { lock(_offLock) { return (double)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()/1000.0+_offset; } }
    static long TickSend() { lock(_lcLock) { _lc++; return _lc; } }
    static void TickRecv(long r) { lock(_lcLock) { if(r>_lc) _lc=r; } }
    static string MakeMsgID(string channel, string username, string message, double ts) {
        var key  = $"{channel}|{username}|{message}|{ts:F3}";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(key));
        return Convert.ToHexString(hash)[..16];
    }
    static string S2SPortOf(string name) => name switch {
        "server-python"=>"5560","server-go"=>"5561","server-csharp"=>"5562","server-c"=>"5563","server-lua"=>"5564",_=>"5560"};

    static void InitDB() {
        Directory.CreateDirectory("/data");
        db = new SqliteConnection("Data Source=/data/server.db"); db.Open();
        new SqliteCommand(@"CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, created_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, timestamp REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT, msg_id TEXT UNIQUE NOT NULL,
                channel TEXT NOT NULL, username TEXT NOT NULL, message TEXT NOT NULL,
                timestamp REAL NOT NULL, clock INTEGER NOT NULL DEFAULT 0, origin TEXT NOT NULL DEFAULT 'local');", db).ExecuteNonQuery();
    }

    static void ReplicateMessage(PubPayload p) {
        if (string.IsNullOrEmpty(p.Channel) || string.IsNullOrEmpty(p.Message)) return;
        var msgID = MakeMsgID(p.Channel, p.Username, p.Message, p.Timestamp);
        lock(_dbLock) {
            var ci = new SqliteCommand("INSERT OR IGNORE INTO channels (name,created_by,created_at) VALUES(@n,@u,@t)", db);
            ci.Parameters.AddWithValue("@n",p.Channel); ci.Parameters.AddWithValue("@u",p.Username); ci.Parameters.AddWithValue("@t",p.Timestamp); ci.ExecuteNonQuery();
            var cm = new SqliteCommand("INSERT OR IGNORE INTO messages (msg_id,channel,username,message,timestamp,clock,origin) VALUES(@id,@c,@u,@m,@t,@lc,@o)", db);
            cm.Parameters.AddWithValue("@id",msgID); cm.Parameters.AddWithValue("@c",p.Channel); cm.Parameters.AddWithValue("@u",p.Username);
            cm.Parameters.AddWithValue("@m",p.Message); cm.Parameters.AddWithValue("@t",p.Timestamp); cm.Parameters.AddWithValue("@lc",p.Clock); cm.Parameters.AddWithValue("@o",p.Origin);
            if (cm.ExecuteNonQuery() > 0) Console.WriteLine($"[{_name}] REPL | channel={p.Channel,-15} | from={p.Username,-12} | origin={p.Origin}");
        }
    }

    static void ReplicationThread() {
        using var sub = new SubscriberSocket();
        sub.Connect($"tcp://{proxyHost}:{xpubPort}");
        Thread.Sleep(1000);
        sub.SubscribeToAnyTopic();
        Console.WriteLine($"[{_name}] REPL SUB | subscribed to all topics on proxy");
        while (true) {
            var topic   = sub.ReceiveFrameString();
            var rawData = sub.ReceiveFrameBytes();
            if (topic == "servers") continue;
            try {
                var p = MessagePackSerializer.Deserialize<PubPayload>(rawData, opts);
                TickRecv(p.Clock); ReplicateMessage(p);
            } catch { }
        }
    }

    static OutMsg ReqCall(string addr, object payload, int timeoutMs=5000) {
        try {
            using var sock = new RequestSocket(); sock.Connect(addr);
            sock.SendFrame(MessagePackSerializer.Serialize(payload, opts));
            byte[]? raw; if (!sock.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(timeoutMs), out raw)||raw==null) return new OutMsg();
            return MessagePackSerializer.Deserialize<OutMsg>(raw, opts);
        } catch { return new OutMsg(); }
    }
    static OutMsg CallRef(object p) => ReqCall($"tcp://{refHost}:{refPort}", p);
    static OutMsg CallS2S(string name, object p) => ReqCall($"tcp://{name}:{S2SPortOf(name)}", p, 3000);

    static void ConnectToReference() {
        var r=CallRef(new{type="register",name=_name,clock=TickSend(),timestamp=NowTS()}); TickRecv(r.Clock); _rank=r.Rank;
        Console.WriteLine($"[{_name}] Registered | rank={_rank}");
    }
    static void GetServerList() {
        var r=CallRef(new{type="list",name=_name,clock=TickSend(),timestamp=NowTS()}); TickRecv(r.Clock);
        lock(_srvLock){_knownServers=r.Servers??new();}
    }
    static void SendHeartbeat() {
        var r=CallRef(new{type="heartbeat",name=_name,clock=TickSend(),timestamp=NowTS()}); TickRecv(r.Clock);
        Console.WriteLine($"[{_name}] HEARTBEAT sent | rank={_rank} | clock={_lc}");
    }
    static void SyncWithCoordinator() {
        string coord; lock(_coordLock){coord=_coordinator;} if(string.IsNullOrEmpty(coord)||coord==_name) return;
        var r=CallS2S(coord,new{type="get_time",name=_name,clock=TickSend(),timestamp=NowTS()});
        if(r.Status!="ok"){new Thread(StartElection){IsBackground=true}.Start();return;}
        TickRecv(r.Clock);
        if(r.Time>0){var off=r.Time-(double)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()/1000.0; lock(_offLock){_offset=off;} Console.WriteLine($"[{_name}] CLOCK SYNC | coord={coord} | ref_time={r.Time:F3} | offset={off:F6}");}
    }
    static void StartElection() {
        Console.WriteLine($"[{_name}] Starting election | rank={_rank}"); GetServerList();
        List<Dictionary<string,object>> others; lock(_srvLock){others=_knownServers.FindAll(s=>s.ContainsKey("name")&&s["name"].ToString()!=_name);}
        var cands=new List<(string name,int rank)>{(_name,_rank)};
        foreach(var srv in others){string n=srv["name"].ToString()!; var r=CallS2S(n,new{type="election",name=_name,rank=_rank,clock=TickSend()}); if(r.Status=="ok") cands.Add((n,r.Rank));}
        var winner=cands[0]; foreach(var c in cands){if(c.rank<winner.rank) winner=c;}
        lock(_coordLock){_coordinator=winner.name;} if(winner.name==_name) AnnounceCoordinator();
        Console.WriteLine($"[{_name}] Election result: coordinator='{winner.name}'");
    }
    static void AnnounceCoordinator() {
        var clk=TickSend(); var p=MessagePackSerializer.Serialize(new{coordinator=_name,clock=clk,timestamp=NowTS()},opts);
        _pub!.SendMoreFrame("servers").SendFrame(p); Console.WriteLine($"[{_name}] ELECTED as coordinator | clock={clk}");
    }
    static void S2SServerThread() {
        using var sock=new ResponseSocket(); sock.Bind($"tcp://*:{s2sPort}");
        while(true){var raw=sock.ReceiveFrameBytes(); var msg=MessagePackSerializer.Deserialize<InMsg>(raw,opts); TickRecv(msg.Clock);
            OutMsg resp=msg.Type switch{"get_time"=>new OutMsg{Status="ok",Time=NowTS(),Clock=TickSend(),Timestamp=NowTS()},"election"=>new OutMsg{Status="ok",Rank=_rank,Clock=TickSend(),Timestamp=NowTS()},_=>new OutMsg{Status="error",Message="Unknown",Clock=TickSend()}};
            sock.SendFrame(MessagePackSerializer.Serialize(resp,opts));}
    }
    static void ServersSubThread() {
        using var sub=new SubscriberSocket(); sub.Connect($"tcp://{proxyHost}:{xpubPort}"); sub.Subscribe("servers");
        while(true){sub.ReceiveFrameBytes(); var raw=sub.ReceiveFrameBytes();
            try{var d=MessagePackSerializer.Deserialize<OutMsg>(raw,opts); TickRecv(d.Clock);
                if(!string.IsNullOrEmpty(d.Coordinator)){lock(_coordLock){_coordinator=d.Coordinator;} Console.WriteLine($"[{_name}] New coordinator: '{d.Coordinator}'");}}catch{}
        }
    }

    static OutMsg MakeResp(string status,string msg,object? data=null)=>new OutMsg{Status=status,Message=msg,Data=data,Clock=TickSend(),Timestamp=NowTS()};
    static OutMsg HandleLogin(InMsg msg){
        if(string.IsNullOrWhiteSpace(msg.Username)) return MakeResp("error","Username cannot be empty");
        lock(_dbLock){var c1=new SqliteCommand("INSERT OR IGNORE INTO users (username,created_at) VALUES(@u,@t)",db); c1.Parameters.AddWithValue("@u",msg.Username); c1.Parameters.AddWithValue("@t",NowTS()); c1.ExecuteNonQuery();
            var c2=new SqliteCommand("INSERT INTO logins (username,timestamp) VALUES(@u,@t)",db); c2.Parameters.AddWithValue("@u",msg.Username); c2.Parameters.AddWithValue("@t",NowTS()); c2.ExecuteNonQuery();}
        var r=MakeResp("ok",$"Welcome, {msg.Username}!"); r.Rank=_rank; return r;
    }
    static OutMsg HandleCreateChannel(InMsg msg){
        if(string.IsNullOrWhiteSpace(msg.ChannelName)) return MakeResp("error","Channel name cannot be empty");
        try{lock(_dbLock){var c=new SqliteCommand("INSERT INTO channels (name,created_by,created_at) VALUES(@n,@u,@t)",db); c.Parameters.AddWithValue("@n",msg.ChannelName); c.Parameters.AddWithValue("@u",msg.Username); c.Parameters.AddWithValue("@t",NowTS()); c.ExecuteNonQuery();} return MakeResp("ok",$"Channel '{msg.ChannelName}' created!");}
        catch(SqliteException){return MakeResp("error",$"Channel '{msg.ChannelName}' already exists");}
    }
    static OutMsg HandleListChannels(){
        lock(_dbLock){var r=new SqliteCommand("SELECT name FROM channels ORDER BY created_at",db).ExecuteReader(); var list=new List<string>(); while(r.Read()) list.Add(r.GetString(0)); return MakeResp("ok","OK",list);}
    }
    static OutMsg HandlePublish(InMsg msg){
        if(string.IsNullOrWhiteSpace(msg.ChannelName)||string.IsNullOrWhiteSpace(msg.Message)) return MakeResp("error","Channel and message required");
        lock(_dbLock){var chk=new SqliteCommand("SELECT name FROM channels WHERE name=@n",db); chk.Parameters.AddWithValue("@n",msg.ChannelName); if(chk.ExecuteScalar()==null) return MakeResp("error",$"Channel '{msg.ChannelName}' does not exist");}
        var clk=TickSend(); var ts=NowTS(); var msgID=MakeMsgID(msg.ChannelName,msg.Username,msg.Message,ts);
        lock(_dbLock){var ins=new SqliteCommand("INSERT OR IGNORE INTO messages (msg_id,channel,username,message,timestamp,clock,origin) VALUES(@id,@c,@u,@m,@t,@lc,@o)",db);
            ins.Parameters.AddWithValue("@id",msgID); ins.Parameters.AddWithValue("@c",msg.ChannelName); ins.Parameters.AddWithValue("@u",msg.Username);
            ins.Parameters.AddWithValue("@m",msg.Message); ins.Parameters.AddWithValue("@t",ts); ins.Parameters.AddWithValue("@lc",clk); ins.Parameters.AddWithValue("@o",_name); ins.ExecuteNonQuery();}
        var payload=MessagePackSerializer.Serialize(new PubPayload{Channel=msg.ChannelName,Username=msg.Username,Message=msg.Message,Timestamp=ts,Received=ts,Clock=clk,Origin=_name},opts);
        _pub!.SendMoreFrame(msg.ChannelName).SendFrame(payload);
        Console.WriteLine($"[{_name}] PUB  | channel={msg.ChannelName,-15} | from={msg.Username,-12} | clock={clk}");
        return MakeResp("ok","Published!");
    }
    static OutMsg HandleHistory(InMsg msg){
        if(string.IsNullOrWhiteSpace(msg.ChannelName)) return MakeResp("error","Channel required");
        lock(_dbLock){var r=new SqliteCommand("SELECT username,message,timestamp,clock,origin FROM messages WHERE channel=@c ORDER BY timestamp",db); r.Parameters.AddWithValue("@c",msg.ChannelName); var reader=r.ExecuteReader();
            var list=new List<Dictionary<string,object>>(); while(reader.Read()) list.Add(new Dictionary<string,object>{{"username",reader.GetString(0)},{"message",reader.GetString(1)},{"timestamp",reader.GetDouble(2)},{"clock",reader.GetInt64(3)},{"origin",reader.GetString(4)}});
            return MakeResp("ok","OK",list);}
    }

    static void Main(){
        string port=Environment.GetEnvironmentVariable("PORT")??"5552";
        s2sPort=Environment.GetEnvironmentVariable("S2S_PORT")??"5562";
        proxyHost=Environment.GetEnvironmentVariable("PROXY_HOST")??"proxy";
        xsubPort=Environment.GetEnvironmentVariable("XSUB_PORT")??"5557";
        xpubPort=Environment.GetEnvironmentVariable("XPUB_PORT")??"5558";
        refHost=Environment.GetEnvironmentVariable("REF_HOST")??"reference";
        refPort=Environment.GetEnvironmentVariable("REF_PORT")??"5559";
        _name=Environment.GetEnvironmentVariable("SERVER_NAME")??"server-csharp";
        InitDB();
        _pub=new PublisherSocket(); _pub.Connect($"tcp://{proxyHost}:{xsubPort}"); Thread.Sleep(1000);
        Thread.Sleep(2000); ConnectToReference(); GetServerList();
        new Thread(S2SServerThread){IsBackground=true}.Start();
        new Thread(ServersSubThread){IsBackground=true}.Start();
        new Thread(ReplicationThread){IsBackground=true}.Start();
        Thread.Sleep(1500); new Thread(StartElection){IsBackground=true}.Start();
        using var server=new ResponseSocket(); server.Bind($"tcp://*:{port}");
        Console.WriteLine($"[{_name}] Listening on port {port} | rank={_rank}");
        long msgCount=0;
        while(true){var raw=server.ReceiveFrameBytes(); var msg=MessagePackSerializer.Deserialize<InMsg>(raw,opts); TickRecv(msg.Clock); msgCount++;
            Console.WriteLine($"[{_name}] RECV | type={msg.Type,-10} | from={msg.Username,-12} | clock={msg.Clock} | lc={_lc}");
            OutMsg resp=msg.Type switch{"login"=>HandleLogin(msg),"channel"=>HandleCreateChannel(msg),"list"=>HandleListChannels(),"publish"=>HandlePublish(msg),"history"=>HandleHistory(msg),_=>MakeResp("error",$"Unknown: {msg.Type}")};
            Console.WriteLine($"[{_name}] SEND | status={resp.Status,-8} | clock={resp.Clock}");
            server.SendFrame(MessagePackSerializer.Serialize(resp,opts));
            if(msgCount%15==0){new Thread(SendHeartbeat){IsBackground=true}.Start(); new Thread(SyncWithCoordinator){IsBackground=true}.Start();}
        }
    }
}