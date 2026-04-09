using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using MessagePack;
using Microsoft.Data.Sqlite;
using NetMQ;
using NetMQ.Sockets;

[MessagePackObject]
public class InMsg {
    [Key("type")]         public string Type        { get; set; } = "";
    [Key("username")]     public string Username    { get; set; } = "";
    [Key("channel_name")] public string ChannelName { get; set; } = "";
    [Key("message")]      public string Message     { get; set; } = "";
    [Key("timestamp")]    public double Timestamp   { get; set; }
}
[MessagePackObject]
public class OutMsg {
    [Key("status")]    public string        Status    { get; set; } = "";
    [Key("message")]   public string        Message   { get; set; } = "";
    [Key("data")]      public List<string>? Data      { get; set; }
    [Key("timestamp")] public double        Timestamp { get; set; }
}
[MessagePackObject]
public class PubPayload {
    [Key("channel")]   public string Channel   { get; set; } = "";
    [Key("username")]  public string Username  { get; set; } = "";
    [Key("message")]   public string Message   { get; set; } = "";
    [Key("timestamp")] public double Timestamp { get; set; }
    [Key("received")]  public double Received  { get; set; }
}

class Server {
    static SqliteConnection? db;
    static readonly MessagePackSerializerOptions opts = MessagePackSerializerOptions.Standard;
    static double NowTS() => (double)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;
    static OutMsg Err(string m) => new() { Status="error", Message=m, Timestamp=NowTS() };
    static OutMsg Ok(string m)  => new() { Status="ok",    Message=m, Timestamp=NowTS() };

    static void InitDB() {
        Directory.CreateDirectory("/data");
        db = new SqliteConnection("Data Source=/data/server.db");
        db.Open();
        new SqliteCommand(@"
            CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, created_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, timestamp REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, username TEXT NOT NULL, message TEXT NOT NULL, timestamp REAL NOT NULL);
        ", db).ExecuteNonQuery();
    }

    static OutMsg HandleLogin(InMsg msg) {
        if (string.IsNullOrWhiteSpace(msg.Username)) return Err("Username cannot be empty");
        var c1 = new SqliteCommand("INSERT OR IGNORE INTO users (username, created_at) VALUES (@u,@t)", db);
        c1.Parameters.AddWithValue("@u", msg.Username); c1.Parameters.AddWithValue("@t", msg.Timestamp); c1.ExecuteNonQuery();
        var c2 = new SqliteCommand("INSERT INTO logins (username, timestamp) VALUES (@u,@t)", db);
        c2.Parameters.AddWithValue("@u", msg.Username); c2.Parameters.AddWithValue("@t", msg.Timestamp); c2.ExecuteNonQuery();
        return Ok($"Welcome, {msg.Username}!");
    }

    static OutMsg HandleCreateChannel(InMsg msg) {
        if (string.IsNullOrWhiteSpace(msg.ChannelName)) return Err("Channel name cannot be empty");
        try {
            var c = new SqliteCommand("INSERT INTO channels (name,created_by,created_at) VALUES (@n,@u,@t)", db);
            c.Parameters.AddWithValue("@n", msg.ChannelName); c.Parameters.AddWithValue("@u", msg.Username); c.Parameters.AddWithValue("@t", msg.Timestamp);
            c.ExecuteNonQuery();
            return Ok($"Channel '{msg.ChannelName}' created!");
        } catch (SqliteException) { return Err($"Channel '{msg.ChannelName}' already exists"); }
    }

    static OutMsg HandleListChannels() {
        var r = new SqliteCommand("SELECT name FROM channels ORDER BY created_at", db).ExecuteReader();
        var list = new List<string>();
        while (r.Read()) list.Add(r.GetString(0));
        return new OutMsg { Status="ok", Message="OK", Data=list, Timestamp=NowTS() };
    }

    static OutMsg HandlePublish(InMsg msg, PublisherSocket pub) {
        if (string.IsNullOrWhiteSpace(msg.ChannelName) || string.IsNullOrWhiteSpace(msg.Message))
            return Err("Channel and message are required");
        var chk = new SqliteCommand("SELECT name FROM channels WHERE name=@n", db);
        chk.Parameters.AddWithValue("@n", msg.ChannelName);
        if (chk.ExecuteScalar() == null) return Err($"Channel '{msg.ChannelName}' does not exist");

        var ins = new SqliteCommand("INSERT INTO messages (channel,username,message,timestamp) VALUES (@c,@u,@m,@t)", db);
        ins.Parameters.AddWithValue("@c", msg.ChannelName); ins.Parameters.AddWithValue("@u", msg.Username);
        ins.Parameters.AddWithValue("@m", msg.Message);     ins.Parameters.AddWithValue("@t", msg.Timestamp);
        ins.ExecuteNonQuery();

        var payload = MessagePackSerializer.Serialize(new PubPayload {
            Channel=msg.ChannelName, Username=msg.Username,
            Message=msg.Message, Timestamp=msg.Timestamp, Received=NowTS()
        }, opts);
        pub.SendMoreFrame(msg.ChannelName).SendFrame(payload);

        Console.WriteLine($"[SERVER-CSHARP] PUB  | channel={msg.ChannelName,-15} | from={msg.Username,-15} | msg={msg.Message}");
        return Ok("Published!");
    }

    static void Main() {
        string port      = Environment.GetEnvironmentVariable("PORT")       ?? "5552";
        string proxyHost = Environment.GetEnvironmentVariable("PROXY_HOST") ?? "proxy";
        string xsubPort  = Environment.GetEnvironmentVariable("XSUB_PORT") ?? "5557";
        InitDB();

        using var server = new ResponseSocket();
        server.Bind($"tcp://*:{port}");

        using var pub = new PublisherSocket();
        pub.Connect($"tcp://{proxyHost}:{xsubPort}");
        Thread.Sleep(500);

        Console.WriteLine($"[SERVER-CSHARP] Listening on port {port}");
        Console.WriteLine($"[SERVER-CSHARP] Publishing to proxy {proxyHost}:{xsubPort}");

        while (true) {
            byte[] raw = server.ReceiveFrameBytes();
            var msg = MessagePackSerializer.Deserialize<InMsg>(raw, opts);
            Console.WriteLine($"[SERVER-CSHARP] RECV | type={msg.Type,-10} | from={msg.Username,-15} | ts={msg.Timestamp:F3}");

            OutMsg resp = msg.Type switch {
                "login"   => HandleLogin(msg),
                "channel" => HandleCreateChannel(msg),
                "list"    => HandleListChannels(),
                "publish" => HandlePublish(msg, pub),
                _         => Err($"Unknown type: {msg.Type}")
            };
            Console.WriteLine($"[SERVER-CSHARP] SEND | status={resp.Status,-8} | msg={resp.Message}");
            server.SendFrame(MessagePackSerializer.Serialize(resp, opts));
        }
    }
}
