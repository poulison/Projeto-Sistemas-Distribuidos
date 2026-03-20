using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using MessagePack;
using Microsoft.Data.Sqlite;
using NetMQ;
using NetMQ.Sockets;

// ── modelos ──────────────────────────────────────────────────────────────────

[MessagePackObject]
public class InMsg
{
    [Key("type")]         public string Type        { get; set; } = "";
    [Key("username")]     public string Username    { get; set; } = "";
    [Key("channel_name")] public string ChannelName { get; set; } = "";
    [Key("timestamp")]    public double Timestamp   { get; set; }
}

[MessagePackObject]
public class OutMsg
{
    [Key("status")]    public string        Status    { get; set; } = "";
    [Key("message")]   public string        Message   { get; set; } = "";
    [Key("data")]      public List<string>? Data      { get; set; }
    [Key("timestamp")] public double        Timestamp { get; set; }
}

// ── servidor ─────────────────────────────────────────────────────────────────

class Server
{
    static readonly string dbPath = "/data/server.db";
    static SqliteConnection? db;

    static double NowTS() =>
        (double)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;

    static OutMsg Err(string msg)  => new() { Status = "error", Message = msg,  Timestamp = NowTS() };
    static OutMsg Ok(string msg)   => new() { Status = "ok",    Message = msg,  Timestamp = NowTS() };

    // ── banco ─────────────────────────────────────────────────────────────────

    static void InitDB()
    {
        Directory.CreateDirectory("/data");
        db = new SqliteConnection($"Data Source={dbPath}");
        db.Open();
        new SqliteCommand(@"
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY, created_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS logins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL, timestamp REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS channels (
                name TEXT PRIMARY KEY,
                created_by TEXT NOT NULL, created_at REAL NOT NULL);
        ", db).ExecuteNonQuery();
    }

    // ── handlers ─────────────────────────────────────────────────────────────

    static OutMsg HandleLogin(InMsg msg)
    {
        if (string.IsNullOrWhiteSpace(msg.Username))
            return Err("Username cannot be empty");

        var cmd1 = new SqliteCommand(
            "INSERT OR IGNORE INTO users (username, created_at) VALUES (@u, @t)", db);
        cmd1.Parameters.AddWithValue("@u", msg.Username);
        cmd1.Parameters.AddWithValue("@t", msg.Timestamp);
        cmd1.ExecuteNonQuery();

        var cmd2 = new SqliteCommand(
            "INSERT INTO logins (username, timestamp) VALUES (@u, @t)", db);
        cmd2.Parameters.AddWithValue("@u", msg.Username);
        cmd2.Parameters.AddWithValue("@t", msg.Timestamp);
        cmd2.ExecuteNonQuery();

        return Ok($"Welcome, {msg.Username}!");
    }

    static OutMsg HandleCreateChannel(InMsg msg)
    {
        if (string.IsNullOrWhiteSpace(msg.ChannelName))
            return Err("Channel name cannot be empty");
        if (msg.ChannelName.Length > 32)
            return Err("Channel name too long (max 32 chars)");

        try
        {
            var cmd = new SqliteCommand(
                "INSERT INTO channels (name, created_by, created_at) VALUES (@n, @u, @t)", db);
            cmd.Parameters.AddWithValue("@n", msg.ChannelName);
            cmd.Parameters.AddWithValue("@u", msg.Username);
            cmd.Parameters.AddWithValue("@t", msg.Timestamp);
            cmd.ExecuteNonQuery();
            return Ok($"Channel '{msg.ChannelName}' created!");
        }
        catch (SqliteException)
        {
            return Err($"Channel '{msg.ChannelName}' already exists");
        }
    }

    static OutMsg HandleListChannels()
    {
        var cmd    = new SqliteCommand("SELECT name FROM channels ORDER BY created_at", db);
        var reader = cmd.ExecuteReader();
        var list   = new List<string>();
        while (reader.Read()) list.Add(reader.GetString(0));
        return new OutMsg { Status = "ok", Message = "OK", Data = list, Timestamp = NowTS() };
    }

    // ── main ─────────────────────────────────────────────────────────────────

    static void Main(string[] args)
    {
        string port = Environment.GetEnvironmentVariable("PORT") ?? "5552";
        InitDB();

        using var server = new ResponseSocket();
        server.Bind($"tcp://*:{port}");
        Console.WriteLine($"[SERVER-CSHARP] Listening on port {port}");

        var options = MessagePackSerializerOptions.Standard;

        while (true)
        {
            byte[] raw = server.ReceiveFrameBytes();
            var msg = MessagePackSerializer.Deserialize<InMsg>(raw, options);

            Console.WriteLine($"[SERVER-CSHARP] RECV | type={msg.Type,-10} | from={msg.Username,-15} | ts={msg.Timestamp:F3}");

            OutMsg resp = msg.Type switch
            {
                "login"   => HandleLogin(msg),
                "channel" => HandleCreateChannel(msg),
                "list"    => HandleListChannels(),
                _         => Err($"Unknown type: {msg.Type}")
            };

            Console.WriteLine($"[SERVER-CSHARP] SEND | status={resp.Status,-8} | msg={resp.Message}");

            byte[] respRaw = MessagePackSerializer.Serialize(resp, options);
            server.SendFrame(respRaw);
        }
    }
}
