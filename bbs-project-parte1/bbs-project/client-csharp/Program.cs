using System;
using System.Collections.Generic;
using System.Threading;
using MessagePack;
using NetMQ;
using NetMQ.Sockets;

[MessagePackObject]
public class OutMsg
{
    [Key("type")]         public string Type        { get; set; } = "";
    [Key("username")]     public string Username    { get; set; } = "";
    [Key("channel_name")] public string ChannelName { get; set; } = "";
    [Key("timestamp")]    public double Timestamp   { get; set; }
}

[MessagePackObject]
public class InMsg
{
    [Key("status")]    public string        Status    { get; set; } = "";
    [Key("message")]   public string        Message   { get; set; } = "";
    [Key("data")]      public List<string>? Data      { get; set; }
    [Key("timestamp")] public double        Timestamp { get; set; }
}

class Client
{
    static string botName    = Environment.GetEnvironmentVariable("BOT_NAME")    ?? "bot-cs-1";
    static string serverHost = Environment.GetEnvironmentVariable("SERVER_HOST") ?? "server-csharp";
    static string serverPort = Environment.GetEnvironmentVariable("SERVER_PORT") ?? "5552";

    static readonly string[] channels = { "geral", "random", "noticias", "projetos", "csharp-talk" };
    static readonly MessagePackSerializerOptions options = MessagePackSerializerOptions.Standard;

    static RequestSocket sock = new RequestSocket();

    static double NowTS() =>
        (double)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;

    static InMsg SendRecv(OutMsg payload)
    {
        byte[] raw = MessagePackSerializer.Serialize(payload, options);
        Console.WriteLine($"[{botName}] SEND | type={payload.Type,-10} | ts={payload.Timestamp:F3}");
        sock.SendFrame(raw);

        byte[] respRaw = sock.ReceiveFrameBytes();
        var resp = MessagePackSerializer.Deserialize<InMsg>(respRaw, options);
        Console.WriteLine($"[{botName}] RECV | status={resp.Status,-8} | msg={resp.Message}");
        return resp;
    }

    static void Login()
    {
        while (true)
        {
            var resp = SendRecv(new OutMsg { Type = "login", Username = botName, Timestamp = NowTS() });
            if (resp.Status == "ok") { Console.WriteLine($"[{botName}] ✔ Login successful!"); return; }
            Console.WriteLine($"[{botName}] ✘ Login failed: {resp.Message} — retrying in 2s...");
            Thread.Sleep(2000);
        }
    }

    static void CreateChannel(string name)
    {
        SendRecv(new OutMsg { Type = "channel", Username = botName, ChannelName = name, Timestamp = NowTS() });
    }

    static void ListChannels()
    {
        var resp = SendRecv(new OutMsg { Type = "list", Username = botName, Timestamp = NowTS() });
        if (resp.Status == "ok")
            Console.WriteLine($"[{botName}] Channels available: [{string.Join(", ", resp.Data ?? new())}]");
    }

    static void Main(string[] args)
    {
        Thread.Sleep(3000);

        sock.Connect($"tcp://{serverHost}:{serverPort}");
        Console.WriteLine($"[{botName}] Connected to {serverHost}:{serverPort}");

        Login();
        Thread.Sleep(500);
        ListChannels();
        Thread.Sleep(500);

        foreach (var ch in channels)
        {
            CreateChannel(ch);
            Thread.Sleep(300);
        }

        ListChannels();
        Console.WriteLine($"[{botName}] ✔ Part 1 done!");
    }
}
