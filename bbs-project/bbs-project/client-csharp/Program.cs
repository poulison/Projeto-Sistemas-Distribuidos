using System;
using System.Collections.Generic;
using System.Threading;
using MessagePack;
using NetMQ;
using NetMQ.Sockets;

[MessagePackObject]
public class ReqMsg {
    [Key("type")]         public string Type        { get; set; } = "";
    [Key("username")]     public string Username    { get; set; } = "";
    [Key("channel_name")] public string ChannelName { get; set; } = "";
    [Key("message")]      public string Message     { get; set; } = "";
    [Key("timestamp")]    public double Timestamp   { get; set; }
}
[MessagePackObject]
public class RespMsg {
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

class Client {
    static string botName    = Environment.GetEnvironmentVariable("BOT_NAME")    ?? "bot-cs-1";
    static string serverHost = Environment.GetEnvironmentVariable("SERVER_HOST") ?? "server-csharp";
    static string serverPort = Environment.GetEnvironmentVariable("SERVER_PORT") ?? "5552";
    static string proxyHost  = Environment.GetEnvironmentVariable("PROXY_HOST")  ?? "proxy";
    static string xpubPort   = Environment.GetEnvironmentVariable("XPUB_PORT")   ?? "5558";
    static readonly MessagePackSerializerOptions opts = MessagePackSerializerOptions.Standard;
    static RequestSocket req = new RequestSocket();
    static Random rng = new Random();
    static string[] words = {"ola","mundo","sistema","distribuido","mensagem","canal",
        "teste","csharp","zmq","pubsub","broker","topico","servidor","rede"};

    static double NowTS() => (double)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;

    static string RandomMsg() {
        int n = 3 + rng.Next(5);
        var parts = new List<string>();
        for (int i = 0; i < n; i++) parts.Add(words[rng.Next(words.Length)]);
        return string.Join(" ", parts);
    }

    static RespMsg SendRecv(ReqMsg payload) {
        Console.WriteLine($"[{botName}] SEND | type={payload.Type,-10} | ts={payload.Timestamp:F3}");
        req.SendFrame(MessagePackSerializer.Serialize(payload, opts));
        var raw = req.ReceiveFrameBytes();
        var resp = MessagePackSerializer.Deserialize<RespMsg>(raw, opts);
        Console.WriteLine($"[{botName}] RECV | status={resp.Status,-8} | msg={resp.Message}");
        return resp;
    }

    static void SubscriberThread(List<string> channels) {
        using var sub = new SubscriberSocket();
        sub.Connect($"tcp://{proxyHost}:{xpubPort}");
        Thread.Sleep(500);
        foreach (var ch in channels) {
            sub.Subscribe(ch);
            Console.WriteLine($"[{botName}] SUB  | subscribed to '{ch}'");
        }
        while (true) {
            var topic   = sub.ReceiveFrameBytes();
            var rawData = sub.ReceiveFrameBytes();
            var p = MessagePackSerializer.Deserialize<PubPayload>(rawData, opts);
            Console.WriteLine($"[{botName}] MSG  | channel={p.Channel,-12} | from={p.Username,-15} | sent={p.Timestamp:F3} | recv={NowTS():F3} | {p.Message}");
        }
    }

    static void Main() {
        Thread.Sleep(3000);
        req.Connect($"tcp://{serverHost}:{serverPort}");
        Console.WriteLine($"[{botName}] Connected to {serverHost}:{serverPort}");

        // login
        while (true) {
            var r = SendRecv(new ReqMsg { Type="login", Username=botName, Timestamp=NowTS() });
            if (r.Status == "ok") { Console.WriteLine($"[{botName}] ✔ Login successful!"); break; }
            Thread.Sleep(2000);
        }

        // lista canais
        var resp = SendRecv(new ReqMsg { Type="list", Username=botName, Timestamp=NowTS() });
        var channels = resp.Data ?? new List<string>();

        // cria canal se < 5
        if (channels.Count < 5) {
            var newCh = $"ch-{botName.Replace("-","").Substring(0,5)}-{rng.Next(100,999)}";
            SendRecv(new ReqMsg { Type="channel", Username=botName, ChannelName=newCh, Timestamp=NowTS() });
            resp = SendRecv(new ReqMsg { Type="list", Username=botName, Timestamp=NowTS() });
            channels = resp.Data ?? new List<string>();
        }
        Console.WriteLine($"[{botName}] Channels available: [{string.Join(", ", channels)}]");

        // inscreve em até 3 canais
        var subChannels = new List<string>(channels);
        for (int i = subChannels.Count - 1; i > 0; i--) { int j = rng.Next(i+1); var t = subChannels[i]; subChannels[i] = subChannels[j]; subChannels[j] = t; }
        if (subChannels.Count > 3) subChannels = subChannels.GetRange(0, 3);

        new Thread(() => SubscriberThread(subChannels)) { IsBackground = true }.Start();
        Thread.Sleep(1500);

        // loop infinito
        Console.WriteLine($"[{botName}] Starting publish loop");
        while (true) {
            var ch = channels[rng.Next(channels.Count)];
            for (int i = 0; i < 10; i++) {
                SendRecv(new ReqMsg { Type="publish", Username=botName, ChannelName=ch, Message=RandomMsg(), Timestamp=NowTS() });
                Thread.Sleep(1000);
            }
            resp = SendRecv(new ReqMsg { Type="list", Username=botName, Timestamp=NowTS() });
            channels = resp.Data ?? channels;
        }
    }
}
