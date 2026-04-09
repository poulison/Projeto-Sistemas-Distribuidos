import zmq
import msgpack
import time
import os
import random
import threading

BOT_NAME    = os.getenv("BOT_NAME",    "bot-py-1")
SERVER_HOST = os.getenv("SERVER_HOST", "server-python")
SERVER_PORT = os.getenv("SERVER_PORT", "5550")
PROXY_HOST  = os.getenv("PROXY_HOST",  "proxy")
XPUB_PORT   = os.getenv("XPUB_PORT",   "5558")

RANDOM_WORDS = [
    "ola", "mundo", "sistema", "distribuido", "mensagem", "canal",
    "teste", "python", "zmq", "pubsub", "broker", "topico", "servidor",
    "cliente", "rede", "dados", "processo", "thread", "socket", "porta"
]

subscribed_channels = []
req_socket = None


def random_message():
    return " ".join(random.choices(RANDOM_WORDS, k=random.randint(3, 7)))


def send_recv(payload):
    raw = msgpack.packb(payload, use_bin_type=True)
    print(f"[{BOT_NAME}] SEND | type={payload['type']:<10} | ts={payload['timestamp']:.3f}", flush=True)
    req_socket.send(raw)
    resp = msgpack.unpackb(req_socket.recv(), raw=False)
    print(f"[{BOT_NAME}] RECV | status={resp['status']:<8} | msg={resp['message']}", flush=True)
    return resp


def login():
    while True:
        resp = send_recv({"type": "login", "username": BOT_NAME, "timestamp": time.time()})
        if resp["status"] == "ok":
            print(f"[{BOT_NAME}] ✔ Login successful!", flush=True)
            return
        time.sleep(2)


def list_channels():
    resp = send_recv({"type": "list", "username": BOT_NAME, "timestamp": time.time()})
    channels = resp.get("data", []) if resp["status"] == "ok" else []
    print(f"[{BOT_NAME}] Channels available: {channels}", flush=True)
    return channels


def create_channel(name):
    return send_recv({"type": "channel", "username": BOT_NAME,
                      "channel_name": name, "timestamp": time.time()})


def publish(channel, message):
    return send_recv({"type": "publish", "username": BOT_NAME,
                      "channel_name": channel, "message": message,
                      "timestamp": time.time()})


# ── thread de recebimento SUB ─────────────────────────────────────────────────

def subscriber_thread():
    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{PROXY_HOST}:{XPUB_PORT}")

    # aguarda inscrições serem definidas
    time.sleep(1)
    for ch in subscribed_channels:
        sub.setsockopt_string(zmq.SUBSCRIBE, ch)
        print(f"[{BOT_NAME}] SUB  | subscribed to '{ch}'", flush=True)

    while True:
        try:
            topic, raw = sub.recv_multipart()
            data = msgpack.unpackb(raw, raw=False)
            channel   = data.get("channel", "?")
            username  = data.get("username", "?")
            message   = data.get("message", "?")
            ts_send   = data.get("timestamp", 0)
            ts_recv   = time.time()
            print(
                f"[{BOT_NAME}] MSG  | channel={channel:<12} | from={username:<15} "
                f"| sent={ts_send:.3f} | recv={ts_recv:.3f} | {message}",
                flush=True
            )
        except Exception as e:
            print(f"[{BOT_NAME}] SUB error: {e}", flush=True)


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    global req_socket, subscribed_channels
    time.sleep(3)

    ctx = zmq.Context()
    req_socket = ctx.socket(zmq.REQ)
    req_socket.connect(f"tcp://{SERVER_HOST}:{SERVER_PORT}")
    print(f"[{BOT_NAME}] Connected to {SERVER_HOST}:{SERVER_PORT}", flush=True)

    login()

    # 1. se existirem menos de 5 canais, cria um novo
    channels = list_channels()
    if len(channels) < 5:
        new_ch = f"ch-{BOT_NAME.replace('-', '')}-{random.randint(100,999)}"
        create_channel(new_ch)
        channels = list_channels()

    # 2. inscreve em até 3 canais aleatórios
    sample = random.sample(channels, min(3, len(channels)))
    subscribed_channels = sample

    # inicia thread de recebimento
    t = threading.Thread(target=subscriber_thread, daemon=True)
    t.start()
    time.sleep(1.5)  # aguarda thread se inscrever

    # 3. loop infinito: escolhe canal, envia 10 mensagens com 1s de intervalo
    print(f"[{BOT_NAME}] Starting publish loop on channels: {subscribed_channels}", flush=True)
    while True:
        channel = random.choice(channels)
        for _ in range(10):
            msg = random_message()
            publish(channel, msg)
            time.sleep(1)
        # atualiza lista de canais periodicamente
        channels = list_channels()


if __name__ == "__main__":
    main()
