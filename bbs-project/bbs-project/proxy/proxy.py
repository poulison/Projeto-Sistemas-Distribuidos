import zmq
import os

XSUB_PORT = int(os.getenv("XSUB_PORT", "5557"))
XPUB_PORT = int(os.getenv("XPUB_PORT", "5558"))

def main():
    ctx  = zmq.Context()

    xsub = ctx.socket(zmq.XSUB)
    xsub.bind(f"tcp://*:{XSUB_PORT}")

    xpub = ctx.socket(zmq.XPUB)
    xpub.bind(f"tcp://*:{XPUB_PORT}")

    print(f"[PROXY] XSUB listening on port {XSUB_PORT}", flush=True)
    print(f"[PROXY] XPUB listening on port {XPUB_PORT}", flush=True)

    zmq.proxy(xsub, xpub)

if __name__ == "__main__":
    main()
