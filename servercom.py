import json
import logging
import os
import threading

from flask import Flask, request
from flask_sock import Sock

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("signaling")

# ---------------- APP ----------------
app = Flask(__name__)

# ⚠️ FIX: only supported option (NO ping_timeout)
app.config["SOCK_SERVER_OPTIONS"] = {
    "ping_interval": 20
}

sock = Sock(app)

# ---------------- CLIENT MODEL ----------------
class Client:
    __slots__ = ("id", "ws", "lock")

    def __init__(self, client_id, ws):
        self.id = client_id
        self.ws = ws
        self.lock = threading.Lock()


# ---------------- STORAGE ----------------
peers = {}
peers_lock = threading.Lock()


# ---------------- SEND HELPERS ----------------
def send(client, obj):
    try:
        with client.lock:
            client.ws.send(json.dumps(obj))
        return True
    except Exception as e:
        log.error("send error: %s", e)
        return False


def send_raw(ws, obj):
    try:
        ws.send(json.dumps(obj))
    except Exception:
        pass


# ---------------- REGISTER ----------------
def register(client_id, ws):
    if not client_id:
        return None

    client = Client(client_id, ws)

    with peers_lock:
        peers[client_id] = client
        current_peers = [cid for cid in peers if cid != client_id]

    log.info("registered %s", client_id)

    send(client, {
        "type": "registered",
        "id": client_id,
        "peers": current_peers
    })

    return client


# ---------------- UNREGISTER ----------------
def unregister(client):
    if not client:
        return

    with peers_lock:
        if peers.get(client.id) is client:
            del peers[client.id]
            log.info("disconnected %s", client.id)


# ---------------- RELAY ----------------
def relay(target_id, message):
    with peers_lock:
        target = peers.get(target_id)

    if not target:
        return False

    return send(target, message)


# ---------------- WEBSOCKET ROUTE ----------------
@sock.route("/ws")
def ws_handler(ws):
    my_id = request.args.get("id")
    client = register(my_id, ws) if my_id else None

    try:
        while True:
            raw = ws.receive()
            if raw is None:
                break

            try:
                msg = json.loads(raw)
            except Exception:
                continue

            mtype = msg.get("type")

            # register via message
            if mtype == "register":
                client = register(msg.get("id"), ws)
                continue

            if client is None:
                send_raw(ws, {"type": "error", "error": "register first"})
                continue

            # relay message
            to = msg.get("to")
            if to:
                msg["from"] = client.id

                if not relay(to, msg):
                    send(client, {
                        "type": "error",
                        "error": f"peer {to} not connected"
                    })

    finally:
        unregister(client)


# ---------------- DASHBOARD ----------------
@app.route("/")
def index():
    with peers_lock:
        ids = sorted(peers.keys())

    rows = "".join(
        f"<div style='padding:6px;border-bottom:1px solid #1f2a36'>{cid}</div>"
        for cid in ids
    ) or "<div>No active connections</div>"

    ws_url = f"ws://{request.host}/ws"

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Secure Signaling Server</title>
    </head>
    <body style="font-family:Arial;background:#0b0f14;color:#e6edf3">
        <div style="max-width:800px;margin:auto;padding:20px">

            <h2>Secure Signaling Server</h2>

            <div style="background:#111a24;padding:10px;border-radius:8px;margin-bottom:15px">
                <b>WebSocket URL:</b><br>
                <code>{ws_url}</code>
            </div>

            <div style="background:#111a24;padding:10px;border-radius:8px">
                <b>Connected Peers</b>
                {rows}
            </div>

        </div>
    </body>
    </html>
    """


# ---------------- MAIN ----------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    log.info("starting server on 0.0.0.0:%s", port)
    app.run(host="0.0.0.0", port=port, threaded=True)
