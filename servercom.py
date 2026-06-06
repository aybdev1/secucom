import json
import logging
import threading

from flask import Flask, request
from flask_sock import Sock

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("signaling")

app = Flask(__name__)

# ✅ REQUIRED for production WebSockets
app.config["SOCK_SERVER_OPTIONS"] = {
    "ping_interval": 20,
    "ping_timeout": 20
}

sock = Sock(app)


class Client:
    __slots__ = ("id", "ws", "lock")

    def __init__(self, client_id, ws):
        self.id = client_id
        self.ws = ws
        self.lock = threading.Lock()


peers = {}
peers_lock = threading.Lock()


# -------------------------
# SAFE SEND FUNCTIONS
# -------------------------

def _send(client, obj):
    try:
        with client.lock:
            client.ws.send(json.dumps(obj))
        return True
    except Exception as e:
        log.warning("send to %s failed: %s", getattr(client, "id", "?"), e)
        return False


def _send_raw(ws, obj):
    try:
        ws.send(json.dumps(obj))
    except Exception:
        pass


# -------------------------
# REGISTER / UNREGISTER
# -------------------------

def register(client_id, ws):
    if not client_id:
        return None

    client = Client(client_id, ws)

    with peers_lock:
        old = peers.get(client_id)
        peers[client_id] = client
        current_peers = [cid for cid in peers if cid != client_id]

    if old:
        try:
            old.ws.close()
        except Exception:
            pass
        log.info("re-registered %s", client_id)
    else:
        log.info("registered %s", client_id)

    _send(client, {
        "type": "registered",
        "id": client_id,
        "peers": current_peers
    })

    return client


def unregister(client):
    if not client:
        return

    with peers_lock:
        if peers.get(client.id) is client:
            del peers[client.id]
            log.info("disconnected %s", client.id)


# -------------------------
# RELAY
# -------------------------

def relay(target_id, message):
    with peers_lock:
        target = peers.get(target_id)

    if not target:
        return False

    return _send(target, message)


# -------------------------
# WEBSOCKET ROUTE
# -------------------------

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

            if mtype == "register":
                client = register(msg.get("id"), ws)
                continue

            if client is None:
                _send_raw(ws, {
                    "type": "error",
                    "error": "register first"
                })
                continue

            to = msg.get("to")

            if to:
                msg["from"] = client.id

                if not relay(to, msg):
                    _send(client, {
                        "type": "error",
                        "error": f"peer {to} not connected"
                    })

    finally:
        unregister(client)


# -------------------------
# STATUS PAGE
# -------------------------

@app.route("/")
def index():
    with peers_lock:
        ids = sorted(peers.keys())

    rows = "".join(f"<li>{i}</li>" for i in ids) or "<li>none</li>"

    return f"""
    <h2>SecureComm Server Running</h2>
    <p>WS: <code>ws://{request.host}/ws</code></p>
    <ul>{rows}</ul>
    """


# -------------------------
# MAIN (IMPORTANT)
# -------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
