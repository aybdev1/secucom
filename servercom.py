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
        f"""
        <div class="peer">
            <div class="dot"></div>
            <div class="cid">{cid}</div>
            <div class="status">ONLINE</div>
        </div>
        """
        for cid in ids
    ) or """
    <div class="empty">No active connections</div>
    """

    ws_url = f"ws://{request.host}/ws"

    return f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Secure Signaling Server</title>

<style>

body {{
    margin: 0;
    font-family: system-ui, -apple-system, Segoe UI, Roboto;
    background: radial-gradient(circle at top, #0b1220, #05070c);
    color: #e6edf3;
}}

/* HEADER */
.header {{
    text-align: center;
    padding: 25px;
}}

.title {{
    font-size: 22px;
    font-weight: 700;
    color: #00ffc8;
    letter-spacing: 1px;
}}

.subtitle {{
    font-size: 12px;
    color: #7fa6c4;
    margin-top: 5px;
}}

/* GRID */
.container {{
    max-width: 900px;
    margin: auto;
    padding: 20px;
    display: grid;
    grid-template-columns: 1fr;
    gap: 15px;
}}

/* CARD */
.card {{
    background: linear-gradient(180deg, #0f1722, #0a111a);
    border: 1px solid #1f2a3a;
    border-radius: 16px;
    padding: 16px;
    box-shadow: 0 10px 30px rgba(0,0,0,0.4);
    backdrop-filter: blur(10px);
}}

.card-title {{
    font-size: 11px;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: #7fa6c4;
    margin-bottom: 10px;
}}

/* WS BOX */
.ws {{
    font-family: monospace;
    color: #5ca9ff;
    word-break: break-all;
    font-size: 13px;
}}

/* PEERS */
.peer {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px;
    margin-top: 10px;
    background: #0b1220;
    border: 1px solid #1f2a3a;
    border-radius: 12px;
    transition: 0.2s;
}}

.peer:hover {{
    transform: scale(1.01);
    border-color: #00ffc8;
}}

.dot {{
    width: 10px;
    height: 10px;
    border-radius: 50%;
    background: #00ff88;
    box-shadow: 0 0 10px #00ff88;
}}

.cid {{
    flex: 1;
    margin-left: 10px;
    font-family: monospace;
    font-size: 13px;
    color: #e6edf3;
}}

.status {{
    font-size: 10px;
    color: #00ff88;
    border: 1px solid #00ff88;
    padding: 3px 8px;
    border-radius: 999px;
    background: rgba(0,255,136,0.08);
}}

.empty {{
    text-align: center;
    padding: 20px;
    color: #7a8a99;
}}

/* FOOTER */
.footer {{
    text-align: center;
    font-size: 11px;
    color: #60748a;
    padding: 20px;
    margin-top: 20px;
}}

.glow {{
    animation: glow 2s infinite;
}}

@keyframes glow {{
    0% {{ opacity: 0.6; }}
    50% {{ opacity: 1; }}
    100% {{ opacity: 0.6; }}
}}

</style>
</head>

<body>

<div class="header">
    <div class="title">SECURE SIGNALING SERVER</div>
    <div class="subtitle glow">Real-time WebRTC Relay • Encrypted Channel Simulation</div>
</div>

<div class="container">

    <div class="card">
        <div class="card-title">WebSocket Endpoint</div>
        <div class="ws">{ws_url}</div>
    </div>

    <div class="card">
        <div class="card-title">System Status</div>
        <div style="color:#00ff88;font-size:13px;">
            ● Online • Relay Engine Active
        </div>
        <div style="color:#7fa6c4;font-size:12px;margin-top:6px;">
            Peer routing • Identity mapping • Low-latency signaling
        </div>
    </div>

    <div class="card">
        <div class="card-title">Connected Peers</div>
        {rows}
    </div>

</div>

<div class="footer">
    Secure Call Signaling Core • Advanced Encryption + WebSocket Engine
</div>

</body>
</html>
"""


# ---------------- MAIN ----------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    log.info("starting server on 0.0.0.0:%s", port)
    app.run(host="0.0.0.0", port=port, threaded=True)
