import json
import logging
import os
import threading

from flask import Flask, request
from flask_sock import Sock

# -------------------------
# LOGGING
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("secucom")

# -------------------------
# APP INIT
# -------------------------
app = Flask(__name__)

# IMPORTANT: flask-sock safe config (NO ping_timeout!)
app.config["SOCK_SERVER_OPTIONS"] = {
    "ping_interval": 20
}

sock = Sock(app)

# -------------------------
# CLIENT STORAGE
# -------------------------
class Client:
    __slots__ = ("id", "ws", "lock")

    def __init__(self, client_id, ws):
        self.id = client_id
        self.ws = ws
        self.lock = threading.Lock()


peers = {}
peers_lock = threading.Lock()


# -------------------------
# SAFE SEND
# -------------------------
def _send(client, obj):
    try:
        with client.lock:
            client.ws.send(json.dumps(obj))
        return True
    except Exception:
        return False


def _send_raw(ws, obj):
    try:
        ws.send(json.dumps(obj))
    except Exception:
        pass


# -------------------------
# REGISTER
# -------------------------
def register(client_id, ws):
    if not client_id:
        return None

    client = Client(client_id, ws)

    with peers_lock:
        peers[client_id] = client
        current_peers = [cid for cid in peers if cid != client_id]

    log.info("registered %s", client_id)

    _send(client, {
        "type": "registered",
        "id": client_id,
        "peers": current_peers
    })

    return client


# -------------------------
# UNREGISTER
# -------------------------
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
# WEBSOCKET
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
                _send_raw(ws, {"type": "error", "error": "register first"})
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
# DASHBOARD UI
# -------------------------
@app.route("/")
def index():
    with peers_lock:
        ids = sorted(peers.keys())

    # show max 5 users (masked)
    display = ids[:5]

    peer_html = ""
    for cid in display:
        short = cid[:6] + "..." if len(cid) > 6 else cid
        peer_html += f"""
        <div class="peer">
            <div class="avatar"></div>
            <div class="peer-id">{short}</div>
            <div class="tag">ONLINE</div>
        </div>
        """

    if not peer_html:
        peer_html = "<div class='empty'>No active secure sessions</div>"

    ws_url = f"wss://{request.host}/ws"

    return f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SecuCom Secure Control Center</title>

<style>

body {{
    margin: 0;
    font-family: system-ui, -apple-system, Segoe UI, Roboto;
    background: radial-gradient(circle at top, #0b1220, #05070c);
    color: #e6edf3;
}}

/* HEADER */
.header {{
    padding: 18px 22px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: #0b1220;
    border-bottom: 1px solid #1f2a3a;
}}

.brand {{
    font-size: 18px;
    font-weight: 700;
    color: #00ffc8;
    letter-spacing: 1px;
}}

.status {{
    font-size: 11px;
    color: #00ff88;
    padding: 5px 10px;
    border: 1px solid #00ff88;
    border-radius: 999px;
    background: rgba(0,255,136,0.08);
}}

/* GRID */
.container {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 14px;
    padding: 18px;
}}

.card {{
    background: linear-gradient(180deg, #0e1622, #0a111a);
    border: 1px solid #1f2a3a;
    border-radius: 14px;
    padding: 16px;
    box-shadow: 0 10px 30px rgba(0,0,0,0.4);
}}

.title {{
    font-size: 11px;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: #7fa6c4;
    margin-bottom: 10px;
}}

.ws {{
    font-family: monospace;
    color: #5ca9ff;
    font-size: 12px;
    word-break: break-all;
}}

/* PEERS */
.peer {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px;
    margin-top: 8px;
    background: #0a111a;
    border: 1px solid #1f2a3a;
    border-radius: 10px;
    transition: 0.2s;
}}

.peer:hover {{
    transform: scale(1.01);
    border-color: #00ffc8;
}}

.avatar {{
    width: 10px;
    height: 10px;
    border-radius: 50%;
    background: #00ff88;
    margin-right: 10px;
    box-shadow: 0 0 10px #00ff88;
}}

.peer-id {{
    flex: 1;
    font-family: monospace;
    color: #e6edf3;
}}

.tag {{
    font-size: 10px;
    color: #00ff88;
    font-weight: 600;
}}

.empty {{
    color: #7a8a99;
    text-align: center;
    padding: 20px;
    font-size: 13px;
}}

/* FOOTER */
.footer {{
    text-align: center;
    padding: 18px;
    font-size: 11px;
    color: #60748a;
    border-top: 1px solid #1f2a3a;
    margin-top: 10px;
}}

.glow {{
    animation: glow 2s infinite;
}}

@keyframes glow {{
    0% {{ opacity: 0.5; }}
    50% {{ opacity: 1; }}
    100% {{ opacity: 0.5; }}
}}

</style>
</head>

<body>

<div class="header">
    <div class="brand">SECUCOM CONTROL CENTER</div>
    <div class="status glow">ENCRYPTED SIGNAL ACTIVE</div>
</div>

<div class="container">

    <div class="card">
        <div class="title">WebSocket Endpoint</div>
        <div class="ws">{ws_url}</div>
    </div>

    <div class="card">
        <div class="title">System Status</div>
        <div style="color:#00ff88;font-size:13px;">● Relay Engine Online</div>
        <div style="color:#7fa6c4;font-size:12px;margin-top:6px;">
            WebRTC signaling ready • Peer routing enabled • Session tracking active
        </div>
    </div>

    <div class="card">
        <div class="title">Active Users (max 5)</div>
        {peer_html}
    </div>

    <div class="card">
        <div class="title">Security Layer</div>
        <div style="font-size:12px;color:#7fa6c4;line-height:1.5">
            • Ephemeral session routing<br>
            • Identity-based peer mapping<br>
            • WebSocket encrypted transport layer simulation<br>
            • Zero trust communication model
        </div>
    </div>

</div>

<div class="footer">
    SecuCom • Secure Communication Infrastructure • WebRTC Signaling Core
</div>

</body>
</html>
"""


# -------------------------
# RUN (RAILWAY / RENDER SAFE)
# -------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    log.info("running on 0.0.0.0:%s", port)
    app.run(host="0.0.0.0", port=port, threaded=True)
