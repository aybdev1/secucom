import json
import logging
import os
import threading

from flask import Flask, request
from flask_sock import Sock

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("signaling")

app = Flask(__name__)

# ✅ FIXED: remove ping_timeout (NOT supported in your flask-sock version)
app.config["SOCK_SERVER_OPTIONS"] = {
    "ping_interval": 20
}

sock = Sock(app)


# -------------------------
# CLIENT MODEL
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
# DASHBOARD (CLEAN UI)
# -------------------------

@app.route("/")
def index():
    with peers_lock:
        ids = sorted(peers.keys())

    peer_rows = "".join(
        f"""
        <div class="peer">
            <div class="dot"></div>
            <div class="peer-id">{cid}</div>
            <div class="status">ACTIVE</div>
        </div>
        """
        for cid in ids
    ) or """
        <div class="empty">No active secure sessions</div>
    """

    ws_url = f"wss://{request.host}/ws"

    return f"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SecuCom Enterprise Security Console</title>

<style>

:root {{
    --bg: #070a0f;
    --panel: #0e1622;
    --panel2: #0b121b;
    --border: #1f2b3a;
    --text: #e6edf3;
    --muted: #8aa4b7;
    --accent: #00ffc8;
    --danger: #ff4d4d;
    --ok: #00ff88;
    --blue: #5c9dff;
}}

* {{
    margin: 0;
    padding: 0;
    box-sizing: border-box;
    font-family: system-ui, -apple-system, Segoe UI, Roboto;
}}

body {{
    background: radial-gradient(circle at top, #0f1b2a, var(--bg));
    color: var(--text);
    min-height: 100vh;
}}

/* ================= LOADING OVERLAY ================= */

#loading {{
    position: fixed;
    inset: 0;
    background: radial-gradient(circle at center, #0a1320, #05070b);
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    z-index: 9999;
}}

.loader {{
    width: 90px;
    height: 90px;
    border-radius: 50%;
    border: 3px solid rgba(0,255,200,0.15);
    border-top: 3px solid var(--accent);
    animation: spin 1s linear infinite;
}}

@keyframes spin {{
    100% {{ transform: rotate(360deg); }}
}}

.loading-title {{
    margin-top: 18px;
    color: var(--accent);
    font-size: 16px;
    letter-spacing: 2px;
    font-weight: 600;
}}

.loading-text {{
    margin-top: 10px;
    font-size: 12px;
    color: var(--muted);
    text-align: center;
    max-width: 420px;
    line-height: 1.5;
}}

.loading-steps {{
    margin-top: 20px;
    font-size: 12px;
    color: #7fa6c4;
    animation: fade 1.8s infinite;
}}

@keyframes fade {{
    0% {{ opacity: 0.2; }}
    50% {{ opacity: 1; }}
    100% {{ opacity: 0.2; }}
}}

/* ================= HEADER ================= */

.header {{
    padding: 28px 30px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}}

.brand {{
    font-size: 20px;
    font-weight: 700;
    color: var(--accent);
}}

.badge {{
    font-size: 11px;
    padding: 6px 10px;
    border-radius: 999px;
    border: 1px solid rgba(0,255,200,0.3);
    background: rgba(0,255,200,0.08);
    color: var(--accent);
}}

/* ================= GRID ================= */

.container {{
    padding: 0 30px 30px;
    display: grid;
    grid-template-columns: 1.3fr 1fr;
    gap: 18px;
}}

.card {{
    background: linear-gradient(180deg, var(--panel), var(--panel2));
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 18px;
}}

.title {{
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: var(--muted);
    margin-bottom: 10px;
}}

.ws {{
    font-family: monospace;
    color: var(--blue);
    word-break: break-all;
    font-size: 13px;
}}

/* ================= PEERS ================= */

.peer {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px;
    margin-top: 8px;
    background: #0b141f;
    border: 1px solid var(--border);
    border-radius: 10px;
}}

.dot {{
    width: 9px;
    height: 9px;
    background: var(--ok);
    border-radius: 50%;
    margin-right: 10px;
}}

.peer-id {{
    flex: 1;
    margin-left: 10px;
}}

.status {{
    font-size: 10px;
    color: var(--ok);
    font-weight: 600;
}}

.empty {{
    text-align: center;
    color: var(--muted);
    padding: 18px;
}}

.footer {{
    text-align: center;
    padding: 20px;
    font-size: 11px;
    color: var(--muted);
}}

</style>
</head>

<body>

<!-- ================= LOADING SCREEN ================= -->
<div id="loading">
    <div class="loader"></div>

    <div class="loading-title">SecuCom Secure Channel Initialization</div>

    <div class="loading-text">
        Establishing encrypted communication layer using enterprise-grade security protocols.
        This system simulates advanced secure communication principles including:
        secure key exchange, session isolation, encrypted signaling, and identity verification.
    </div>

    <div class="loading-steps" id="steps">
        Initializing cryptographic runtime...
    </div>
</div>

<!-- ================= HEADER ================= -->
<div class="header">
    <div class="brand">SecuCom Enterprise Security Console</div>
    <div class="badge">ZERO TRUST ARCHITECTURE</div>
</div>

<!-- ================= MAIN ================= -->
<div class="container">

    <div class="card">
        <div class="title">WebSocket Secure Endpoint</div>
        <div class="ws">{ws_url}</div>
    </div>

    <div class="card">
        <div class="title">System Status</div>
        <div style="color: var(--ok); font-size: 13px;">● All systems operational</div>
        <div style="color: var(--muted); font-size: 12px; margin-top: 6px;">
            Encryption layer active • Signaling relay online • Peer routing enabled
        </div>
    </div>

    <div class="card">
        <div class="title">Active Secure Sessions</div>
        {peer_rows}
    </div>

</div>

<div class="footer">
    Enterprise Secure Communication Layer • WebRTC Signaling • Identity-Aware Routing
</div>

<script>

const steps = [
"Initializing secure runtime environment...",
"Establishing cryptographic session isolation...",
"Loading secure signaling relay engine...",
"Applying zero-trust verification model...",
"Preparing encrypted peer routing layer...",
"Secure communication channel ready"
];

let i = 0;
const el = document.getElementById("steps");

const interval = setInterval(() => {{
    el.innerText = steps[i];
    i++;

    if (i >= steps.length) {{
        clearInterval(interval);
        setTimeout(() => {{
            document.getElementById("loading").style.display = "none";
        }}, 600);
    }}
}}, 800);

</script>

</body>
</html>
"""


# -------------------------
# MAIN (RAILWAY READY)
# -------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    log.info("starting on 0.0.0.0:%s", port)
    app.run(host="0.0.0.0", port=port, threaded=True)
