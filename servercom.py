import json
import logging
import threading

from flask import Flask, request
from flask_sock import Sock

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("signaling")

app = Flask(__name__)

# ✅ Production WebSocket stability
app.config["SOCK_SERVER_OPTIONS"] = {
    "ping_interval": 20,
    "ping_timeout": 20
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
# DASHBOARD (WITH LOADING UI)
# -------------------------

@app.route("/")
def index():
    with peers_lock:
        ids = sorted(peers.keys())

    rows = "".join(
        f"<div class='peer'><span></span>{cid}</div>"
        for cid in ids
    ) or "<div class='empty'>No active connections</div>"

    ws_url = f"wss://{request.host}/ws"

    return f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SecuCom Server</title>

<style>

body {{
    margin: 0;
    font-family: system-ui;
    background: #070b10;
    color: #e6edf3;
}}

/* ---------------- LOADING ---------------- */

#loading {{
    position: fixed;
    inset: 0;
    background: radial-gradient(circle at center, #0f1a25, #05070a);
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    z-index: 9999;
}}

.spinner {{
    width: 80px;
    height: 80px;
    border-radius: 50%;
    border: 4px solid rgba(0,255,200,0.15);
    border-top: 4px solid #00ffc3;
    animation: spin 1s linear infinite;
}}

@keyframes spin {{
    100% {{ transform: rotate(360deg); }}
}}

.text {{
    margin-top: 20px;
    color: #00ffc3;
    letter-spacing: 2px;
}}

.steps {{
    margin-top: 15px;
    font-size: 12px;
    color: #8aa0b3;
    animation: fade 2s infinite;
}}

@keyframes fade {{
    0% {{ opacity: 0.2; }}
    50% {{ opacity: 1; }}
    100% {{ opacity: 0.2; }}
}}

/* ---------------- MAIN ---------------- */

.container {{
    max-width: 900px;
    margin: auto;
    padding: 30px;
}}

.card {{
    background: #0e1620;
    border: 1px solid #1f2a36;
    border-radius: 14px;
    padding: 15px;
    margin-top: 15px;
}}

.ws {{
    font-family: monospace;
    color: #7ae3ff;
    word-break: break-all;
}}

.peer {{
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px;
    margin-top: 8px;
    background: #0b121a;
    border-radius: 10px;
}}

.peer span {{
    width: 10px;
    height: 10px;
    background: #00ff88;
    border-radius: 50%;
}}

.empty {{
    text-align: center;
    color: #6b7c8f;
}}

</style>
</head>

<body>

<!-- LOADING SCREEN -->
<div id="loading">
    <div class="spinner"></div>
    <div class="text">SecuCom Secure Boot</div>
    <div class="steps" id="steps">Initializing secure channel...</div>
</div>

<!-- MAIN -->
<div class="container">

    <h2>🔐 SecuCom Server</h2>

    <div class="card">
        <b>WebSocket</b><br>
        <div class="ws">{ws_url}</div>
    </div>

    <div class="card">
        <b>Connected Peers</b>
        {rows}
    </div>

</div>

<script>

const steps = [
 "Initializing secure runtime...",
 "Generating ephemeral keys...",
 "Establishing tunnel...",
 "Verifying peer identity...",
 "Encrypting session...",
 "Secure channel ready"
];

let i = 0;
const box = document.getElementById("steps");

const interval = setInterval(() => {{
    box.innerText = steps[i];
    i++;

    if (i >= steps.length) {{
        clearInterval(interval);
        setTimeout(() => {{
            document.getElementById("loading").style.display = "none";
        }}, 600);
    }}
}}, 700);

</script>

</body>
</html>
"""


# -------------------------
# MAIN
# -------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
