"""
SecureComm local signaling server (for testing).

A *blind relay*: it never sees media, plaintext, or session keys. It only forwards
signaling JSON between two connected peers, routed by a `to` field. This is exactly
what WebRTC needs to exchange SDP offer/answer + ICE candidates so two phones can
find each other; the actual audio (and its encryption) never touches this server.

Protocol (raw WebSocket, one JSON object per message):
  Client -> server, to register who you are:
      {"type": "register", "id": "alice"}
    (or connect with the id in the URL query: ws://HOST:8080/ws?id=alice)

  Client -> server, to reach another peer (server adds "from" and forwards verbatim):
      {"type": "peer-key", "to": "bob", "publicKey": "<base64 X25519>"}
      {"type": "offer",    "to": "bob", "sdp": "<sdp>"}
      {"type": "answer",   "to": "bob", "sdp": "<sdp>"}
      {"type": "ice",      "to": "bob", "candidate": "...", "sdpMid": "...", "sdpMLineIndex": 0}

  Server -> client (informational):
      {"type": "registered", "id": "alice", "peers": ["bob"]}
      {"type": "presence", "id": "bob", "online": true}
      {"type": "error", "error": "peer bob not connected"}

Run:
    pip install -r requirements.txt
    python signaling_server.py
Then point the app at  ws://<your-PC-LAN-IP>:8080/ws
"""

import json
import logging
import threading

from flask import Flask, request
from flask_sock import Sock

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("signaling")

app = Flask(__name__)
# Keep WebSockets open: no ping timeout cap for relaxed local testing.
app.config["SOCK_SERVER_OPTIONS"] = {"ping_interval": 25}
sock = Sock(app)


class Client:
    __slots__ = ("id", "ws", "lock")

    def __init__(self, client_id, ws):
        self.id = client_id
        self.ws = ws
        self.lock = threading.Lock()  # serialize sends to this socket


peers = {}                 # id -> Client
peers_lock = threading.Lock()


def _send(client, obj):
    """Thread-safe send of a JSON object to one client."""
    try:
        with client.lock:
            client.ws.send(json.dumps(obj))
        return True
    except Exception as e:
        log.warning("send to %s failed: %s", client.id, e)
        return False


def _broadcast_presence(client_id, online):
    with peers_lock:
        others = [c for cid, c in peers.items() if cid != client_id]
    for c in others:
        _send(c, {"type": "presence", "id": client_id, "online": online})


def register(client_id, ws):
    if not client_id:
        return None
    client = Client(client_id, ws)
    with peers_lock:
        old = peers.get(client_id)
        peers[client_id] = client
        current_peers = [cid for cid in peers if cid != client_id]
    if old is not None:
        # A second connection with the same id replaces the first.
        try:
            old.ws.close()
        except Exception:
            pass
        log.info("re-registered %-12s (replaced previous connection)", client_id)
    else:
        log.info("registered   %-12s  online now: %s", client_id, ", ".join(current_peers) or "(only this one)")
    _send(client, {"type": "registered", "id": client_id, "peers": current_peers})
    _broadcast_presence(client_id, True)
    return client


def unregister(client):
    with peers_lock:
        if peers.get(client.id) is client:
            del peers[client.id]
            removed = True
        else:
            removed = False
    if removed:
        log.info("disconnected %s", client.id)
        _broadcast_presence(client.id, False)


def relay(target_id, message):
    with peers_lock:
        target = peers.get(target_id)
    if target is None:
        return False
    return _send(target, message)


@sock.route("/ws")
def ws_handler(ws):
    # Optional id in the URL query: ws://host:8080/ws?id=alice
    my_id = request.args.get("id")
    client = register(my_id, ws) if my_id else None

    try:
        while True:
            raw = ws.receive()           # blocks; returns None when the client disconnects
            if raw is None:
                break
            try:
                msg = json.loads(raw)
            except (ValueError, TypeError):
                log.warning("ignored non-JSON frame: %r", raw[:120])
                continue

            mtype = msg.get("type")

            if mtype == "register":
                client = register(msg.get("id"), ws)
                continue

            if client is None:
                _send_raw(ws, {"type": "error", "error": "register first (send {\"type\":\"register\",\"id\":\"...\"})"})
                continue

            to = msg.get("to")
            if to:
                msg["from"] = client.id
                if relay(to, msg):
                    log.info("relay  %-9s %-12s -> %-12s", mtype, client.id, to)
                else:
                    log.info("relay  %-9s %-12s -> %-12s  [peer offline]", mtype, client.id, to)
                    _send(client, {"type": "error", "error": f"peer {to} not connected", "for": mtype})
            else:
                log.info("message with no 'to' from %s: %s", client.id, mtype)
    finally:
        if client is not None:
            unregister(client)


def _send_raw(ws, obj):
    try:
        ws.send(json.dumps(obj))
    except Exception:
        pass


@app.route("/")
def index():
    with peers_lock:
        ids = sorted(peers.keys())
    rows = "".join(f"<li><code>{cid}</code></li>" for cid in ids) or "<li><em>none connected</em></li>"
    return f"""
    <html><head><title>SecureComm signaling</title>
    <style>body{{font-family:system-ui;background:#0E1419;color:#E3E8EC;padding:32px}}
    code{{color:#00BFA6}} a{{color:#5C9DFF}}</style></head>
    <body>
      <h2>SecureComm signaling server — running</h2>
      <p>WebSocket endpoint: <code>ws://{request.host}/ws</code></p>
      <p>Connected peers ({len(ids)}):</p>
      <ul>{rows}</ul>
      <p>Open the <a href="/test">browser test client</a> in two tabs to verify relaying.</p>
    </body></html>
    """


@app.route("/test")
def test_page():
    # Served inline so there are no file-path issues.
    return TEST_HTML


TEST_HTML = r"""
<!doctype html><html><head><meta charset="utf-8"><title>SecureComm signaling tester</title>
<style>
 body{font-family:system-ui;background:#0E1419;color:#E3E8EC;margin:0;padding:24px;max-width:760px}
 h2{color:#00BFA6} input,button,textarea{font-size:14px;padding:8px;border-radius:8px;border:1px solid #3A4651;background:#161D24;color:#E3E8EC}
 input{width:160px} textarea{width:100%;height:90px;box-sizing:border-box;font-family:monospace}
 button{background:#00897B;border:none;cursor:pointer;margin-left:4px}
 #log{background:#0a0f13;border:1px solid #233;border-radius:8px;padding:12px;height:260px;overflow:auto;font-family:monospace;font-size:12px;white-space:pre-wrap}
 .row{margin:10px 0} label{color:#B0BAC4;margin-right:6px}
</style></head><body>
<h2>SecureComm signaling tester</h2>
<div class="row">
  <label>Server</label><input id="url" value="" style="width:280px">
  <label>My id</label><input id="me" value="alice">
  <button onclick="connect()">Connect</button>
  <span id="state">disconnected</span>
</div>
<div class="row">
  <label>Send to</label><input id="to" value="bob">
  <label>type</label><input id="type" value="offer" style="width:90px">
  <button onclick="sendMsg()">Send test message</button>
</div>
<div class="row"><textarea id="payload">{"sdp":"hello from alice"}</textarea></div>
<div class="row"><div id="log"></div></div>
<script>
 let ws=null;
 document.getElementById('url').value = "ws://" + location.host + "/ws";
 function logLine(s){const l=document.getElementById('log');l.textContent+=s+"\n";l.scrollTop=l.scrollHeight;}
 function connect(){
   const url=document.getElementById('url').value, me=document.getElementById('me').value;
   ws=new WebSocket(url);
   ws.onopen=()=>{document.getElementById('state').textContent='connected';
     ws.send(JSON.stringify({type:'register',id:me}));logLine('-> register '+me);};
   ws.onmessage=e=>logLine('<- '+e.data);
   ws.onclose=()=>{document.getElementById('state').textContent='disconnected';logLine('* closed');};
   ws.onerror=()=>logLine('* error (is the server running? same host/port?)');
 }
 function sendMsg(){
   if(!ws||ws.readyState!==1){logLine('* not connected');return;}
   let body={}; try{body=JSON.parse(document.getElementById('payload').value);}catch(e){logLine('* payload not JSON');return;}
   const msg=Object.assign({type:document.getElementById('type').value,to:document.getElementById('to').value},body);
   ws.send(JSON.stringify(msg));logLine('-> '+JSON.stringify(msg));
 }
</script></body></html>
"""


if __name__ == "__main__":
    host, port = "0.0.0.0", 8080
    log.info("SecureComm signaling on ws://%s:%d/ws   (status page: http://localhost:%d/)", host, port, port)
    log.info("Point the app at  ws://<your-PC-LAN-IP>:%d/ws", port)
    # threaded=True so each WebSocket connection gets its own thread.
    app.run(host=host, port=port, threaded=True)
