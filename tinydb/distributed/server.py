import http.server
import json
import threading
import urllib.parse
import urllib.request
from tinydb import TinyDB, Query
from tinydb.table import Document
from .replication import ReplicationManager, HeartbeatDaemon

db_lock = threading.Lock()

def build_query_from_hash(hashval):
    if hashval is None:
        return None
    val_type = hashval.get("type")
    val_data = hashval.get("data")
    if val_type == "scalar":
        return val_data
    elif val_type == "list":
        return [build_query_from_hash(x) for x in val_data]
    elif val_type == "dict":
        return {k: build_query_from_hash(v) for k, v in val_data.items()}
    elif val_type == "frozenset":
        return frozenset([build_query_from_hash(x) for x in val_data])
    elif val_type == "tuple":
        t = tuple([build_query_from_hash(x) for x in val_data])
        if len(t) >= 1:
            op = t[0]
            if op in ('==', '!=', '<', '<=', '>', '>='):
                path, val = t[1], t[2]
                q = Query()
                for p in path: q = q[p]
                if op == '==': return q == val
                if op == '!=': return q != val
                if op == '<': return q < val
                if op == '<=': return q <= val
                if op == '>': return q > val
                if op == '>=': return q >= val
            elif op == 'exists':
                q = Query()
                for p in t[1]: q = q[p]
                return q.exists()
            elif op == 'and':
                sub = list(t[1])
                return build_query_from_hash(sub[0]) & build_query_from_hash(sub[1])
            elif op == 'or':
                sub = list(t[1])
                return build_query_from_hash(sub[0]) | build_query_from_hash(sub[1])
            elif op == 'not':
                return ~build_query_from_hash(t[1])
        return t
    return None

class TinyDBRequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args): pass
    def send_json(self, data, status_code=200):
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if self.server.node.is_crashed and parsed.path == "/api/heartbeat":
            self.send_error(503)
            return
        if parsed.path in ("/", "/dashboard"):
            self.serve_dashboard()
        elif parsed.path == "/api/heartbeat":
            self.send_json({"status": "ok", "role": self.server.node.role})
        elif parsed.path == "/api/dashboard_data":
            self.serve_dashboard_data()
        else:
            self.send_error(404)

    def do_POST(self):
        if self.server.node.is_crashed and not (self.path.startswith("/api/simulate") or self.path.startswith("/api/promote")):
            self.send_error(503)
            return
        content_length = int(self.headers.get('Content-Length', 0))
        data = json.loads(self.rfile.read(content_length).decode("utf-8")) if content_length else {}

        if self.path == "/api/query":
            self.handle_query(data)
        elif self.path == "/api/replicate":
            self.handle_replicate(data)
        elif self.path == "/api/promote":
            self.handle_promote(data)
        elif self.path == "/api/register":
            if self.server.node.role == 'PRIMARY':
                self.server.node.replication_manager.register_backup(data.get("backup_url"))
                self.send_json({"success": True})
            else:
                self.send_json({"success": False, "error": "Not Primary"}, 400)
        elif self.path == "/api/simulate_crash":
            self.server.node.is_crashed = True
            self.server.node.add_log("Node CRASHED")
            self.send_json({"success": True})
        elif self.path == "/api/simulate_revive":
            self.server.node.is_crashed = False
            self.server.node.add_log("Node REVIVED")
            self.send_json({"success": True})

    def handle_query(self, data):
        action = data.get("action")
        table_name = data.get("table", "_default")
        if action in ("insert", "update", "remove", "truncate") and self.server.node.role == 'BACKUP':
            self.send_json({"success": False, "error": "Read-only Backup node"}, 400)
            return
        with db_lock:
            try:
                table = self.server.node.db.table(table_name)
                cond = build_query_from_hash(data.get("query"))
                res = None
                if action == "insert":
                    res = table.insert(data.get("doc"))
                    self.server.node.replication_manager.replicate("insert", table_name, {"doc": data.get("doc")})
                elif action == "all":
                    res = [{"doc_id": d.doc_id, "value": dict(d)} for d in table.all()]
                elif action == "search":
                    res = [{"doc_id": d.doc_id, "value": dict(d)} for d in table.search(cond)]
                elif action == "update":
                    res = table.update(data.get("fields"), cond=cond, doc_ids=data.get("doc_ids"))
                    self.server.node.replication_manager.replicate("update", table_name, {"fields": data.get("fields"), "query": data.get("query"), "doc_ids": data.get("doc_ids")})
                elif action == "remove":
                    res = table.remove(cond=cond, doc_ids=data.get("doc_ids"))
                    self.server.node.replication_manager.replicate("remove", table_name, {"query": data.get("query"), "doc_ids": data.get("doc_ids")})
                elif action == "len":
                    res = len(table)
                self.send_json({"success": True, "result": res})
            except Exception as e:
                self.send_json({"success": False, "error": str(e)}, 500)

    def handle_replicate(self, data):
        action = data.get("action")
        table_name = data.get("table")
        payload = data.get("payload", {})
        with db_lock:
            try:
                table = self.server.node.db.table(table_name)
                cond = build_query_from_hash(payload.get("query"))
                if action == "insert":
                    table.insert(payload.get("doc"))
                elif action == "update":
                    table.update(payload.get("fields"), cond=cond, doc_ids=payload.get("doc_ids"))
                elif action == "remove":
                    table.remove(cond=cond, doc_ids=payload.get("doc_ids"))
                self.server.node.add_log(f"Replicated {action} on '{table_name}'")
                self.send_json({"success": True})
            except Exception as e:
                self.send_json({"success": False, "error": str(e)}, 500)

    def handle_promote(self, data):
        new_url = data.get("new_primary_url")
        if self.server.node.my_url == new_url:
            self.server.node.promote_to_primary()
        else:
            self.server.node.role = 'BACKUP'
            self.server.node.primary_url = new_url
            self.server.node.add_log(f"Chuyển Primary sang: {new_url}")
        self.send_json({"success": True})

    def serve_dashboard(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>TinyDB Cluster Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style>
        body {{ font-family: 'Outfit', sans-serif; background: #0f172a; color: #f8fafc; padding: 20px; }}
        .container {{ max-width: 1000px; margin: auto; }}
        header {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #334155; padding-bottom: 10px; margin-bottom: 20px; }}
        .badge {{ padding: 5px 10px; border-radius: 5px; font-size: 0.8rem; font-weight: bold; text-transform: uppercase; }}
        .PRIMARY {{ background: #86198f; color: #fdf4ff; }}
        .BACKUP {{ background: #1e3a8a; color: #eff6ff; }}
        .ONLINE {{ background: #166534; color: #f0fdf4; }}
        .CRASHED {{ background: #991b1b; color: #fef2f2; }}
        .panel {{ background: #1e293b; border-radius: 10px; padding: 20px; margin-bottom: 20px; border: 1px solid #334155; }}
        .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
        .node-list {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px; }}
        .node-card {{ background: #0f172a; border-radius: 8px; padding: 15px; border: 1px solid #334155; }}
        .btn {{ cursor: pointer; padding: 5px 10px; border: none; border-radius: 5px; font-weight: bold; font-size: 0.8rem; margin-top: 5px; }}
        .btn-crash {{ background: #ef4444; color: white; }}
        .btn-revive {{ background: #22c55e; color: white; }}
        .btn-promote {{ background: #a855f7; color: white; width: 100%; margin-top: 10px; }}
        .logs {{ background: #020617; font-family: monospace; height: 200px; overflow-y: auto; padding: 10px; border-radius: 5px; font-size: 0.85rem; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 0.9rem; }}
        th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #334155; }}
        th {{ background: #0f172a; }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h2>TinyDB Distributed Console</h2>
            <div><span class="badge {self.server.node.role}">{self.server.node.role}</span> {self.server.node.my_url}</div>
        </header>
        <div class="panel">
            <h3>Cluster Node Map</h3>
            <div class="node-list" id="nodes"></div>
        </div>
        <div class="grid">
            <div class="panel">
                <h3>Database Records</h3>
                <table>
                    <thead><tr><th>ID</th><th>Data</th></tr></thead>
                    <tbody id="records"></tbody>
                </table>
            </div>
            <div class="panel">
                <h3>Event Logs</h3>
                <div class="logs" id="logs"></div>
            </div>
        </div>
    </div>
    <script>
        async function update() {{
            try {{
                const res = await fetch("/api/dashboard_data");
                const data = await res.json();
                
                document.getElementById("nodes").innerHTML = data.cluster_status.map(n => `
                    <div class="node-card">
                        <div style="display:flex;justify-content:space-between;align-items:center;">
                            <strong>${{n.url}}</strong>
                            <span class="badge ${{n.is_crashed ? 'CRASHED' : 'ONLINE'}}">${{n.is_crashed ? 'CRASHED' : 'ONLINE'}}</span>
                        </div>
                        <div style="margin-top:10px;display:flex;justify-content:space-between;">
                            <span class="badge ${{n.role}}">${{n.role}}</span>
                            ${{n.url === '{self.server.node.my_url}' ? 
                                (n.is_crashed ? 
                                    `<button class="btn btn-revive" onclick="fetch('/api/simulate_revive',{{method:'POST'}})">Revive</button>` : 
                                    `<button class="btn btn-crash" onclick="fetch('/api/simulate_crash',{{method:'POST'}})">Crash</button>`) : ''}}
                        </div>
                        ${{n.role === 'BACKUP' && !n.is_crashed && !data.cluster_status.find(x => x.url === '{self.server.node.my_url}').is_crashed ? 
                            `<button class="btn btn-promote" onclick="promote('${{n.url}}')">Promote to Primary</button>` : ''}}
                    </div>
                `).join("");
                
                document.getElementById("records").innerHTML = data.records.map(r => `
                    <tr><td>${{r.doc_id}}</td><td>${{JSON.stringify(r.value)}}</td></tr>
                `).join("");
                
                document.getElementById("logs").innerHTML = data.logs.map(l => `<div>${{l}}</div>`).join("");
            }} catch(e) {{}}
        }}
        async function promote(url) {{
            await fetch(url + "/api/promote", {{method: 'POST', body: JSON.stringify({{new_primary_url: url}})}});
        }}
        setInterval(update, 1000);
        update();
    </script>
</body>
</html>
"""
        self.wfile.write(html.encode("utf-8"))

    def serve_dashboard_data(self):
        status = []
        for url in self.server.node.cluster_nodes:
            is_crashed, online, role = False, False, "UNKNOWN"
            if url == self.server.node.my_url:
                online, is_crashed, role = True, self.server.node.is_crashed, self.server.node.role
            else:
                try:
                    with urllib.request.urlopen(f"{url.rstrip('/')}/api/heartbeat", timeout=0.5) as r:
                        online = True
                        role = json.loads(r.read().decode("utf-8")).get("role", "UNKNOWN")
                except:
                    is_crashed = True
            status.append({"url": url, "online": online, "is_crashed": is_crashed, "role": role})
            
        with db_lock:
            try:
                table = self.server.node.db.table("_default")
                records = [{"doc_id": d.doc_id, "value": dict(d)} for d in table.all()]
            except:
                records = []
                
        self.send_json({
            "role": self.server.node.role,
            "cluster_status": status,
            "logs": self.server.node.get_logs(),
            "records": records
        })

class TinyDBServer:
    def __init__(self, db_path, host="127.0.0.1", port=5000, role="PRIMARY", primary_url=None, cluster_nodes=None):
        self.db_path = db_path
        self.host = host
        self.port = port
        self.my_url = f"http://{host}:{port}"
        self.role = role
        self.primary_url = primary_url
        self.cluster_nodes = cluster_nodes if cluster_nodes else [self.my_url]
        self.is_crashed = False
        self._logs = []
        self._logs_lock = threading.Lock()
        self.db = TinyDB(db_path)
        self.replication_manager = ReplicationManager(self)
        self.heartbeat_daemon = HeartbeatDaemon(self)
        self.server = None
        self.add_log(f"Node initialized on {self.my_url} as {self.role}")

    def add_log(self, msg):
        import datetime
        t = datetime.datetime.now().strftime("%H:%M:%S")
        with self._logs_lock:
            self._logs.append(f"[{t}] {msg}")
            if len(self._logs) > 30: self._logs.pop(0)

    def get_logs(self):
        with self._logs_lock: return list(self._logs)

    def promote_to_primary(self):
        if self.role == 'PRIMARY': return
        self.role = 'PRIMARY'
        self.primary_url = None
        self.heartbeat_daemon.stop()
        self.add_log("Promoted to PRIMARY!")
        
        for node in self.cluster_nodes:
            if node != self.my_url:
                try:
                    req = urllib.request.Request(
                        f"{node}/api/promote",
                        data=json.dumps({"new_primary_url": self.my_url}).encode("utf-8"),
                        headers={"Content-Type": "application/json"}
                    )
                    urllib.request.urlopen(req, timeout=0.5)
                except: pass

    def start(self):
        self.server = http.server.ThreadingHTTPServer((self.host, self.port), TinyDBRequestHandler)
        self.server.node = self
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.add_log(f"HTTP Server started on {self.my_url}")
        
        if self.role == 'BACKUP' and self.primary_url:
            self.heartbeat_daemon.start()
            self._register()

    def _register(self):
        try:
            req = urllib.request.Request(
                f"{self.primary_url}/api/register",
                data=json.dumps({"backup_url": self.my_url}).encode("utf-8"),
                headers={"Content-Type": "application/json"}
            )
            urllib.request.urlopen(req, timeout=1.0)
            self.add_log(f"Registered with primary: {self.primary_url}")
        except Exception as e:
            self.add_log(f"Registration failed: {e}")

    def stop(self):
        self.heartbeat_daemon.stop()
        if self.server:
            self.server.shutdown()
            self.server.server_close()
        self.db.close()