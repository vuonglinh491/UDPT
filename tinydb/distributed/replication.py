import time
import threading
import urllib.request
import json

class ReplicationManager:
    def __init__(self, server_node):
        self.server = server_node
        self.backups = set()

    def register_backup(self, url):
        self.backups.add(url)
        self.server.add_log(f"Đã đăng ký backup: {url}")

    def replicate(self, action, table, payload):
        for backup_url in list(self.backups):
            threading.Thread(target=self._send, args=(backup_url, action, table, payload), daemon=True).start()

    def _send(self, backup_url, action, table, payload):
        url = f"{backup_url.rstrip('/')}/api/replicate"
        req = urllib.request.Request(
            url, 
            data=json.dumps({"action": action, "table": table, "payload": payload}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=1.5) as r:
                res = json.loads(r.read().decode("utf-8"))
                if res.get("success"):
                    self.server.add_log(f"Đồng bộ {action} thành công đến {backup_url}")
        except Exception as e:
            self.server.add_log(f"Lỗi đồng bộ đến {backup_url}: {e}")

class HeartbeatDaemon:
    def __init__(self, server_node):
        self.server = server_node
        self.running = False
        self.failed = 0

    def start(self):
        self.running = True
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self):
        self.running = False

    def _run(self):
        while self.running:
            if self.server.role == 'BACKUP' and self.server.primary_url:
                url = f"{self.server.primary_url.rstrip('/')}/api/heartbeat"
                try:
                    req = urllib.request.Request(url, method="GET")
                    with urllib.request.urlopen(req, timeout=1.0) as r:
                        if json.loads(r.read().decode("utf-8")).get("status") == "ok":
                            self.failed = 0
                except Exception:
                    self.failed += 1
                    self.server.add_log(f"Heartbeat lỗi ({self.failed}/3) tới {self.server.primary_url}")

                if self.failed >= 3:
                    self.server.add_log("Primary Offline! Bắt đầu bầu chọn leader mới...")
                    self._elect()
                    time.sleep(5)
            time.sleep(2)

    def _elect(self):
        my_url = self.server.my_url
        alive = []
        for node in self.server.cluster_nodes:
            if node == self.server.primary_url:
                continue
            if node == my_url:
                alive.append(node)
                continue
            try:
                with urllib.request.urlopen(f"{node.rstrip('/')}/api/heartbeat", timeout=0.8) as r:
                    if json.loads(r.read().decode("utf-8")).get("status") == "ok":
                        alive.append(node)
            except:
                pass
        alive.sort(key=lambda x: self.server.cluster_nodes.index(x))
        if alive and alive[0] == my_url:
            self.server.promote_to_primary()
