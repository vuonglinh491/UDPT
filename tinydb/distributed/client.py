import urllib.request
import urllib.parse
import json
from tinydb.table import Document

def serialize_query(query):
    if query is None or not hasattr(query, '_hash'):
        return None
    return serialize_hash_val(query._hash)

def serialize_hash_val(val):
    if val is None:
        return None
    if isinstance(val, tuple):
        return {"type": "tuple", "data": [serialize_hash_val(x) for x in val]}
    elif isinstance(val, frozenset):
        return {"type": "frozenset", "data": [serialize_hash_val(x) for x in val]}
    elif isinstance(val, list):
        return {"type": "list", "data": [serialize_hash_val(x) for x in val]}
    elif isinstance(val, dict):
        return {"type": "dict", "data": {k: serialize_hash_val(v) for k, v in val.items()}}
    else:
        return {"type": "scalar", "data": val}

def make_request(url, path, method="POST", data=None):
    full_url = f"{url.rstrip('/')}{path}"
    headers = {"Content-Type": "application/json"}
    req_data = json.dumps(data).encode("utf-8") if data is not None else None
    
    req = urllib.request.Request(full_url, data=req_data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            res_data = response.read().decode("utf-8")
            return json.loads(res_data)
    except Exception as e:
        return {"success": False, "error": str(e)}

class RemoteTable:
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def insert(self, document):
        res = self.client._send_request("/api/query", {"action": "insert", "table": self.name, "doc": document})
        if not res.get("success"):
            raise RuntimeError(res.get("error", "Error during insert"))
        return res["result"]

    def all(self):
        res = self.client._send_request("/api/query", {"action": "all", "table": self.name})
        if not res.get("success"):
            raise RuntimeError(res.get("error", "Error during all"))
        return [Document(d["value"], doc_id=d["doc_id"]) for d in res["result"]]

    def search(self, cond):
        res = self.client._send_request("/api/query", {"action": "search", "table": self.name, "query": serialize_query(cond)})
        if not res.get("success"):
            raise RuntimeError(res.get("error", "Error during search"))
        return [Document(d["value"], doc_id=d["doc_id"]) for d in res["result"]]

    def update(self, fields, cond=None, doc_ids=None):
        res = self.client._send_request("/api/query", {
            "action": "update", "table": self.name, "fields": fields, 
            "query": serialize_query(cond), "doc_ids": doc_ids
        })
        if not res.get("success"):
            raise RuntimeError(res.get("error", "Error during update"))
        return res["result"]

    def remove(self, cond=None, doc_ids=None):
        res = self.client._send_request("/api/query", {
            "action": "remove", "table": self.name, 
            "query": serialize_query(cond), "doc_ids": doc_ids
        })
        if not res.get("success"):
            raise RuntimeError(res.get("error", "Error during remove"))
        return res["result"]

class TinyDBClient:
    def __init__(self, server_url):
        self.server_url = server_url
        self._default_table = RemoteTable(self, '_default')

    def table(self, name):
        return RemoteTable(self, name)

    def _send_request(self, path, data):
        return make_request(self.server_url, path, "POST", data)

    def __getattr__(self, name):
        return getattr(self._default_table, name)

    def __len__(self):
        res = self._send_request("/api/query", {"action": "len", "table": "_default"})
        return res.get("result", 0)
