import time
import os
from tinydb import where
from tinydb.distributed import TinyDBServer, TinyDBClient

def test_distributed():
    db1, db2 = "test_primary.json", "test_backup.json"
    for f in (db1, db2):
        if os.path.exists(f): os.remove(f)
    cluster = ["http://127.0.0.1:15000", "http://127.0.0.1:15001"]
    
    srv1 = TinyDBServer(db1, host="127.0.0.1", port=15000, role="PRIMARY", cluster_nodes=cluster)
    srv1.start()
    srv2 = TinyDBServer(db2, host="127.0.0.1", port=15001, role="BACKUP", primary_url="http://127.0.0.1:15000", cluster_nodes=cluster)
    srv2.start()
    
    time.sleep(1.0)
    
    try:
        client = TinyDBClient("http://127.0.0.1:15000")
        doc_id = client.insert({"name": "Alice"})
        assert doc_id == 1
        
        time.sleep(0.5)
        backup_client = TinyDBClient("http://127.0.0.1:15001")
        assert len(backup_client.all()) == 1
        print("Replication test: PASSED")
        
        try:
            backup_client.insert({"name": "Bob"})
            assert False, "Backup node allowed write!"
        except RuntimeError:
            print("Read-only Backup test: PASSED")
    finally:
        srv1.stop(); srv2.stop()
        for f in (db1, db2):
            if os.path.exists(f): os.remove(f)

if __name__ == '__main__':
    test_distributed()
