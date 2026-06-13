import time
import os
import sys
from tinydb.distributed import TinyDBServer, TinyDBClient

def main():
    db1, db2, db3 = "db_primary.json", "db_backup1.json", "db_backup2.json"
    for f in (db1, db2, db3):
        if os.path.exists(f):
            try: os.remove(f)
            except: pass

    cluster_nodes = ["http://127.0.0.1:5000", "http://127.0.0.1:5001", "http://127.0.0.1:5002"]
    
    print("=" * 60)
    print("        TINYDB DISTRIBUTED CLUSTER DEMO")
    print("=" * 60)
    print("Đang khởi tạo các node...")
    
    node1 = TinyDBServer(db1, host="127.0.0.1", port=5000, role="PRIMARY", cluster_nodes=cluster_nodes)
    node1.start()
    node2 = TinyDBServer(db2, host="127.0.0.1", port=5001, role="BACKUP", primary_url="http://127.0.0.1:5000", cluster_nodes=cluster_nodes)
    node2.start()
    node3 = TinyDBServer(db3, host="127.0.0.1", port=5002, role="BACKUP", primary_url="http://127.0.0.1:5000", cluster_nodes=cluster_nodes)
    node3.start()
    
    time.sleep(1.5)
    
    print("\nĐang chèn dữ liệu mẫu...")
    try:
        client = TinyDBClient("http://127.0.0.1:5000")
        client.insert({"name": "Nguyen Van A", "email": "a@example.com", "role": "Student"})
        client.insert({"name": "Tran Thi B", "email": "b@example.com", "role": "Teacher"})
        print("-> Đã chèn dữ liệu thành công.")
    except Exception as e:
        print(f"Lỗi: {e}")
        
    print("\nTruy cập dashboard để giám sát:")
    print(" - Node 1: http://127.0.0.1:5000/dashboard")
    print(" - Node 2: http://127.0.0.1:5001/dashboard")
    print(" - Node 3: http://127.0.0.1:5002/dashboard")
    print("Nhấn Ctrl+C để tắt cluster và dọn dẹp dữ liệu.")
    
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        node1.stop(); node2.stop(); node3.stop()
        for f in (db1, db2, db3):
            if os.path.exists(f):
                try: os.remove(f)
                except: pass
        sys.exit(0)

if __name__ == '__main__':
    main()
