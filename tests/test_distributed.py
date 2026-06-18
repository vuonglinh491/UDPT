import time
import os
import pytest
import urllib.request
import json
from tinydb import TinyDB, Query
from tinydb.distributed import TinyDBServer, TinyDBClient

@pytest.fixture
def cluster_setup():
    # Khởi tạo đường dẫn db tạm thời
    db1, db2 = "test_db_primary.json", "test_db_backup.json"
    for f in (db1, db2):
        if os.path.exists(f):
            try: os.remove(f)
            except: pass

    cluster_nodes = ["http://127.0.0.1:6000", "http://127.0.0.1:6001"]
    
    # Khởi động Primary và Backup
    primary = TinyDBServer(db1, host="127.0.0.1", port=6000, role="PRIMARY", cluster_nodes=cluster_nodes)
    primary.start()
    
    backup = TinyDBServer(db2, host="127.0.0.1", port=6001, role="BACKUP", primary_url="http://127.0.0.1:6000", cluster_nodes=cluster_nodes)
    backup.start()
    
    # Đợi cụm khởi động và kết nối heartbeat
    time.sleep(2.0)
    
    yield primary, backup
    
    # Dọn dẹp sau khi test
    primary.stop()
    backup.stop()
    time.sleep(0.5)
    for f in (db1, db2):
        if os.path.exists(f):
            try: os.remove(f)
            except: pass

def test_replication(cluster_setup):
    primary, backup = cluster_setup
    
    # Khởi tạo client kết nối tới Primary
    client = TinyDBClient("http://127.0.0.1:6000")
    
    # Chèn dữ liệu vào Primary
    doc_id = client.insert({"item": "apple", "quantity": 10})
    assert doc_id == 1
    
    # Chờ đồng bộ sang Backup
    time.sleep(1.0)
    
    # Kiểm tra dữ liệu trên Backup (thông qua đọc trực tiếp từ DB file của backup)
    backup_db = TinyDB("test_db_backup.json")
    results = backup_db.all()
    backup_db.close()
    
    assert len(results) == 1
    assert results[0]["item"] == "apple"
    assert results[0]["quantity"] == 10

def test_backup_readonly(cluster_setup):
    primary, backup = cluster_setup
    
    # Khởi tạo client kết nối trực tiếp tới Backup
    client_backup = TinyDBClient("http://127.0.0.1:6001")
    
    # Thử ghi dữ liệu lên Backup trực tiếp -> Phải ném ra lỗi Runtime do Backup chỉ đọc
    with pytest.raises(Exception) as excinfo:
        client_backup.insert({"item": "orange", "quantity": 5})
    
    assert "Read-only" in str(excinfo.value) or "Backup" in str(excinfo.value)

def test_failover_and_promote(cluster_setup):
    primary, backup = cluster_setup
    
    # Giả lập Primary bị sập (crash)
    # Gửi request crash tới Primary
    req = urllib.request.Request("http://127.0.0.1:6000/api/simulate_crash", method="POST")
    try:
        with urllib.request.urlopen(req, timeout=1.0) as r:
            res = json.loads(r.read().decode("utf-8"))
            assert res.get("success") is True
    except Exception as e:
        pytest.fail(f"Không thể giả lập crash Primary: {e}")
        
    # Đợi Heartbeat của Backup phát hiện lỗi (3 lần lỗi * 2 giây = 6 giây, cộng thời gian bầu chọn)
    # Ta đợi khoảng 8-9 giây
    time.sleep(9.0)
    
    # Kiểm tra xem Backup đã được promote thành PRIMARY chưa
    assert backup.role == "PRIMARY"
    
    # Thử chèn dữ liệu vào Backup (giờ đã là Primary mới)
    client_new_primary = TinyDBClient("http://127.0.0.1:6001")
    doc_id = client_new_primary.insert({"item": "banana", "quantity": 20})
    assert doc_id == 1
    
    # Kiểm tra dữ liệu ghi thành công vào DB của backup
    backup_db = TinyDB("test_db_backup.json")
    results = backup_db.all()
    backup_db.close()
    assert len(results) == 1
    assert results[0]["item"] == "banana"
