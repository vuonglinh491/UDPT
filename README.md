# UDPT

UDPT là một dự án Python xây dựng trên TinyDB, cung cấp demo cụm phân tán và hỗ trợ server/client.

## Tổng quan

- Lưu trữ dữ liệu dạng document bằng `tinydb`
- Demo cụm phân tán sử dụng `tinydb.distributed.TinyDBServer` và `tinydb.distributed.TinyDBClient`
- Bao gồm `demo_cluster.py` để khởi chạy cụm ba node và giám sát qua dashboard web

## Yêu cầu

- Python 3.10 đến 3.14
- Không cần phụ thuộc ngoài thư viện chuẩn

## Cài đặt

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
```

## Chạy demo phân tán

```powershell
python demo_cluster.py
```

Sau đó mở trình duyệt với các địa chỉ:

- `http://127.0.0.1:5000/dashboard`
- `http://127.0.0.1:5001/dashboard`
- `http://127.0.0.1:5002/dashboard`

Nhấn `Ctrl+C` trong terminal để dừng cụm và xóa các file dữ liệu.

## Sử dụng ví dụ

```python
from tinydb import TinyDB, Query

db = TinyDB('db.json')
db.insert({'name': 'Alice', 'role': 'Developer'})
print(db.all())
```

## Kiểm thử

```powershell
python -m pip install pytest
python -m pytest
```

## Cấu trúc dự án

- `tinydb/` - phần cài đặt lõi của thư viện
- `tinydb/distributed/` - mã server và client phân tán
- `demo_cluster.py` - script ví dụ khởi chạy cụm
- `tests/` - bộ kiểm thử

## Giấy phép

MIT License

## Video trình bày

https://drive.google.com/file/d/1QjUp7HuKwXjCl8ws3B4TxUQMcLI1rmQV/view?usp=drive_link
