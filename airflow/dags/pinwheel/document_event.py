"""
Module chứa các hàm xử lý document events từ MinIO
"""
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import unquote
import json
import psycopg2
from psycopg2.extras import RealDictCursor

from airflow.models import Variable
from pinwheel.database_utils import get_postgres_conn_info


def determine_file_type(file_path: str, etag: str, event_name: str, 
                       existing_records: List[Dict]) -> str:
    """
    Xác định type của file dựa trên file_path (tên file) và etag.
    
    LOGIC PHÂN LOẠI:
    ================
    
    1. DELETE: 
       - Nếu event_name chứa "Delete" hoặc "Removed"
       - File đã bị xóa khỏi MinIO
    
    2. CREATE:
       - Không có record nào trước đó với cùng file_path (KHÔNG PHẢI DELETE)
       - File mới được upload lần đầu
       - HOẶC file đã bị DELETE trước đó và đang được tạo lại (dù có cùng etag)
    
    3. UPDATE:
       - Có record trước đó (KHÔNG PHẢI DELETE) với cùng file_path VÀ etag khác
       - File đã tồn tại nhưng nội dung đã thay đổi (etag khác = nội dung khác)
    
    4. KEEP:
       - Có record trước đó (KHÔNG PHẢI DELETE) với cùng file_path VÀ cùng etag
       - File đã tồn tại và nội dung không thay đổi (có thể là duplicate event)
    
    QUAN TRỌNG: 
    ===========
    - Nếu record mới nhất (có event_time lớn nhất) trong existing_records là DELETE,
      thì file mới upload lại phải là CREATE, bất kể có records cũ hơn với cùng etag.
    - Chỉ so sánh với các records KHÔNG PHẢI DELETE
    - Nếu file đã bị DELETE, dù có cùng etag, file mới upload lại vẫn là CREATE
    
    VÍ DỤ:
    ======
    - T1: CREATE với etag="abc"
    - T2: DELETE
    - T3: CREATE với etag="abc" (cùng với T1)
    → T3 phải là CREATE (không phải KEEP), vì T2 là DELETE (mới nhất trước T3)
    
    CÁC TRƯỜNG HỢP CỤ THỂ:
    ======================
    
    Trường hợp 1: event_name = "s3:ObjectRemoved:Delete"
    → DELETE (dừng ngay, không cần kiểm tra gì thêm)
    
    Trường hợp 2: existing_records = [] (rỗng)
    → CREATE (file chưa từng xuất hiện trước đó)
    
    Trường hợp 3: Record mới nhất trong existing_records là DELETE
    → CREATE (file đã bị DELETE và đang được tạo lại)
    
    Trường hợp 4: Có existing_records KHÔNG PHẢI DELETE
    → So sánh etag:
    
      4a. Etag khác với tất cả records trước đó (không phải DELETE)
          → UPDATE (file đã thay đổi nội dung)
    
      4b. Etag giống với một số records trước đó (không phải DELETE)
          → KEEP (file không thay đổi, có thể là duplicate event)
    
    Args:
        file_path: Đường dẫn file trên MinIO (tên file)
        etag: ETag của file (hash của nội dung file)
        event_name: Tên event (s3:ObjectCreated:Put, s3:ObjectRemoved:Delete, etc.)
        existing_records: Danh sách các record đã tồn tại với cùng file_path (event_time cũ hơn)
    
    Returns:
        str: 'CREATE', 'UPDATE', 'DELETE', hoặc 'KEEP'
    """
    # ============================================================
    # BƯỚC 1: Kiểm tra DELETE (ưu tiên cao nhất)
    # ============================================================
    # Nếu event_name chứa "Delete" hoặc "Removed" → DELETE
    if event_name and ('Delete' in event_name or 'Removed' in event_name):
        return 'DELETE'
    
    # ============================================================
    # BƯỚC 2: Kiểm tra xem record mới nhất có phải là DELETE không
    # ============================================================
    # QUAN TRỌNG: Nếu record mới nhất (có event_time lớn nhất) trong 
    # existing_records là DELETE, thì file mới upload lại phải là CREATE,
    # bất kể có records cũ hơn với cùng etag hay không.
    # 
    # Ví dụ:
    # - T1: CREATE với etag="abc"
    # - T2: DELETE
    # - T3: CREATE với etag="abc" (cùng với T1)
    # → T3 phải là CREATE (không phải KEEP), vì T2 là DELETE (mới nhất trước T3)
    
    if existing_records:
        # Tìm record có event_time lớn nhất (mới nhất)
        latest_existing_record = max(
            existing_records,
            key=lambda x: x.get('event_time') if x.get('event_time') is not None else datetime.min
        )
        
        # Kiểm tra xem record mới nhất có phải là DELETE không
        latest_event_name = latest_existing_record.get('event_name', '')
        if latest_event_name and ('Delete' in latest_event_name or 'Removed' in latest_event_name):
            # Record mới nhất là DELETE → file mới là CREATE
            return 'CREATE'
    
    # ============================================================
    # BƯỚC 3: Lọc bỏ các records DELETE khỏi existing_records
    # ============================================================
    # Chỉ so sánh với các records KHÔNG PHẢI DELETE
    non_delete_records = [
        r for r in existing_records 
        if r.get('event_name') and 
        'Delete' not in r.get('event_name', '') and 
        'Removed' not in r.get('event_name', '')
    ]
    
    # ============================================================
    # BƯỚC 4: Kiểm tra CREATE
    # ============================================================
    # Nếu không có record nào KHÔNG PHẢI DELETE → CREATE
    # (có thể là file mới hoặc file đã bị DELETE và đang được tạo lại)
    if not non_delete_records:
        return 'CREATE'
    
    # ============================================================
    # BƯỚC 5: So sánh etag với non_delete_records
    # ============================================================
    # Tìm các record có cùng etag (cùng nội dung file)
    same_etag_records = [r for r in non_delete_records if r.get('etag') == etag]
    
    # Nếu có record với cùng etag → KEEP (file không thay đổi)
    if same_etag_records:
        return 'KEEP'
    
    # ============================================================
    # BƯỚC 6: Etag khác với tất cả records trước đó
    # ============================================================
    # Nếu etag khác → nội dung file đã thay đổi → UPDATE
    return 'UPDATE'


def extract_event_info(event_data: Any) -> Optional[Dict[str, Any]]:
    """
    Extract thông tin từ event_data JSON.
    
    Args:
        event_data: Dict hoặc string JSON chứa event data từ MinIO
    
    Returns:
        Dict với các thông tin: file_path, etag, version_id, event_name, bucket_name
        hoặc None nếu không parse được
    """
    try:
        # event_data có thể là string JSON hoặc đã là dict (nếu là JSONB từ Postgres)
        if isinstance(event_data, str):
            event_data = json.loads(event_data)
        
        # Lấy record đầu tiên từ Records
        if not isinstance(event_data, dict):
            return None
        
        if 'Records' not in event_data:
            return None
        
        records = event_data.get('Records', [])
        if not records or len(records) == 0:
            return None
        
        record = records[0]
        
        # Extract thông tin từ s3 object
        s3_obj = record.get('s3', {})
        object_info = s3_obj.get('object', {})
        bucket_info = s3_obj.get('bucket', {})
        
        file_path = object_info.get('key', '')
        etag = object_info.get('eTag', '')
        version_id = object_info.get('versionId', 'null')
        bucket_name = bucket_info.get('name', '')
        event_name = record.get('eventName', '')
        
        return {
            'file_path': file_path,
            'etag': etag,
            'version_id': version_id if version_id else 'null',
            'event_name': event_name,
            'bucket_name': bucket_name,
        }
    except (json.JSONDecodeError, KeyError, TypeError, AttributeError) as e:
        print(f"Lỗi khi parse event_data: {e}")
        return None


def query_new_documents(conn_id: str = 'pgvector_conn', dag_id: str = 'check_document_metadata', 
                       bucket_name: Optional[str] = None, table_name: Optional[str] = None) -> Dict[str, List[Dict[str, str]]]:
    """
    Query bảng document_metadata để lấy các file mới upload.
    Bảng chỉ có 2 cột: event_time (timestamp) và event_data (JSON).
    
    CÁC BƯỚC XỬ LÝ:
    1. Xác định tên table (nếu không truyền, sử dụng event_<bucket_name>)
    2. Lấy last_event_time từ Airflow Variable (dựa trên tên DAG)
    3. Query CHỈ các records có event_time > last_event_time từ bảng document_metadata
    4. Parse event_data JSON để extract: file_path, etag, version_id, event_name, bucket_name
    5. LỌC các records theo bucket_name (nếu được chỉ định)
    6. Nhóm các records theo file_path (sau khi URL decode)
    7. Với mỗi file_path, lấy record có event_time MỚI NHẤT
    8. So sánh record mới nhất với các record cũ hơn của cùng file_path để xác định type:
       - CREATE: Không có record nào trước đó
       - UPDATE: Có record trước đó nhưng etag khác
       - DELETE: event_name chứa "Delete" hoặc "Removed"
       - KEEP: Có record trước đó và cùng etag
    9. Cập nhật Airflow Variable với event_time mới nhất
    
    LOGIC LẤY FILE MỚI NHẤT:
    - Chỉ query các record có event_time > last_event_time (từ Variable)
    - Lọc theo bucket_name nếu được chỉ định
    - Dựa trên event_time: Record có event_time lớn nhất = mới nhất
    - Mỗi file_path chỉ lấy 1 record (record mới nhất)
    - So sánh với các record có event_time < event_time của record mới nhất
    
    Args:
        conn_id: Airflow connection ID cho Postgres database
        dag_id: Tên DAG để tạo Variable name
        bucket_name: Tên bucket để lọc (None = lấy tất cả buckets)
        table_name: Tên bảng để query (None = sử dụng event_<bucket_name>)
    
    Returns:
        Dict[str, List[Dict[str, str]]]: Dict với các key 'delete', 'keep', 'create', 'update'.
            Mỗi key chứa danh sách các dict với 'bucket_name' và 'file_path'.
    """
    # ============================================================
    # BƯỚC 0: Xác định tên table
    # ============================================================
    if table_name is None:
        if bucket_name is None:
            raise ValueError("Nếu không truyền table_name, phải truyền bucket_name để tạo tên table (event_<bucket_name>)")
        table_name = f"event_{bucket_name.replace('-', '')}"
    
    print(f"BƯỚC 0: Sử dụng table_name = '{table_name}'")
    
    # ============================================================
    # BƯỚC 1: Lấy hoặc tạo Airflow Variable để lưu last_event_time
    # ============================================================
    variable_name = f"{dag_id}_last_event_time"
    
    try:
        last_event_time_str = Variable.get(variable_name, default_var=None)
        if last_event_time_str:
            # Parse từ string sang datetime
            # Xử lý cả format có Z và không có Z
            try:
                if 'Z' in last_event_time_str:
                    last_event_time = datetime.fromisoformat(last_event_time_str.replace('Z', '+00:00'))
                else:
                    last_event_time = datetime.fromisoformat(last_event_time_str)
                print(f"BƯỚC 1: Đã lấy last_event_time từ Variable '{variable_name}': {last_event_time}")
            except ValueError as ve:
                print(f"WARNING: Không thể parse last_event_time '{last_event_time_str}': {ve}, sẽ query tất cả records")
                last_event_time = None
        else:
            last_event_time = None
            print(f"BƯỚC 1: Variable '{variable_name}' chưa tồn tại, sẽ query tất cả records")
    except Exception as e:
        print(f"WARNING: Không thể lấy Variable '{variable_name}': {e}, sẽ query tất cả records")
        last_event_time = None
    
    # Lấy thông tin kết nối Postgres
    conn_info = get_postgres_conn_info(conn_id)
    
    # Kết nối đến database
    conn = psycopg2.connect(
        host=conn_info['host'],
        port=conn_info['port'],
        user=conn_info['user'],
        password=conn_info['password'],
        database='postgres'
    )
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Kiểm tra xem bảng có tồn tại không
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table_name,))
            result = cursor.fetchone()
            if result is None:
                print(f"WARNING: Không thể kiểm tra sự tồn tại của bảng '{table_name}'")
                return {'delete': [], 'keep': [], 'create': [], 'update': []}
            
            # RealDictCursor trả về dict, tuple cursor trả về tuple
            # Với EXISTS query, lấy giá trị đầu tiên (có thể là 'exists' hoặc tên cột khác)
            if isinstance(result, dict):
                # Lấy giá trị đầu tiên trong dict
                table_exists = list(result.values())[0] if result else False
            else:
                table_exists = result[0] if len(result) > 0 else False
            
            if not table_exists:
                print(f"WARNING: Bảng '{table_name}' không tồn tại trong database")
                return {'delete': [], 'keep': [], 'create': [], 'update': []}
            
            # ============================================================
            # BƯỚC 2: Query CHỈ các records MỚI (event_time > last_event_time)
            # ============================================================
            # Bảng chỉ có 2 cột: event_time (timestamp) và event_data (JSON)
            # Chỉ lấy các record có event_time > last_event_time
            # Sắp xếp theo event_time DESC để record mới nhất ở đầu
            if last_event_time:
                query = f"""
                    SELECT 
                        event_time,
                        event_data
                    FROM {table_name}
                    WHERE event_time > %s
                    ORDER BY event_time DESC
                """
                cursor.execute(query, (last_event_time,))
            else:
                # Nếu chưa có last_event_time, query tất cả
                query = f"""
                    SELECT 
                        event_time,
                        event_data
                    FROM {table_name}
                    ORDER BY event_time DESC
                """
                cursor.execute(query)
            
            all_records = cursor.fetchall()
            
            if not all_records:
                print(f"BƯỚC 2: Không có record mới nào (sau {last_event_time if last_event_time else 'tất cả'})")
                return {'delete': [], 'keep': [], 'create': [], 'update': []}
            
            print(f"BƯỚC 2: Tìm thấy {len(all_records)} record(s) mới trong bảng {table_name}")
            
            # ============================================================
            # BƯỚC 3: Parse event_data JSON và extract thông tin
            # ============================================================
            # Lưu lại event_time mới nhất để cập nhật Variable sau
            max_event_time = None
            # Lưu thông tin bucket_name để lọc
            if bucket_name:
                print(f"BƯỚC 3: Sẽ lọc các records theo bucket_name = '{bucket_name}'")
            # Mỗi event_data chứa JSON với cấu trúc:
            # {
            #   "Records": [{
            #     "s3": {
            #       "object": {"key": "file_path", "eTag": "...", "versionId": "..."},
            #       "bucket": {"name": "..."}
            #     },
            #     "eventName": "s3:ObjectCreated:Put"
            #   }]
            # }
            parsed_records = []
            for idx, record in enumerate(all_records):
                try:
                    # RealDictCursor trả về dict, nhưng cần kiểm tra
                    if isinstance(record, dict):
                        event_data = record.get('event_data')
                        event_time = record.get('event_time')
                    else:
                        # Nếu là tuple, giả sử thứ tự: event_time, event_data
                        event_time = record[0] if len(record) > 0 else None
                        event_data = record[1] if len(record) > 1 else None
                    
                    if event_data is None:
                        print(f"WARNING: event_data là None cho record với event_time: {event_time}")
                        continue
                    
                    # Extract thông tin từ event_data (hàm extract_event_info sẽ xử lý parse)
                    event_info = extract_event_info(event_data)
                    if event_info:
                        # Lọc theo bucket_name nếu được chỉ định
                        if bucket_name is not None:
                            event_bucket_name = event_info.get('bucket_name', '')
                            if event_bucket_name != bucket_name:
                                # Bỏ qua record này vì không khớp bucket_name
                                continue
                        
                        event_info['event_time'] = event_time
                        parsed_records.append(event_info)
                        
                        # Cập nhật max_event_time
                        if event_time is not None:
                            if max_event_time is None or event_time > max_event_time:
                                max_event_time = event_time
                    else:
                        print(f"WARNING: Không thể extract thông tin từ record {idx}, event_data type: {type(event_data)}")
                except (KeyError, IndexError, TypeError) as e:
                    print(f"Lỗi khi xử lý record {idx}: {e}, record type: {type(record)}, record: {str(record)[:200]}")
                    continue
            
            if not parsed_records:
                if bucket_name:
                    print(f"Không tìm thấy record nào hợp lệ sau khi parse và lọc theo bucket_name = '{bucket_name}'")
                else:
                    print("Không tìm thấy record nào hợp lệ sau khi parse")
                return {'delete': [], 'keep': [], 'create': [], 'update': []}
            
            if bucket_name:
                print(f"BƯỚC 3: Đã parse và lọc thành công {len(parsed_records)} record(s) từ bucket '{bucket_name}'")
            else:
                print(f"BƯỚC 3: Đã parse thành công {len(parsed_records)} record(s)")
            
            # ============================================================
            # BƯỚC 4: Nhóm các records theo file_path (đã URL decode)
            # ============================================================
            # Mục đích: Một file có thể có nhiều events (upload nhiều lần)
            # Cần nhóm lại để xử lý từng file
            file_groups = {}
            for record in parsed_records:
                # URL decode file_path (ví dụ: "hr%2Ffile.pdf" -> "hr/file.pdf")
                file_path = unquote(record['file_path']) if record['file_path'] else ''
                if file_path not in file_groups:
                    file_groups[file_path] = []
                file_groups[file_path].append(record)
            
            print(f"BƯỚC 4: Đã nhóm thành {len(file_groups)} file_path khác nhau")
            
            # ============================================================
            # BƯỚC 5: Lấy record MỚI NHẤT cho mỗi file_path
            # ============================================================
            # LOGIC: Dựa trên event_time - record có event_time lớn nhất = mới nhất
            # Mỗi file_path chỉ lấy 1 record (record mới nhất)
            latest_records = []
            for file_path, records in file_groups.items():
                # Tìm record có event_time lớn nhất
                # Xử lý trường hợp event_time có thể là None (dùng datetime.min làm fallback)
                latest_record = max(
                    records, 
                    key=lambda x: x['event_time'] if x['event_time'] is not None else datetime.min
                )
                latest_records.append(latest_record)
            
            print(f"BƯỚC 5: Đã lấy {len(latest_records)} file(s) mới nhất (1 file = 1 record mới nhất)")
            
            # ============================================================
            # BƯỚC 6: Xác định type (CREATE/UPDATE/DELETE/KEEP) cho mỗi file
            # ============================================================
            # LOGIC XÁC ĐỊNH TYPE:
            # - DELETE: event_name chứa "Delete" hoặc "Removed"
            # - CREATE: Không có record nào trước đó (existing_records rỗng)
            # - UPDATE: Có record trước đó và etag khác
            # - KEEP: Có record trước đó và cùng etag
            #
            # QUAN TRỌNG: existing_records phải query từ DATABASE (không chỉ từ file_groups)
            # vì file_groups chỉ chứa records mới trong lần query hiện tại.
            # Nếu file đã upload trước đó, record cũ không có trong file_groups.
            result = {
                'delete': [],
                'keep': [],
                'create': [],
                'update': []
            }
            
            for record in latest_records:
                file_path = unquote(record['file_path']) if record['file_path'] else ''
                current_event_time = record['event_time']
                
                # QUERY TỪ DATABASE để lấy tất cả các records CŨ HƠN của cùng file_path
                # (không giới hạn bởi last_event_time, để tìm các records từ các lần query trước)
                existing_records = []
                
                # Query tất cả records có cùng file_path và event_time < current_event_time
                # Query tất cả records cũ hơn, sau đó parse và so sánh file_path (đã decode)
                if current_event_time is not None:
                    # Query tất cả records có cùng file_path (so sánh cả encoded và decoded)
                    # và event_time < current_event_time
                    query_existing = f"""
                        SELECT 
                            event_time,
                            event_data
                        FROM {table_name}
                        WHERE event_time < %s
                        ORDER BY event_time DESC
                    """
                    cursor.execute(query_existing, (current_event_time,))
                    all_older_records = cursor.fetchall()
                    
                    # Parse và lọc các records có cùng file_path
                    for older_record in all_older_records:
                        try:
                            if isinstance(older_record, dict):
                                older_event_data = older_record.get('event_data')
                                older_event_time = older_record.get('event_time')
                            else:
                                older_event_time = older_record[0] if len(older_record) > 0 else None
                                older_event_data = older_record[1] if len(older_record) > 1 else None
                            
                            if older_event_data is None:
                                continue
                            
                            # Extract thông tin từ event_data
                            older_event_info = extract_event_info(older_event_data)
                            if older_event_info:
                                # URL decode để so sánh
                                older_file_path = unquote(older_event_info.get('file_path', ''))
                                
                                # So sánh file_path (cả encoded và decoded)
                                if older_file_path == file_path:
                                    # Lọc theo bucket_name nếu được chỉ định
                                    if bucket_name is not None:
                                        older_bucket_name = older_event_info.get('bucket_name', '')
                                        if older_bucket_name != bucket_name:
                                            continue
                                    
                                    # Thêm vào existing_records
                                    older_event_info['event_time'] = older_event_time
                                    existing_records.append(older_event_info)
                        except Exception as e:
                            print(f"WARNING: Lỗi khi xử lý older_record cho file_path '{file_path}': {e}")
                            continue
                
                # Ngoài ra, cũng lấy các records cũ hơn từ file_groups (trong cùng lần query)
                # để đảm bảo không bỏ sót
                file_groups_older = [
                    r for r in file_groups.get(file_path, [])
                    if (current_event_time is not None and 
                        r['event_time'] is not None and 
                        r['event_time'] < current_event_time) or
                       (current_event_time is None and r['event_time'] is not None)
                ]
                
                # Merge và loại bỏ duplicate (dựa trên event_time và etag)
                # Thêm các records từ file_groups nếu chưa có trong existing_records
                for fg_record in file_groups_older:
                    # Kiểm tra xem đã có record với cùng event_time và etag chưa
                    is_duplicate = any(
                        er.get('event_time') == fg_record.get('event_time') and 
                        er.get('etag') == fg_record.get('etag')
                        for er in existing_records
                    )
                    if not is_duplicate:
                        existing_records.append(fg_record)
                
                print(f"  File: {file_path}")
                print(f"    Current event_time: {current_event_time}")
                print(f"    Found {len(existing_records)} existing record(s) from database")
                
                # Xác định type (chỉ dựa trên file_path và etag)
                file_type = determine_file_type(
                    file_path=file_path,
                    etag=record.get('etag', ''),
                    event_name=record.get('event_name', ''),
                    existing_records=existing_records
                )
                
                # Chuyển đổi type sang lowercase để dùng làm key
                file_type_lower = file_type.lower()
                
                # Thêm vào result dict với chỉ bucket_name và file_path
                result[file_type_lower].append({
                    'bucket_name': record.get('bucket_name', ''),
                    'file_path': file_path,
                })
            
            # ============================================================
            # BƯỚC 7: Cập nhật Airflow Variable với event_time mới nhất
            # ============================================================
            if max_event_time is not None:
                try:
                    # Chuyển datetime sang ISO format string để lưu vào Variable
                    max_event_time_str = max_event_time.isoformat()
                    Variable.set(variable_name, max_event_time_str)
                    print(f"BƯỚC 7: Đã cập nhật Variable '{variable_name}' với event_time mới nhất: {max_event_time}")
                except Exception as e:
                    print(f"WARNING: Không thể cập nhật Variable '{variable_name}': {e}")
            else:
                print(f"BƯỚC 7: Không có event_time mới để cập nhật Variable")
            
            # ============================================================
            # KẾT QUẢ CUỐI CÙNG
            # ============================================================
            total_files = sum(len(result[key]) for key in result)
            print(f"\n{'='*60}")
            print(f"KẾT QUẢ: Tổng số file tìm thấy: {total_files}")
            print(f"{'='*60}")
            for file_type in ['delete', 'keep', 'create', 'update']:
                files = result[file_type]
                if files:
                    print(f"  {file_type.upper()}: {len(files)} file(s)")
                    for item in files:
                        print(f"    - Bucket: {item['bucket_name']} | File: {item['file_path']}")
            
            return result
            
    except psycopg2.Error as e:
        print(f"Lỗi khi query database: {e}")
        raise
    except (KeyError, IndexError, TypeError, AttributeError) as e:
        print(f"Lỗi khi xử lý dữ liệu: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        conn.close()

