"""
End-to-end RAG DAG: Query document metadata và xử lý RAG workflow
Kết hợp logic từ check_dag.py và newdag.py
Mỗi file CREATE sẽ có pipeline riêng với các tasks riêng
"""
from datetime import datetime, timedelta
from typing import List, Dict, Any
from airflow import DAG
from airflow.operators.python import PythonOperator
try:
    from airflow.operators.empty import EmptyOperator as DummyOperator
except ImportError:
    from airflow.operators.dummy import DummyOperator

# Import từ check_dag.py (relative import)
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from check_dag import query_new_documents

# Import từ newdag.py
from pinwheel.s3_utils import get_s3_config_from_airflow_connection
from rag.loading.loading_builder import load_documents
from rag.chunking.chunking_builder import chunk_documents
from rag.indexing.indexing_builder import index_documents_with_embeddings_config
from pinwheel.database_utils import parse_pgvector_connection_url

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'e2e_rag_dag',
    default_args=default_args,
    description='End-to-end RAG DAG: Query document metadata và xử lý RAG workflow',
    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['rag', 'e2e', 'document-processing'],
)


def process_files_by_type(**context):
    """Xử lý files theo type: CREATE, UPDATE, DELETE, KEEP"""
    ti = context['ti']
    files = ti.xcom_pull(task_ids='query_new_documents')
    
    if not files:
        print("Không có file nào để xử lý")
        return {
            'CREATE': [],
            'UPDATE': [],
            'DELETE': [],
            'KEEP': []
        }
    
    files_by_type = {
        'CREATE': [],
        'UPDATE': [],
        'DELETE': [],
        'KEEP': []
    }
    
    for file_info in files:
        file_type = file_info.get('type', 'KEEP')
        if file_type in files_by_type:
            files_by_type[file_type].append(file_info)
        else:
            files_by_type['KEEP'].append(file_info)
    
    print(f"Phân loại files:")
    print(f"  - CREATE: {len(files_by_type['CREATE'])} file(s)")
    print(f"  - UPDATE: {len(files_by_type['UPDATE'])} file(s)")
    print(f"  - DELETE: {len(files_by_type['DELETE'])} file(s)")
    print(f"  - KEEP: {len(files_by_type['KEEP'])} file(s)")
    
    return files_by_type


def get_create_files_list(**context):
    """Lấy danh sách các file CREATE"""
    ti = context['ti']
    files_by_type = ti.xcom_pull(task_ids='process_files_by_type')
    create_files = files_by_type.get('CREATE', [])
    
    if not create_files:
        print("Không có file CREATE nào để xử lý")
        return []
    
    print(f"Sẽ tạo pipeline cho {len(create_files)} file(s) CREATE")
    for idx, file_info in enumerate(create_files):
        print(f"  {idx+1}. {file_info.get('file_path')}")
    
    return create_files


# ============================================================
# TASKS - Query và phân loại
# ============================================================

query_documents_task = PythonOperator(
    task_id='query_new_documents',
    python_callable=query_new_documents,
    op_kwargs={
        'conn_id': 'pgvector_conn',
        'dag_id': 'e2e_rag_dag',
        'bucket_name': 'source-bucket'
    },
    dag=dag,
)

process_files_by_type_task = PythonOperator(
    task_id='process_files_by_type',
    python_callable=process_files_by_type,
    dag=dag,
)

get_create_files_task = PythonOperator(
    task_id='process_create_files',
    python_callable=get_create_files_list,
    dag=dag,
)


# ============================================================
# TASKS - Pipeline cho mỗi file CREATE
# Mỗi file sẽ có các tasks riêng: get_s3_config >> load >> chunk >> get_pgvector >> index
# Sử dụng Dynamic Task Mapping với .expand() để tạo tasks riêng cho mỗi file
# ============================================================

# Helper function để tạo task_id an toàn từ file_path
def sanitize_task_id(file_path: str, prefix: str = "") -> str:
    """Tạo task_id an toàn từ file_path"""
    safe_id = file_path.replace('/', '_').replace(' ', '_').replace('%', '_').replace('.', '_')
    safe_id = ''.join(c if c.isalnum() or c in ('_', '-') else '_' for c in safe_id)
    safe_id = safe_id[:80]  # Giới hạn độ dài
    return f"{prefix}_{safe_id}" if prefix else safe_id


# ============================================================
# Pipeline cho mỗi file CREATE sử dụng Dynamic Task Mapping
# Mỗi file sẽ có pipeline riêng với các tasks riêng
# ============================================================

# Task 1: Get S3 config (chung cho tất cả files, không phụ thuộc vào process_files_by_type)
def get_s3_config_task_func(**context):
    """Get S3 config - giống nhau cho tất cả files"""
    return get_s3_config_from_airflow_connection('minio_conn')

get_s3_config_task = PythonOperator(
    task_id='get_s3_config',
    python_callable=get_s3_config_task_func,
    dag=dag,
)


# Task 2: Load documents (cho mỗi file CREATE - riêng biệt)
def load_documents_for_file(file_info: Dict[str, Any], s3_config: Any, **context):
    """Load documents cho một file CREATE"""
    file_path = file_info.get('file_path', '')
    bucket_name = file_info.get('bucket_name', 'source-bucket')
    print(f"[{file_path}] Loading documents from bucket: {bucket_name}")
    return load_documents(
        s3_config=s3_config,
        s3_bucket=bucket_name,
        s3_file_path=file_path,
        type='pdf_pypdf'
    )

# Expand để tạo task riêng cho mỗi file CREATE, nhận s3_config từ task chung
load_docs_tasks = PythonOperator.partial(
    task_id='load_documents',
    python_callable=load_documents_for_file,
    dag=dag,
).expand(
    op_kwargs=get_create_files_task.output.map(lambda x: {'file_info': x}),
    op_args=get_s3_config_task.output
)


# Task 3: Chunk documents (cho mỗi file CREATE - riêng biệt)
def chunk_documents_for_file(documents: Any, **context):
    """Chunk documents cho một file CREATE - chỉ cần documents"""
    print("Chunking documents...")
    return chunk_documents(
        documents=documents,
        type='recursive_character'
    )

# Expand để tạo task riêng cho mỗi file, nhận documents từ load task tương ứng
# Không cần get_create_files_task, chỉ cần expand dựa trên load_docs_tasks.output
chunk_docs_tasks = PythonOperator.partial(
    task_id='chunk_documents',
    python_callable=chunk_documents_for_file,
    dag=dag,
).expand(
    op_args=load_docs_tasks.output
)


# Task 4: Get pgvector connection (chung cho tất cả files, không phụ thuộc vào process_files_by_type)
def get_pgvector_conn_task_func(**context):
    """Get pgvector connection - giống nhau cho tất cả files"""
    return parse_pgvector_connection_url('pgvector_conn')

get_pgvector_task = PythonOperator(
    task_id='get_pgvector_conn',
    python_callable=get_pgvector_conn_task_func,
    dag=dag,
)


# Task 5: Index documents (cho mỗi file CREATE - riêng biệt)
def index_documents_for_file(
    chunks: Any,
    connection_url: str,
    **context
):
    """Index documents cho một file CREATE - chỉ cần chunks và connection_url"""
    print("Indexing documents...")
    index_documents_with_embeddings_config(
        type='pgvector',
        documents=chunks,
        embeddings_config={
            'type': 'openai',
            'model': 'text-embedding-3-small',
            'api_key': 'SAMPLE_API'
        },
        collection_name='demo_rag',
        connection_url=connection_url
    )
    return {'status': 'success'}

# Expand để tạo task riêng cho mỗi file CREATE, nhận chunks và connection_url từ các tasks
# Không cần get_create_files_task, chỉ cần expand dựa trên chunk_docs_tasks.output
index_docs_tasks = PythonOperator.partial(
    task_id='index_documents',
    python_callable=index_documents_for_file,
    dag=dag,
).expand(
    op_args=[chunk_docs_tasks.output, get_pgvector_task.output]
)


# ============================================================
# TASKS - Xử lý các type khác (dummy)
# ============================================================

def process_update_files(**context):
    """Dummy function để xử lý các file có type = UPDATE"""
    ti = context['ti']
    files_by_type = ti.xcom_pull(task_ids='process_files_by_type')
    update_files = files_by_type.get('UPDATE', [])
    if not update_files:
        print("Không có file UPDATE nào để xử lý")
        return []
    print(f"Xử lý {len(update_files)} file(s) UPDATE (dummy)")
    for file_info in update_files:
        print(f"  - File: {file_info.get('file_path')}, Type: UPDATE")
    return [{'file_path': f.get('file_path'), 'status': 'processed'} for f in update_files]


def process_delete_files(**context):
    """Dummy function để xử lý các file có type = DELETE"""
    ti = context['ti']
    files_by_type = ti.xcom_pull(task_ids='process_files_by_type')
    delete_files = files_by_type.get('DELETE', [])
    if not delete_files:
        print("Không có file DELETE nào để xử lý")
        return []
    print(f"Xử lý {len(delete_files)} file(s) DELETE (dummy)")
    for file_info in delete_files:
        print(f"  - File: {file_info.get('file_path')}, Type: DELETE")
    return [{'file_path': f.get('file_path'), 'status': 'processed'} for f in delete_files]


def process_keep_files(**context):
    """Dummy function để xử lý các file có type = KEEP"""
    ti = context['ti']
    files_by_type = ti.xcom_pull(task_ids='process_files_by_type')
    keep_files = files_by_type.get('KEEP', [])
    if not keep_files:
        print("Không có file KEEP nào để xử lý")
        return []
    print(f"Xử lý {len(keep_files)} file(s) KEEP (dummy)")
    for file_info in keep_files:
        print(f"  - File: {file_info.get('file_path')}, Type: KEEP")
    return [{'file_path': f.get('file_path'), 'status': 'processed'} for f in keep_files]


process_update_task = PythonOperator(
    task_id='process_update_files',
    python_callable=process_update_files,
    dag=dag,
)

process_delete_task = PythonOperator(
    task_id='process_delete_files',
    python_callable=process_delete_files,
    dag=dag,
)

process_keep_task = PythonOperator(
    task_id='process_keep_files',
    python_callable=process_keep_files,
    dag=dag,
)


# ============================================================
# TASK DEPENDENCIES
# ============================================================
query_documents_task >> process_files_by_type_task

process_files_by_type_task >> [
    get_create_files_task,
    process_update_task,
    process_delete_task,
    process_keep_task
]

# Pipeline cho CREATE files: mỗi file có pipeline riêng
# get_s3_config (chung) >> load >> chunk >> index
# get_pgvector (chung) >> index
# get_s3_config và get_pgvector_conn KHÔNG phụ thuộc vào process_files_by_type
# Chỉ các tasks xử lý documents (load, chunk, index) mới phụ thuộc vào get_create_files_task

# Dependencies được thiết lập tự động qua op_args trong .expand()
# - load_docs_tasks phụ thuộc vào get_s3_config_task (chung) và get_create_files_task (qua op_kwargs)
# - chunk_docs_tasks phụ thuộc vào load_docs_tasks (qua op_args) - chỉ cần documents
# - index_docs_tasks phụ thuộc vào chunk_docs_tasks và get_pgvector_task (chung) (qua op_args)
get_s3_config_task >> load_docs_tasks
get_create_files_task >> load_docs_tasks
load_docs_tasks >> chunk_docs_tasks
chunk_docs_tasks >> index_docs_tasks
get_pgvector_task >> index_docs_tasks


