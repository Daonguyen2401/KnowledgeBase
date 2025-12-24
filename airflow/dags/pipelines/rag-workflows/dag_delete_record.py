"""
DAG để xóa records từ PGVector dựa trên metadata filter.

Tham số đầu vào:
- metadata_filter: Dict chứa các key-value pairs để filter documents cần xóa
  Ví dụ: {"source": "document.pdf"} hoặc {"source": "document.pdf", "page": 1}
- collection_name: Tên collection trong PGVector (mặc định: "demo_rag")
- k: Số lượng documents tối đa để tìm và xóa (mặc định: 100000)
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from pinwheel.database_utils import parse_pgvector_connection_url
from rag.indexing.indexing_builder import get_indexer
from rag.embedding.embedding_builder import create_embedder

# Các giá trị mặc định cho DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def delete_records_by_metadata_task(**context):
    """
    Xóa records từ PGVector dựa trên metadata filter.
    
    Lấy tham số từ DAG params hoặc op_kwargs.
    
    Args:
        **context: Airflow context chứa params và các thông tin khác
    
    Returns:
        Dict với thông tin về số lượng documents đã xóa
    """
    # Lấy params từ DAG run
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf:
        # Nếu có conf từ trigger, ưu tiên dùng conf
        params = dag_run.conf
    else:
        # Nếu không có conf, lấy từ DAG params
        params = context.get("params", {})
    
    # Lấy các tham số từ params hoặc op_kwargs
    metadata_filter = params.get("metadata_filter") or context.get("metadata_filter", {})
    collection_name = params.get("collection_name") or context.get("collection_name", "demo_rag")
    k = params.get("k") or context.get("k", 100000)
    conn_id = context.get("conn_id", "pgvector_conn")
    embeddings_config = context.get("embeddings_config", {})
    
    # Validate metadata_filter
    if not metadata_filter or not isinstance(metadata_filter, dict):
        raise ValueError(
            f"metadata_filter phải là một dict không rỗng. "
            f"Giá trị hiện tại: {metadata_filter} (type: {type(metadata_filter)})"
        )
    
    # Get pgvector connection URL from Airflow connection
    connection_url = parse_pgvector_connection_url(conn_id)
    
    # Tạo embedder (cần để khởi tạo PGVector indexer)
    embeddings = create_embedder(**embeddings_config)
    
    # Tạo indexer
    indexer_creator = get_indexer("pgvector")
    indexer = indexer_creator(
        embedding_model=embeddings,
        collection_name=collection_name,
        connection_url=connection_url,
    )
    
    # Xóa documents theo metadata filter
    print(f"[delete_records_by_metadata] Bắt đầu xóa documents với metadata filter: {metadata_filter}")
    print(f"[delete_records_by_metadata] Collection: {collection_name}, k: {k}")
    
    indexer.delete_by_metadata(metadata_filter=metadata_filter, k=k)
    
    print(f"[delete_records_by_metadata] Đã hoàn thành xóa documents với metadata filter: {metadata_filter}")
    
    return {
        "status": "success",
        "metadata_filter": metadata_filter,
        "collection_name": collection_name,
    }


# Định nghĩa DAG với params để có thể đổi metadata filter khi trigger
dag = DAG(
    dag_id="delete_records_by_metadata_pgvector",
    default_args=default_args,
    description="Xóa records từ PGVector dựa trên metadata filter",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["delete", "pgvector", "metadata"],
    params={
        # Giá trị mặc định có thể override khi trigger DAG
        "metadata_filter": {
            "source": "example.pdf",  # Ví dụ: xóa tất cả documents có source = "example.pdf"
        },
        "collection_name": "demo_rag",
        "k": 100000,  # Số lượng documents tối đa để tìm và xóa
    },
)


task_delete = PythonOperator(
    task_id="delete_records_by_metadata",
    python_callable=delete_records_by_metadata_task,
    op_kwargs={
        "conn_id": "pgvector_conn",
        # Truyền params từ DAG params vào hàm
        "metadata_filter": "{{ params.metadata_filter }}",
        "collection_name": "{{ params.collection_name }}",
        "k": "{{ params.k }}",
        "embeddings_config": {
            "type": "openai",
            "model": "text-embedding-3-small",
            # Thay bằng API key thực tế hoặc sử dụng biến môi trường/Connection
            "api_key": "SAMPLE_API",
        },
    },
    dag=dag,
)

