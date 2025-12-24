"""
DAG demo sử dụng các hàm gộp trong thư mục rag-function
để thực hiện pipeline tương tự newdag.py.

Tham số đầu vào chính: s3_file_path
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from rag_function.load import load_documents_task
from rag_function.chunk import chunk_documents_task
from rag_function.index import index_documents_task

# Các giá trị mặc định cho DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Định nghĩa DAG với param s3_file_path để có thể đổi file đầu vào
dag = DAG(
    dag_id="demo-dag-for-rag-workflows",
    default_args=default_args,
    description="Demo RAG pipeline dùng hàm gộp trong rag-function",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "rag"],
    params={
        # Giá trị mặc định có thể override khi trigger DAG
        "s3_file_path": "BankDocument/BankRelationships.pdf",
    },
)


task_load = PythonOperator(
    task_id="load_documents",
    python_callable=load_documents_task,
    op_kwargs={
        "conn_id": "minio_conn",
        "s3_bucket": "demo-rag",
        # Cho phép override thông qua params
        "s3_file_path": "{{ params.s3_file_path }}",
        "type": "pdf_pypdf",
    },
    dag=dag,
)

task_chunk = PythonOperator(
    task_id="chunk_documents",
    python_callable=chunk_documents_task,
    op_kwargs={
        "documents": task_load.output,
        "type": "recursive_character",
    },
    dag=dag,
)

task_index = PythonOperator(
    task_id="index_documents",
    python_callable=index_documents_task,
    op_kwargs={
        "conn_id": "pgvector_conn",
        "documents": task_chunk.output,
        "type": "pgvector",
        "embeddings_config": {
            "type": "openai",
            "model": "text-embedding-3-small",
            # Thay bằng API key thực tế hoặc sử dụng biến môi trường/Connection
            "api_key": "SAMPLE_API",
        },
        "collection_name": "demo_rag",
    },
    dag=dag,
)

task_load >> task_chunk >> task_index
