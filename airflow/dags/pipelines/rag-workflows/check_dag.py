"""
DAG để query document_metadata table và xác định các file mới upload từ MinIO
"""
from datetime import datetime, timedelta
from typing import List, Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from pinwheel.document_event import query_new_documents
from rag_function.load import get_loading_config
from rag_function.chunk import get_chunking_config
from rag_function.index import get_indexing_config
from rag_function.embedding import get_embedding_config
from rag_function.execution import execute_delete_documents_by_metadata, run_indexing_pipeline
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
    'check_document_metadata',
    default_args=default_args,
    description='Query document_metadata table to get newly uploaded files from MinIO',
    schedule=timedelta(hours=1),  # Run every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['minio', 'document-metadata', 'monitoring'],
)

get_loading_config_task = PythonOperator(
    task_id='get_loading_config',
    python_callable=get_loading_config,
    op_kwargs={
        'conn_id': 'minio_conn',
        's3_bucket': 'source-bucket',
        'type': 'pdf_pypdf',
    },
    dag=dag,
)
get_chunking_config_task = PythonOperator(
    task_id='get_chunking_config',
    python_callable=get_chunking_config,
    op_kwargs={
        'type': 'recursive_character',
    },
    dag=dag,
)
get_indexing_config_task = PythonOperator(
    task_id='get_indexing_config',
    python_callable=get_indexing_config,
    op_kwargs={
        'conn_id': 'pgvector_conn',
        'collection_name': 'demo_rag',
        'type': 'pgvector',
    },
    dag=dag,
)
get_embedding_config_task = PythonOperator(
    task_id='get_embedding_config',
    python_callable=get_embedding_config,
    op_kwargs={
        'type': 'openai',
        'model': 'text-embedding-3-small',
        'api_key': 'SAMPLE_API',
    },
    dag=dag,
)
# Tạo task
query_documents_task = PythonOperator(
    task_id='query_new_documents',
    python_callable=query_new_documents,
    op_kwargs={
        'conn_id': 'pgvector_conn',
        'dag_id': dag.dag_id,
        'bucket_name': "{{ ti.xcom_pull(task_ids='get_loading_config').get('s3_bucket') }}",
        'table_name': None,
    },
    dag=dag,
)
execute_delete_documents_by_metadata_task = PythonOperator(
    task_id='execute_delete_documents_by_metadata',
    python_callable=execute_delete_documents_by_metadata,
    op_kwargs={
        'conn_id': 'pgvector_conn',
        'type': 'pgvector',
        'query_result': "{{ ti.xcom_pull(task_ids='query_new_documents') }}",
        'collection_name': "{{ ti.xcom_pull(task_ids='get_indexing_config').get('collection_name') }}",
        'embeddings_config': "{{ ti.xcom_pull(task_ids='get_embedding_config') }}",
    },
    dag=dag,
)

run_indexing_pipeline_task = PythonOperator(
    task_id='run_indexing_pipeline',
    python_callable=run_indexing_pipeline,
    op_kwargs={
        'loading_config': "{{ ti.xcom_pull(task_ids='get_loading_config') }}",
        'chunking_config': "{{ ti.xcom_pull(task_ids='get_chunking_config') }}",
        'embedding_config': "{{ ti.xcom_pull(task_ids='get_embedding_config') }}",
        'indexing_config': "{{ ti.xcom_pull(task_ids='get_indexing_config') }}",
        'query_result': "{{ ti.xcom_pull(task_ids='query_new_documents') }}",
    },
    dag=dag,
)

# Thiết lập dependency
get_loading_config_task >> query_documents_task
query_documents_task >> execute_delete_documents_by_metadata_task
query_documents_task >> run_indexing_pipeline_task
get_indexing_config_task >> execute_delete_documents_by_metadata_task
get_indexing_config_task >> run_indexing_pipeline_task
get_embedding_config_task >> execute_delete_documents_by_metadata_task
get_embedding_config_task >> run_indexing_pipeline_task
get_chunking_config_task >> run_indexing_pipeline_task
get_loading_config_task >> run_indexing_pipeline_task
# Chạy indexing pipeline sau khi delete xong (để đảm bảo delete documents cũ trước khi index lại)
execute_delete_documents_by_metadata_task >> run_indexing_pipeline_task

