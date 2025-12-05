"""
Simple RAG DAG with 3 tasks printing Hello World
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    'dag-for-rag-workflows',
    default_args=default_args,
    description='A simple RAG DAG ',
    schedule=timedelta(days=1),  # Run daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'rag', 'hello-world'],
)

task_1 = PythonOperator(
    task_id='get_s3_config_from_airflow_connection',
    python_callable=get_s3_config_from_airflow_connection,
    op_kwargs={'conn_id': 'minio_conn'},
    dag=dag,
)
task_2 = PythonOperator(
    task_id='load_documents',
    python_callable=load_documents,
    op_kwargs= {'s3_config': task_1.output, 's3_bucket': 'demo-rag', 's3_file_path': 'BankDocument/BankRelationships.pdf', 'type': 'pdf_pypdf'},
    dag=dag,
)
task_3 = PythonOperator(
    task_id='chunk_documents',
    python_callable=chunk_documents,
    op_kwargs={'documents': task_2.output, 'type': 'recursive_character'},
    dag=dag,
)
task_4 = PythonOperator(
    task_id='get_pgvector_conn_info',
    python_callable=parse_pgvector_connection_url,
    op_kwargs={'conn_id': 'pgvector_conn'},
    dag=dag,
)
task_5 = PythonOperator(
    task_id='index_documents',
    python_callable=index_documents_with_embeddings_config,
    op_kwargs={'type': 'pgvector', 'documents': task_3.output, 'embeddings_config': {'type': 'openai', 'model': 'text-embedding-3-small', 'api_key': ''}, 'collection_name': 'demo_rag', 'connection_url': task_4.output},
    dag=dag,
)   
task_1>> task_2 >> task_3 >> task_4 >> task_5


