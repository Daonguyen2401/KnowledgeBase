"""
Simple RAG DAG with 3 tasks printing Hello World
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pinwheel.s3_utils import get_s3_config_from_airflow_connection, get_file_from_s3
from pinwheel.database_utils import get_postgres_conn_info
from rag.loading.loading_builder import load_documents

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
    op_kwargs= {'s3_config': task_1.output, 's3_bucket': 'demo-rag', 's3_file_path': 'Bankdocument/BankRelationships.pdf', 'type': 'pdf_pypdf'},
    dag=dag,
)
task_1>> task_2


