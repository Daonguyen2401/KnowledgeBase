"""
Simple RAG DAG with 3 tasks printing Hello World
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pinwheel.s3_utils import get_s3_config_from_airflow_connection, get_file_from_s3
from pinwheel.database_utils import get_postgres_conn_info

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
    'example_rag_dag',
    default_args=default_args,
    description='A simple RAG DAG ',
    schedule=timedelta(days=1),  # Run daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'rag', 'hello-world'],
)

# Define the function to print Hello World
def print_hello_world_task1():
    """Print Hello World - Task 1"""
    print("Hello World - Task 1")
    return "Hello World - Task 1"

def print_hello_world_task2():
    """Print Hello World - Task 2"""
    print("Hello World - Task 2")
    return "Hello World - Task 2"


def download_file_from_s3(conn_id: str):
    """Download file from S3 using Airflow Connection"""
    # Lấy config từ Airflow Connection
    # Lưu ý: Cần tạo connection với ID 's3_connection' trong Airflow UI
    s3_config = get_s3_config_from_airflow_connection(conn_id)
    
    # Download file từ S3
    print(s3_config)
    return s3_config

def get_postgres_conn_info_task(conn_id: str):
    """Get Postgres connection info from Airflow Connection"""
    from pinwheel.database_utils import get_postgres_conn_info
    postgres_conn_info = get_postgres_conn_info(conn_id)
    print(postgres_conn_info)
    return postgres_conn_info

# Create the tasks
task_1 = PythonOperator(
    task_id='print_hello_world_task1',
    python_callable=print_hello_world_task1,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='print_hello_world_task2',
    python_callable=print_hello_world_task2,
    dag=dag,
)

qdrant = PythonOperator(
    task_id=f'get_qdrant_conn_id_info',
    python_callable=get_postgres_conn_info_task,
    op_kwargs={'conn_id': 'Qdrant_conn'},
    dag=dag,
)

task_4 = PythonOperator(
    task_id='download_file_from_s3',
    python_callable=download_file_from_s3,
    op_kwargs={'conn_id': 'minio_conn'},
    dag=dag,
)

# Set task dependencies (sequential execution)
task_1 >> task_2 >> qdrant >> task_4

