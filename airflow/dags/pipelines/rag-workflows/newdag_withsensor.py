from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    's3_file_sensor_example',
    default_args=default_args,
    description='A simple DAG that waits for a file in S3',
    schedule="@once",
    catchup=False,
    tags=['example', 's3', 'sensor'],
)

# Task 1: Wait for file to appear in S3
wait_for_s3_file = S3KeySensor(
    task_id='wait_for_s3_file',
    bucket_key='hr/haha.pdf',  # {{ ds }} is execution date in YYYY-MM-DD format
    bucket_name='source-bucket',
    aws_conn_id='minio_conn',  # Connection ID configured in Airflow
    wildcard_match=True,  # allow the *.pdf pattern
    timeout=600,  # Timeout after 10 minutes
    poke_interval=30,  # Check every 30 seconds
    mode='reschedule',  # 'poke' or 'reschedule'
    dag=dag,
)

# Task 2: Process the file once it's available
def process_file(**context):
    execution_date = context['ds']
    print(f"Processing file for date: {execution_date}")
    print("File found! Starting processing...")
    # Add your processing logic here
    return "Processing completed successfully"

process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_file,
    dag=dag,
)

# Task 3: Send completion notification
notify_completion = BashOperator(
    task_id='notify_completion',
    bash_command='echo "Pipeline completed successfully for {{ ds }}"',
    dag=dag,
)

# Define task dependencies
wait_for_s3_file >> process_data >> notify_completion

