from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# Import your Kafka producer function
from src.kafka_producer import kafka_producer

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'Streaming_Producer',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Run your producer
    produce_to_kafka = PythonOperator(
        task_id='produce_traffic_data',
        python_callable=kafka_producer
    )

    