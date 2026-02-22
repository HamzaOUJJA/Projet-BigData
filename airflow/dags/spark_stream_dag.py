from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'Spark_Streaming',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    task_spark = BashOperator(
        task_id='run_spark_streaming',
        bash_command=(
            'docker exec -u 0 spark-master /opt/spark/bin/spark-submit '
            '--master spark://spark-master:7077 '
            '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 '
            '/opt/airflow/src/Spark_Streaming.py'
        )
    )
    