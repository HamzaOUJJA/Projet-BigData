from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'Spark_Batch',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    task_batch_spark = BashOperator(
        task_id='run_spark_batch',
        bash_command=(
            'docker exec -u 0 spark-master /opt/spark/bin/spark-submit '
            '--master spark://spark-master:7077 '
            '--packages org.apache.hadoop:hadoop-aws:3.3.4 '
            '/opt/airflow/src/spark_batch.py'
        )
    )
    