# Основний файл (entrypoint) з дагами для Airflow DAG,
# який послідовно запускає файли landing_to_bronze.py, bronze_to_silver.py, silver_to_gold.py
import os
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


BASE_PATH = os.path.dirname(os.path.abspath(__file__))


default_args = {
    "owner": "airflow",
    'start_date': datetime(2025, 11, 16, 0, 0),
}

with DAG(
    "sazhukov_datalake",
    default_args=default_args,
    description="Data Lake ETL pipeline",
    schedule=None,
    catchup=False,
    tags=["sazhukov"],
) as dag:

    # Start task landing_to_bronze.py
    landing_to_bronze = SparkSubmitOperator(
        application=os.path.join(BASE_PATH, 'landing_to_bronze.py'),
        task_id='landing_to_bronze',
        conn_id='spark-default',
        verbose=1,
        dag=dag,
    )

    # Start task bronze_to_silver.py
    bronze_to_silver = SparkSubmitOperator(
        application=os.path.join(BASE_PATH, 'bronze_to_silver.py'),
        task_id='bronze_to_silver',
        conn_id='spark-default',
        verbose=1,
        dag=dag,
    )

    # Start task silver_to_gold.py
    silver_to_gold = SparkSubmitOperator(
        application=os.path.join(BASE_PATH, 'silver_to_gold.py'),
        task_id='silver_to_gold',
        conn_id='spark-default',
        verbose=1,
        dag=dag,
    )


    landing_to_bronze >> bronze_to_silver >> silver_to_gold