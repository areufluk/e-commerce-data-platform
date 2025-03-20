from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import pendulum


default_args = {
    "owner": "Chanayut"
}

with DAG(
    dag_id="test_spark_submit",
    schedule=None,
    start_date=pendulum.datetime(2025, 3, 19, tz="Asia/Bangkok"),
    catchup=False,
    default_args=default_args
) as dag:

    extract_dim_city_job = SparkSubmitOperator(
        task_id='extract_dim_city',
        conn_id='spark_conn',
        application='/opt/airflow/dags/repo/scripts/dim_city.py',
        jars='jar/gcs-connector-hadoop3-latest.jar',
        driver_class_path='/opt/airflow/jars/',
        dag=dag
    )

    extract_dim_city_job
