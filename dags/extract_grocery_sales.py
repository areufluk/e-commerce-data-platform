from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum


with DAG(
    dag_id="test_spark_submit",
    schedule=None,
    start_date=pendulum.datetime(2025, 3, 22, tz="Asia/Bangkok"),
    catchup=False
) as dag:
    
    extract_dim_city_job = SparkSubmitOperator(
        task_id='extract_dim_city',
        conn_id='spark_conn',
        application='/opt/airflow/dags/repo/scripts/dim_city.py',
        jars='/opt/airflow/jars/gcs-connector-hadoop3-latest.jar',
        driver_class_path='/opt/airflow/jars/',
        py_files='/opt/airflow/dags/repo/scripts/create_session.py',
        dag=dag
    )

    extract_dim_city_job
