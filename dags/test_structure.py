from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from scripts.metric_summary.tasks import test_main_function
import pendulum


with DAG(
    dag_id="transform_test",
    schedule=None,
    start_date=pendulum.datetime(2025, 3, 26, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    test_py_function = PythonOperator(
        task_id='test_py',
        python_callable=test_main_function,
        dag=dag
    )

    extract_dim_category_job = SparkSubmitOperator(
        task_id='extract_test_1',
        conn_id='spark_conn',
        application='/opt/airflow/dags/repo/scripts/spark/dim_category.py',
        jars='/opt/airflow/jars/gcs-connector-hadoop3-latest.jar',
        driver_class_path='/opt/airflow/jars/',
        py_files='/opt/airflow/dags/repo/scripts/spark/utils/create_session.py',
        dag=dag
    )

    (
        test_py_function >>
        extract_dim_category_job
    )
