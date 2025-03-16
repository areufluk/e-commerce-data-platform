from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

from scripts.test_env import show_env


default_args = {
    "owner": "Chanayut"
}

with DAG(
    dag_id="transform_ecommerce_order",
    schedule=None,
    start_date=pendulum.datetime(2025, 3, 16, tz="Asia/Bangkok"),
    catchup=False,
    tags=["test"],
    default_args=default_args
) as dag:
    show_env_task = PythonOperator(
        task_id="show_env",
        python_callable=show_env,
        dag=dag
    )

    show_env_task
