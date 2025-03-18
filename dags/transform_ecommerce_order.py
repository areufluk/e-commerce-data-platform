from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
# import os

from dags.scripts.test_env import show_env


# def show_env():
#     st_env_from_kube = os.environ.get("AIRFLOW_DB_NAME")
#     nd_env_from_kube = os.environ.get("SUPERSET_DB_USER")
#     print("first env: ", st_env_from_kube)
#     print("second env: ", nd_env_from_kube)

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
