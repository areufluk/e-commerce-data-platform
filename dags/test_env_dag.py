from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.test_env import print_env_from_configmap
import pendulum


with DAG(
    dag_id="print_env",
    schedule=None,
    start_date=pendulum.datetime(2025, 3, 26, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    test_new_env = PythonOperator(
        task_id='extract_dim_category',
        python_callable=print_env_from_configmap,
        dag=dag
    )

    test_new_env
