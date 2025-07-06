from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

from scripts.test_env import top_ten_products_sales, top_ten_salesperson_sales


with DAG(
    dag_id="pandas_sales",
    schedule=None,
    start_date=pendulum.datetime(2025, 7, 5, tz="Asia/Bangkok"),
    catchup=False
) as dag:
    most_product_sales = PythonOperator(
        task_id="top_ten_products_sales",
        python_callable=top_ten_products_sales,
    )

    most_salesperson_sales = PythonOperator(
        task_id="top_ten_salesperson_sales",
        python_callable=top_ten_salesperson_sales,
    )

    (most_product_sales >> most_salesperson_sales)
