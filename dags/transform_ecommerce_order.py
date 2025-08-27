from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum


with DAG(
    dag_id="transform_grocery_sales",
    schedule=None,
    start_date=pendulum.datetime(2025, 3, 26, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    extract_dim_category_job = SparkSubmitOperator(
        task_id='extract_dim_category',
        conn_id='spark_conn',
        application='/opt/airflow/dags/repo/scripts/spark/dim_category.py',
        jars='/opt/airflow/jars/gcs-connector-hadoop3-latest.jar',
        driver_class_path='/opt/airflow/jars/',
        py_files='/opt/airflow/dags/repo/scripts/spark/utils/create_session.py',
        dag=dag
    )

    extract_dim_city_job = SparkSubmitOperator(
        task_id='extract_dim_city',
        conn_id='spark_conn',
        application='/opt/airflow/dags/repo/scripts/spark/dim_city.py',
        jars='/opt/airflow/jars/gcs-connector-hadoop3-latest.jar',
        driver_class_path='/opt/airflow/jars/',
        py_files='/opt/airflow/dags/repo/scripts/spark/utils/create_session.py',
        dag=dag
    )

    extract_dim_country_job = SparkSubmitOperator(
        task_id='extract_dim_country',
        conn_id='spark_conn',
        application='/opt/airflow/dags/repo/scripts/spark/dim_country.py',
        jars='/opt/airflow/jars/gcs-connector-hadoop3-latest.jar',
        driver_class_path='/opt/airflow/jars/',
        py_files='/opt/airflow/dags/repo/scripts/spark/utils/create_session.py',
        dag=dag
    )

    extract_dim_customer_job = SparkSubmitOperator(
        task_id='extract_dim_customer',
        conn_id='spark_conn',
        application='/opt/airflow/dags/repo/scripts/spark/dim_customer.py',
        jars='/opt/airflow/jars/gcs-connector-hadoop3-latest.jar',
        driver_class_path='/opt/airflow/jars/',
        py_files='/opt/airflow/dags/repo/scripts/spark/utils/create_session.py',
        dag=dag
    )

    extract_dim_employee_job = SparkSubmitOperator(
        task_id='extract_dim_employee',
        conn_id='spark_conn',
        application='/opt/airflow/dags/repo/scripts/spark/dim_employee.py',
        jars='/opt/airflow/jars/gcs-connector-hadoop3-latest.jar',
        driver_class_path='/opt/airflow/jars/',
        py_files='/opt/airflow/dags/repo/scripts/spark/utils/create_session.py',
        dag=dag
    )

    extract_dim_product_job = SparkSubmitOperator(
        task_id='extract_dim_product',
        conn_id='spark_conn',
        application='/opt/airflow/dags/repo/scripts/spark/dim_product.py',
        jars='/opt/airflow/jars/gcs-connector-hadoop3-latest.jar',
        driver_class_path='/opt/airflow/jars/',
        py_files='/opt/airflow/dags/repo/scripts/spark/utils/create_session.py',
        dag=dag
    )

    extract_fact_grocery_sales_job = SparkSubmitOperator(
        task_id='extract_fact_grocery_sales',
        conn_id='spark_conn',
        application='/opt/airflow/dags/repo/scripts/spark/fact_grocery_sales.py',
        jars='/opt/airflow/jars/gcs-connector-hadoop3-latest.jar',
        driver_class_path='/opt/airflow/jars/',
        py_files='/opt/airflow/dags/repo/scripts/spark/utils/create_session.py',
        dag=dag
    )

    soda_quality_check = BashOperator(
        task_id="soda_quality_check",
        bash_command="soda scan -d grocery_sales -c /opt/airflow/dags/repo/scripts/soda/configuration.yml /opt/airflow/dags/repo/scripts/soda/grocery_sales/checks.yml"
    )

    (
        extract_dim_category_job
        >> extract_dim_city_job
        >> extract_dim_country_job
        >> extract_dim_customer_job
        >> extract_dim_employee_job
        >> extract_dim_product_job
        >> extract_fact_grocery_sales_job
        >> soda_quality_check
    )
