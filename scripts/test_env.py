import os


def print_env_from_configmap():
    print(os.getenv("AIRFLOW_DB_NAME"))
