import os


def show_env():
    st_env_from_kube = os.environ.get("AIRFLOW_DB_NAME")
    nd_env_from_kube = os.environ.get("SUPERSET_DB_USER")
    print("first env: ", st_env_from_kube)
    print("second env: ", nd_env_from_kube)
