from pyspark.sql import SparkSession
import os


def create_spark_session(app_name="DefaultApp"):
    """
    Creates and returns a Spark session with dynamic configurations.
    :param app_name: Name of the Spark application.
    :return: SparkSession object.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.host", "spark-driver.airflow.svc.cluster.local") \
        .config("spark.driver.port", "2222") \
        .config("spark.blockManager.port", "7777") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .getOrCreate()

    SERVICE_ACCOUNT_KEY_ID = os.environ.get("SA_KEY_ID")
    SERVICE_ACCOUNT_EMAIL = os.environ.get("SA_EMAIL")
    SERVICE_ACCOUNT_PRIVATE_KEY = os.environ.get("SA_PRIVATE_KEY")

    builder.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    builder.conf.set("fs.gs.auth.service.account.enable", "true")
    builder.conf.set("fs.gs.auth.service.account.private.key.id", SERVICE_ACCOUNT_KEY_ID)
    builder.conf.set("fs.gs.auth.service.account.email", SERVICE_ACCOUNT_EMAIL)
    builder.conf.set("fs.gs.auth.service.account.private.key", SERVICE_ACCOUNT_PRIVATE_KEY)

    return builder
