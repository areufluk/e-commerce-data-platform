from pyspark.sql import SparkSession
import os


def create_spark_session(app_name="DefaultApp"):
    """
    Creates and returns a Spark session with dynamic configurations.
    :param app_name: Name of the Spark application.
    :param master: Spark master URL (e.g., 'local[*]', 'spark://master:7077').
    :return: SparkSession object.
    """
    builder = SparkSession.builder.appName(app_name)

    GCS_CONNECTOR_JAR_PATH = "jar/gcs-connector-hadoop3-latest.jar"
    SERVICE_ACCOUNT_KEY_ID = os.environ.get("SA_KEY_ID")
    SERVICE_ACCOUNT_EMAIL = os.environ.get("SA_EMAIL")
    SERVICE_ACCOUNT_PRIVATE_KEY = os.environ.get("SA_PRIVATE_KEY")

    spark_configs = {
        "spark.jars": GCS_CONNECTOR_JAR_PATH,
        "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        "spark.hadoop.fs.gs.auth.service.account.enable": "true",
        "fs.gs.auth.service.account.private.key.id": SERVICE_ACCOUNT_KEY_ID,
        "fs.gs.auth.service.account.email": SERVICE_ACCOUNT_EMAIL,
        "fs.gs.auth.service.account.private.key": SERVICE_ACCOUNT_PRIVATE_KEY
    }

    for key, value in spark_configs.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()
