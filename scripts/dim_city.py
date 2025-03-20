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


def transform_dim_city():
    bucket_name = "e-commerce-data-platform"
    source_file_path = f"gs://{bucket_name}/raw/cities.csv"
    destination_file_path = f"gs://{bucket_name}/transform/city"

    spark = create_spark_session(
        app_name="Transform dim city"
    )

    # Read csv from raw storage
    df = spark.read \
        .option("inferSchema", True) \
        .option("header", True) \
        .csv(source_file_path)

    # Change column name
    df = df \
        .withColumnRenamed("CityID", "id") \
        .withColumnRenamed("CityName", "city_name") \
        .withColumnRenamed("Zipcode", "zipcode") \
        .withColumnRenamed("CountryID", "country_id")

    # Write parquet to transform storage
    df.write \
        .mode("overwrite") \
        .parquet(destination_file_path)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    transform_dim_city()
