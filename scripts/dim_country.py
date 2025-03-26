from .create_session import create_spark_session


def transform_dim_country():
    bucket_name = "e-commerce-data-platform"
    source_file_path = f"gs://{bucket_name}/raw/countries.csv"
    destination_file_path = f"gs://{bucket_name}/transform/country"

    spark = create_spark_session(
        app_name="Transform dim country"
    )

    # Read csv from raw storage
    df = spark.read \
        .option("inferSchema", True) \
        .option("header", True) \
        .csv(source_file_path)

    # Change column name
    df = df \
        .withColumnRenamed("CountryID", "id") \
        .withColumnRenamed("CountryName", "country_name") \
        .withColumnRenamed("CountryCode", "country_code")

    # Write parquet to transform storage
    df.write \
        .mode("overwrite") \
        .parquet(destination_file_path)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    transform_dim_country()
