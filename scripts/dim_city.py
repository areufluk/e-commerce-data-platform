from scripts.create_session import create_spark_session


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
