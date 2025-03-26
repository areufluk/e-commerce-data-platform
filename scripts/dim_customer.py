from .create_session import create_spark_session


def transform_dim_customer():
    bucket_name = "e-commerce-data-platform"
    source_file_path = f"gs://{bucket_name}/raw/customers.csv"
    destination_file_path = f"gs://{bucket_name}/transform/customer"

    spark = create_spark_session(
        app_name="Transform dim customer"
    )

    # Read csv from raw storage
    df = spark.read \
        .option("inferSchema", True) \
        .option("header", True) \
        .csv(source_file_path)

    # Change column name
    df = df \
        .withColumnRenamed("CustomerID", "id") \
        .withColumnRenamed("FirstName", "first_name") \
        .withColumnRenamed("MiddleInitial", "middle_initial") \
        .withColumnRenamed("LastName", "last_name") \
        .withColumnRenamed("CityID", "city_id") \
        .withColumnRenamed("Address", "address")

    # Write parquet to transform storage
    df.write \
        .mode("overwrite") \
        .parquet(destination_file_path)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    transform_dim_customer()
