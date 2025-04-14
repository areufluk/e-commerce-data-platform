from spark.utils.create_session import create_spark_session


def transform_dim_category():
    bucket_name = "e-commerce-data-platform"
    source_file_path = f"gs://{bucket_name}/raw/categories.csv"
    destination_file_path = f"gs://{bucket_name}/transform/category"

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
        .withColumnRenamed("CategoryID", "id") \
        .withColumnRenamed("CategoryName", "category_name")

    # Write parquet to transform storage
    df.write \
        .mode("overwrite") \
        .parquet(destination_file_path)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    transform_dim_category()
