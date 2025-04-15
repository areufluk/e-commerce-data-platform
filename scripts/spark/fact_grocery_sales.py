from create_session import create_spark_session
from pyspark.sql.functions import round, col, date_format, dayofweek, dayofyear, weekofyear, \
    month, quarter, year, expr, lit, when, to_date, hour, minute, second


def transform_fact_grocery_sales():
    bucket_name = "e-commerce-data-platform"
    source_file_path = f"gs://{bucket_name}/raw/sales.csv"
    product_file_path = f"gs://{bucket_name}/transform/product"
    fact_sale_file_path = f"gs://{bucket_name}/transform/fact_grocery_sales"
    dim_date_file_path = f"gs://{bucket_name}/transform/dim_date"
    dim_time_file_path = f"gs://{bucket_name}/transform/dim_time"

    spark = create_spark_session(
        app_name="Transform fact sales"
    )

    # Read csv from raw storage
    df = spark.read \
        .option("inferSchema", True) \
        .option("header", True) \
        .csv(source_file_path)

    # Change column name
    df = df \
        .withColumnRenamed("SalesID", "id") \
        .withColumnRenamed("SalesPersonID", "employee_id") \
        .withColumnRenamed("CustomerID", "customer_id") \
        .withColumnRenamed("ProductID", "product_id") \
        .withColumnRenamed("Quantity", "quantity") \
        .withColumnRenamed("Discount", "discount") \
        .withColumnRenamed("TotalPrice", "total_price") \
        .withColumnRenamed("SalesDate", "sales_date") \
        .withColumnRenamed("TransactionNumber", "transaction_number")

    # Read dim_product from transform storage
    df_dim_product = spark.read \
        .parquet(product_file_path)

    # Join dim_product to calculate total_price
    df = df.join(df_dim_product, df.product_id == df_dim_product.id, "left").select(df['*'], df_dim_product.price)
    df = df.withColumn("total_price", round(((df.price * df.quantity) * (1 - df.discount)), 2))

    # Create date and time key
    df = df \
        .withColumn("date_key", date_format(to_date("sales_date"), "yyyyMMdd").cast("int")) \
        .withColumn("time_key", expr("hour(sales_date) * 10000 + minute(sales_date) * 100 + second(sales_date)"))

    # Write parquet to transform storage
    df.write \
        .mode("overwrite") \
        .parquet(fact_sale_file_path)

    # Extract unique dates and make date dimension
    dim_date = df.withColumn("date", to_date(col("sales_date"))) \
        .select("date").distinct()

    dim_date = dim_date.withColumn("id", date_format("date", "yyyyMMdd").cast("int")) \
        .withColumn("day_of_week", dayofweek("date")) \
        .withColumn("day_name", date_format("date", "EEEE")) \
        .withColumn("day_of_month", date_format("date", "d")) \
        .withColumn("day_of_year", dayofyear("date")) \
        .withColumn("week_of_year", weekofyear("date")) \
        .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), lit(True)).otherwise(lit(False))) \
        .withColumn("month", month("date")) \
        .withColumn("month_name", date_format("date", "MMMM")) \
        .withColumn("quarter", quarter("date")) \
        .withColumn("year", year("date")) \
        .withColumn("month_start_date", expr("trunc(date, 'MONTH')")) \
        .withColumn("month_end_date", expr("last_day(date)")) \
        .withColumn("season", expr("""
            CASE
                WHEN month IN (12, 1, 2) THEN 'Winter'
                WHEN month IN (3, 4, 5) THEN 'Spring'
                WHEN month IN (6, 7, 8) THEN 'Summer'
                ELSE 'Fall'
            END
        """)) \
        .drop("date")

    # Write parquet to transform storage
    dim_date.write \
        .mode("overwrite") \
        .parquet(dim_date_file_path)

    # Extract unique times and make time dimension
    dim_time = df.withColumn("time", date_format("sales_date", "HH:mm:ss")) \
        .select("time").distinct()

    dim_time = dim_time.withColumn("hour", hour("time")) \
        .withColumn("minute", minute("time")) \
        .withColumn("second", second("time")) \
        .withColumn("id", expr("hour * 10000 + minute * 100 + second")) \
        .withColumn("hour_12", expr("CASE WHEN hour = 0 THEN 12 WHEN hour <= 12 THEN hour ELSE hour - 12 END")) \
        .withColumn("am_pm", expr("CASE WHEN hour < 12 THEN 'AM' ELSE 'PM' END")) \
        .withColumn("is_morning", when(col("hour") < 12, lit(True)).otherwise(lit(False))) \
        .withColumn("is_afternoon", when((col("hour") >= 12) & (col("hour") < 18), lit(True)).otherwise(lit(False))) \
        .withColumn("is_evening", when(col("hour") >= 18, lit(True)).otherwise(lit(False))) \
        .withColumn("is_rush_hour", when(col("hour").isin(7, 8, 9, 16, 17, 18), lit(True)).otherwise(lit(False))) \
        .drop("time")

    # Write parquet to transform storage
    dim_time.write \
        .mode("overwrite") \
        .parquet(dim_time_file_path)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    transform_fact_grocery_sales()
