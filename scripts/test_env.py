import pandas as pd


def top_ten_products_sales():
    df_sales = pd.read_csv(
        'gs://e-commerce-data-platform/raw/sales.csv',
        storage_options={'token': '/opt/airflow/secrets/service-account.json'}
    )

    df_sales = df_sales.groupby("ProductID").agg({"Quantity": "sum"}).reset_index()

    df_product = pd.read_csv(
        'gs://e-commerce-data-platform/raw/products.csv',
        storage_options={'token': '/opt/airflow/secrets/service-account.json'}
    )
    df_sales = df_sales.merge(df_product, left_on="ProductID", right_on="ProductID", how="left")

    df_sales["SalesTotal"] = df_sales["Price"] * df_sales["Quantity"]
    df_sales = df_sales.sort_values("SalesTotal", ascending=False)

    print(df_sales[["ProductName", "SalesTotal"]].head(10))


def top_ten_salesperson_sales():
    df_sales = pd.read_csv(
        'gs://e-commerce-data-platform/raw/sales.csv',
        storage_options={'token': '/opt/airflow/secrets/service-account.json'}
    )

    df_sales = df_sales.groupby("SalesPersonID").agg({"Quantity": "sum"}).reset_index()

    df_salesperson = pd.read_csv(
        'gs://e-commerce-data-platform/raw/employees.csv',
        storage_options={'token': '/opt/airflow/secrets/service-account.json'}
    )
    df_sales = df_sales.merge(df_salesperson, left_on="SalesPersonID", right_on="EmployeeID", how="left")

    df_sales["SalesTotal"] = df_sales["Price"] * df_sales["Quantity"]
    df_sales = df_sales.sort_values("SalesTotal", ascending=False)

    print(df_sales[["FirstName", "SalesTotal"]].head(10))