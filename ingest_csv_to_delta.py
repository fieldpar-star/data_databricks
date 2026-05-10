import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

urls = {
    "airports":   "https://raw.githubusercontent.com/fieldpar-star/data_databricks/refs/heads/main/airports.csv",
    "bookings":   "https://raw.githubusercontent.com/fieldpar-star/data_databricks/refs/heads/main/bookings.csv",
    "passengers": "https://raw.githubusercontent.com/fieldpar-star/data_databricks/refs/heads/main/passengers.csv",
}

catalog = "bloomsbdcatalog"
schema  = "raw"

for table_name, url in urls.items():
    print(f"Processing {table_name}...")

    # Fetch with Pandas
    pdf = pd.read_csv(url)
    print(f"  Rows: {len(pdf)}, Columns: {list(pdf.columns)}")

    # Convert to Spark DataFrame
    sdf = spark.createDataFrame(pdf)

    # Write as Delta table
    full_table = f"{catalog}.{schema}.{table_name}"
    sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table)

    print(f"  Created table: {full_table}")

print("\nAll tables created successfully!")
