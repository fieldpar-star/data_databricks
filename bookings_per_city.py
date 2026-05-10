from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.getOrCreate()

enriched = spark.table("bloomsbdcatalog.enr.bookings_enriched")

bookings_per_city = (
    enriched
    .groupBy("city")
    .agg(count("booking_id").alias("booking_count"))
    .orderBy("booking_count", ascending=False)
)

(
    bookings_per_city.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bloomsbdcatalog.cur.bookings_per_city")
)

print("Done: bloomsbdcatalog.cur.bookings_per_city written successfully.")
