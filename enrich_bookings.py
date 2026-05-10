from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

bookings = spark.table("bloomsbdcatalog.raw.bookings")
passengers = spark.table("bloomsbdcatalog.raw.passengers")
airports = spark.table("bloomsbdcatalog.raw.airports")

enriched = (
    bookings
    .join(passengers, on="passenger_id", how="left")
    .join(airports, on="airport_id", how="left")
)

(
    enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bloomsbdcatalog.enr.bookings_enriched")
)

print("Done: bloomsbdcatalog.enr.bookings_enriched written successfully.")
