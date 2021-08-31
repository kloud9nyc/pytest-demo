# import spark Related pkgs from pyspark.sql(many classes inside)
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql import HiveContext

if __name__ == "__main__":
    print("welcome to pyspark")

spark = SparkSession \
    .builder \
    .appName("priceApp") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

print("now stopping the spark session object...")

bookingsSchema = StructType( \
    [StructField("id", IntegerType(), True),
     StructField("name", StringType(), True),
     StructField("host_id", IntegerType(), True),
     StructField("host_name", StringType(), True),
     StructField("neighbourhood_group", StringType(), True),
     StructField("neighbourhood", StringType(), True),
     StructField("latitude", StringType(), True),
     StructField("longitude", StringType(), True),
     StructField("room_type", StringType(), True),
     StructField("price", IntegerType(), True),
     StructField("minimum_nights", IntegerType(), True),
     StructField("number_of_reviews", IntegerType(), True),
     StructField("last_review", StringType(), True),
     StructField("reviews_per_month", FloatType(), True),
     StructField("calculated_host_listings_count", IntegerType(), True),
     StructField("availability_365", IntegerType(), True),
     ])

df = spark.read.option("header", "true").schema(bookingsSchema).csv("file:////Users/nithya/data/bookings.csv")

bookings_Bronx = df.select("id", "name", "neighbourhood_group", "number_of_reviews").where(
    "neighbourhood_group=='Bronx'")

# bookings_Bronx.show(20)

bookings_Bronx_2 = df.select("id", "name", "neighbourhood_group", "number_of_reviews").filter("neighbourhood_group"
                                                                                              "=='Bronx'")

# bookings_Bronx_2.show(20)

bookings_Bronx_3 = bookings_Bronx.filter("number_of_reviews > 200")
#
# bookings_Bronx_3.show(20)


bookings_Bronx_3.write.csv("hdfs://localhost:9000/bigdata_results/")





spark.stop()
