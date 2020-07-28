# import spark Related pkgs from pyspark.sql(many classes inside)
import sys
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext

if __name__ == "__main__":
    print ("welcome to pyspark")

spark = SparkSession \
.builder \
.appName ("prodApp") \
.master("local[*]") \
.config("hive.metastore.uris", "thrift://localhost:9083") \
.enableHiveSupport() \
.getOrCreate()
print (spark)
print(spark.sparkContext)
print(spark.sparkContext.appName)
print("now stopping the spark session object...")
df = spark.read.csv("file:////Users/kureshuchandra.n/PycharmProjects/best-practice/src/data/sales.csv")
df.show()
spark.sql("show databases").show(10,False)
#hive_context = HiveContext(spark)
#bank = hive_context.table("raw_sales_db.sales_transcation")
#bank.show()
spark.stop()