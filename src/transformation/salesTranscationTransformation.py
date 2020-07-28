import sys
from pyspark.sql import functions as sf
class SalesTranscationTransformation():

  def __init__(self):
    print("Inside the constructor of SalesTranscationTransformation ")

  #This is the function that to read readSalesData,
  def transformSalesData(self,dataframe):
    print("Inside the transformSalesData")
    try:
        dataframe = dataframe.withColumn("UniqueKey",sf.concat(sf.col('invoiceno'),sf.lit('_'), sf.col('customerid')))
        dataframe = dataframe.distinct()
        return dataframe
    except Exception as e:
        raise(e,"testRaghu")
