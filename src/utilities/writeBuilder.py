import sys
from src.context import sparkcontext

class WriteBuilder(sparkcontext.GetSparkSession):

  def __init__(self):
    print("Inside the constructor of Class phases ")
    try:
      self.spark = sparkcontext.GetSparkSession().get_instance()
    except Exception as e:
        raise(e,"testRaghu")


  def writeDataToHive(self,data,tablename):

    try:
        temp_data = data.createOrReplaceTempView("temp_data")
        status = self.spark.sql("insert into table {0} select * from temp_data".format(tablename))
        return status
    except Exception as e:
        raise(e,"testRaghu")