import sys
from src.context import sparkcontext

class ReadBuilder(sparkcontext.GetSparkSession):

  def __init__(self):
    print("Inside the ReadBuilder class")
    try:
      self.spark = sparkcontext.GetSparkSession().get_instance()
    except Exception as e:
      print("Error while reading the spark conetxt : ",e.__class__, "occurred.")
    self.readFormat = ""
    self.readSchema = ""

  #This is the function will set the file format,
  def withFormat(self, fileformat):
    print("Inside the doSomething")
    self.readFormat = fileformat

  #This is the function will set the sechema,
  def withSchema(self, schema):
    print("Inside the doSomething")
    self.readSchema = schema

  # This function is responsible to read csv data
  def readCsvData(self,filename):
    try:
      print("data format" + self.readFormat)
      data = self.spark.read.format(self.readFormat).option("inferSchema", "true").option("header", "true").csv(filename)
      return data
    except Exception as e:
      raise(e,"testRaghu")

  # This function is responsible to read hive data
  def readSalesDataFromHive(self,rawTablename):
    try:
      print("Inside readSalesDataFromHive")
      data = self.spark.sql("select * From " + rawTablename)
      return data
    except Exception as e:
      raise(e,"testRaghu")