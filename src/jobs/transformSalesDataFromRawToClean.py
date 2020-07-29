import sys
from src.reading import salesTranscationReader
from src.writing import salesTranscationWriter
from src.transformation import salesTranscationTransformation
from src.context import sparkcontext
import configparser

class TransformSalesData(salesTranscationReader.SalesTranscationReader,salesTranscationWriter.SalesTranscationWriter):

  def __init__(self):
    print("Inside the constructor of Class TransformSalesData ")


  #This is the function is to read the csv data,
  def readData(self,rawTablename):
    try:
      print("Inside the readData of TransformSalesData ")
      hiveData = salesTranscationReader.SalesTranscationReader().readSalesDataFromHive(rawTablename)
      return hiveData
    except Exception as e:
      print("Error while storing the reading the data : ", e.__class__, "occurred.")

  #T his is the function is to transferm the data
  def doTransformation(self,data):
    try:
      print("Inside the doTransfermation ")
      data = salesTranscationTransformation.SalesTranscationTransformation().transformSalesData(data)
      return data
    except Exception as e:
      print("Error while doing the transormation : ", e.__class__, "occurred.")

  # This is the function is to store the clean data to hive,
  def storeData(self,data,cleanTable):
    try:
      status = salesTranscationWriter.SalesTranscationWriter().writeSalesData(data,cleanTable)
      return status
    except Exception as e:
      print("Error while storing the clean data : ", e.__class__, "occurred.")

config = configparser.RawConfigParser()
config.read('../properties/config.properties')
rawTablename = config.get("General", "rawSalesDB") + "."+ config.get("General", "rawSalesTable")
transformData = TransformSalesData()
data = transformData.readData(rawTablename)
cleanData = transformData.doTransformation(data)
cleanTable = config.get("General", "cleanSalesDB") + "."+ config.get("General", "cleanSalesTable")
transformData.storeData(cleanData,cleanTable)