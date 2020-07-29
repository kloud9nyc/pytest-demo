import sys
from src.reading import salesTranscationReader
from src.writing import salesTranscationWriter
from src.context import sparkcontext
import os
import configparser

class ProceeSalesData(salesTranscationReader.SalesTranscationReader,salesTranscationWriter.SalesTranscationWriter):

  def __init__(self):
    print("Inside the constructor of Class proceeSalesData ")

  #This is the function is to read the csv data,
  def readData(self,filename):
    try:
      print("Inside the readData of salesTranscationReader ")
      csvdata = salesTranscationReader.SalesTranscationReader().readSalesData(filename)
      return csvdata
    except Exception as e:
      print("Error while reading the data : ", e.__class__, "occurred.")

  # This is the function is to store the csv data to hive,
  def storeData(self,data,rawTable):
    try:
      status = salesTranscationWriter.SalesTranscationWriter().writeSalesData(data,rawTable)
      return status
    except Exception as e:
      print("Error while storing the data : ", e.__class__, "occurred.")


if __name__ == "__main__":
  config = configparser.RawConfigParser()
  config.read('../properties/config.properties')
  csvFile = config.get("General", "readCSVFilePath")
  processData = ProceeSalesData()
  csvdata = processData.readData(csvFile)
  rawTable = config.get("General", "rawSalesDB") + "." + config.get("General", "rawSalesTable")
  processData.storeData(csvdata,rawTable)

