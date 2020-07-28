import sys
from src.reading import salesTranscationReader
from src.writing import salesTranscationWriter
from src.context import sparkcontext

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




csvFile = "file:///Users/raghunathan.bakkianathan/Downloads/best-practice/src/data/sales.csv"
processData = ProceeSalesData()
csvdata = processData.readData(csvFile)
rawTable = "raw_sales_db.raw_sales_transcation"
processData.storeData(csvdata,rawTable)

