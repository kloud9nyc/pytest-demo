import sys
from src.utilities import readBuilder

class SalesTranscationReader(readBuilder.ReadBuilder):

  def __init__(self):
    print("Inside the constructor of SalesTranscationReader ")

  #This is the function that to read readSalesData,
  def readSalesData(self,filename):
    print("Inside the readSalesData")
    try:
      readBuilder.ReadBuilder().withFormat("csv")
      data = readBuilder.ReadBuilder().readCsvData(filename)
      return data
    except Exception as e:
      raise(e,"testRaghu")

  #This is the function that to read readSalesData from Hive,
  def readSalesDataFromHive(self,rawTablename):
    print("Inside the readSalesDataFromHive")
    try:
      data = readBuilder.ReadBuilder().readSalesDataFromHive(rawTablename)
      return data
    except Exception as e:
      raise(e,"testRaghu")