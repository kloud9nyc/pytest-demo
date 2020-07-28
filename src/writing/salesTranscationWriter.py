import sys
from src.utilities import writeBuilder

class SalesTranscationWriter(writeBuilder.WriteBuilder):

  def __init__(self):
    print("Inside the constructor of SalesTranscationWriter ")

  #This is the function that is to write sales data,
  def writeSalesData(self,data,tablename):
    print("Inside the writeSalesData",tablename)
    try:
      status = writeBuilder.WriteBuilder().writeDataToHive(data,tablename)
      return status
    except Exception as e:
      raise(e,"testRaghu")