""" pytest fixtures that can be resued across tests. the filename needs to be test_dummy_data.py
"""
import pytest
from src.jobs.processSalesData import ProceeSalesData
from src.jobs.transformSalesDataFromRawToClean import TransformSalesData

from pyspark.sql import functions as sf
import pyspark
import sys

@pytest.fixture(scope="session")
def setup(input_dataframe_data):
    return input_dataframe_data

@pytest.fixture(scope="session")
def setup_sparksession(spark_context):
    return spark_context


# def test_readData():
#     print("test_readData Called")
#     csvfile = "hdfs://localhost:8020/sales/data/test_data.csv"
#     processData = ProceeSalesData()
#     csvdata = processData.readData(csvfile)
#     count = csvdata.count()
#     assert count == 18


#def test_readdata_fail():
#    print("test_readdata_fail Called")
#    csvfile = "file:///Users/raghunathan.bakkianathan/Work/test_data.csv"
#    processData = ProceeSalesData()
#    csvdata = processData.readData(csvfile)
#    count = csvdata.count()
#    assert count == 1

# def test_exception_transformation_readData_withWrongTableName():
#     print("test_readData Called")
#     with pytest.raises(Exception) as excinfo:
#         wrongTableName = "xyz"
#         transformData = TransformSalesData()
#         csvdata = transformData.readData(wrongTableName)
#         assert str(excinfo.value) == 'testRaghu'


# def test_readdata_with_fixture(setup):
#     print("test_readdata_with_fixture Called")
#     csvfile = "hdfs://localhost:8020/sales/data/test_data.csv"
#     expected_count = setup.count()
#     processData = ProceeSalesData()
#     csvdata = processData.readData(csvfile)
#     count = csvdata.count()
#     assert count == expected_count


def test_storedata(setup,setup_sparksession):
    print("test_storedata Called")
    testdata = setup
    processData = ProceeSalesData()
    tablename= "raw_sales_db.raw_sales_transcation_test"
    orginaldata = setup_sparksession.sql("""select * from raw_sales_db.raw_sales_transcation_test""")
    orginalCount = orginaldata.count()
    processData.storeData(testdata,tablename)
    data = setup_sparksession.sql("""select * from raw_sales_db.raw_sales_transcation_test""")
    count = data.count()
    finalcount = count - orginalCount
    assert finalcount == testdata.count()

def test_exception_storedata(setup,setup_sparksession):
    print("test_storedata Called")
    testdata = setup
    processData = ProceeSalesData()

    with pytest.raises(Exception) as excinfo:
        print("entered")
        tablename= "raw_sales_db.raw_sales_transcation_test1123"
        processData.storeData(testdata,tablename)
        assert str(excinfo.value) == 'testRaghu'

def test_exception_readData():
    print("test_readData Called")
    with pytest.raises(Exception) as excinfo:
        csvfile = "hdfs://localhost:8020/sales/data/test_data.csv"
        processData = ProceeSalesData()
        csvdata = processData.readData(csvfile)
        assert str(excinfo.value) == 'testRaghu'



def test_doTransformation(setup):
    print("test_doTransformation Called")

    transformData = TransformSalesData()
    cleanData = transformData.doTransformation(setup)
    actual_result = cleanData.count()

    setup = setup.withColumn("UniqueKey",sf.concat(sf.col('invoiceno'),sf.lit('_'), sf.col('customerid')))
    setup = setup.distinct()
    expected_count = setup.count()
    assert actual_result == expected_count


