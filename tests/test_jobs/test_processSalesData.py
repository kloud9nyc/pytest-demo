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

@pytest.fixture(scope="function")
def setup_readConfig(readConfig):
    return readConfig

@pytest.fixture(scope="function")
def setup_createTestRawTable(createTestRawTable):
    return createTestRawTable


def test_readData(setup_readConfig):
    print("test_readData Called")
    csvfile = setup_readConfig.get("TestingEnv","testreadCSVFilePath") #"hdfs://localhost:8020/sales/data/test_data.csv"
    processData = ProceeSalesData()
    csvdata = processData.readData(csvfile)
    count = csvdata.count()
    assert count == 18

def test_exception_transformation_readData_withWrongTableName():
    with pytest.raises(Exception) as excinfo:
        wrongTableName = "xyz"
        transformData = TransformSalesData()
        csvdata = transformData.readData(wrongTableName)
        assert str(excinfo.value) == 'testRaghu'


def test_readdata_with_fixture(setup,setup_readConfig):
    csvfile = setup_readConfig.get("TestingEnv","testreadCSVFilePath")
    expected_count = setup.count()
    processData = ProceeSalesData()
    csvdata = processData.readData(csvfile)
    count = csvdata.count()
    assert count == expected_count


def test_storedata(setup,setup_sparksession,setup_createTestRawTable):
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
    testdata = setup
    processData = ProceeSalesData()

    with pytest.raises(Exception) as excinfo:
        print("entered")
        tablename= "raw_sales_db.raw_sales_transcation_test1123"
        processData.storeData(testdata,tablename)
        assert str(excinfo.value) == 'testRaghu'

def test_exception_readData(setup_readConfig):
    with pytest.raises(Exception) as excinfo:
        csvfile = setup_readConfig.get("TestingEnv","testreadCSVFilePath") #"hdfs://localhost:8020/sales/data/test_data.csv"
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


