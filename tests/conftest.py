""" General configuration file for pytest with fixtures (mocked data/methods) the filename needs to be conftest.py
"""

import logging
import pytest

from pyspark.sql import SparkSession

def quiet_py4j():
    """ turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.ERROR)


@pytest.fixture(scope="session")
def spark_context(request):
    """ fixture for creating a spark context
     Args:
         request: pytest.FixtureRequest object

     """
    quiet_py4j()

    sparkSession = (
        SparkSession
            .builder
            .appName("myspark")
            .master("[*]")
            .config("hive.metastore.uris", "thrift://localhost:9083")
            .enableHiveSupport()
            .getOrCreate()
    )
    request.addfinalizer(lambda: sparkSession.stop())
    return sparkSession


@pytest.fixture(scope="session")
def input_dataframe_data(spark_context):
    """ fixture for creating a test data frame
    :type spark_context: object
    :param spark_context:
    :return:
    """
    input_mock_data = spark_context.read.csv("hdfs://localhost:8020/sales/data/test_data.csv",
                                            inferSchema=True, header=True)
    return input_mock_data
