__author__ = "Kloud9"

from pyspark import SQLContext, SparkContext, SparkConf
from pyspark.sql import SparkSession
class GetSparkSession:
    """ A singleton backbone for Spark Context Creation"""

    class __internal:
        """ Implementation of the singleton interface """

        def __init__(self):
            self.conf = SparkConf().setAppName('app-name'). \
                set('spark.executor.memory', '16g'). \
                set('spark.driver.memory', '16g')
            # correct way to initiate spark context
            #self.sc = SparkContext(master='local[10]', conf=self.conf).getOrCreate()
            self.sc = SparkSession \
                     .builder \
                     .master("local[*]") \
                     .appName("how to read csv file") \
                     .config("hive.metastore.uris", "thrift://localhost:9083") \
                     .enableHiveSupport() \
                     .getOrCreate()
            self.sql_context = SQLContext(self.sc)

        def get_instance(self):
            """ instance retrieval method, return spark context """
            return self.sc

        def get_sql_context(self):
            """ instance retrieval method, return sql context for dataframes """
            return self.sql_context

    # storage for the instance reference
    __spark_instance = None

    def __init__(self):
        """ Create singleton Spark instance """
        # Check whether we already have an instance
        if GetSparkSession.__spark_instance is None:
            # Create instance
            GetSparkSession.__spark_instance = GetSparkSession.__internal()

        # Store instance reference as the only member in the handle
        self.__dict__['SparkInstance'] = GetSparkSession.__spark_instance

    def __getattr__(self, attr):
        """ Delegate access to implementation """
        return getattr(self.__spark_instance, attr)