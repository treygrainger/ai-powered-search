
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

def get_spark_session():
    conf = SparkConf()
    conf.set("spark.driver.memory", "7g")
    conf.set("spark.executor.memory", "7g")
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.ui.port", "4040")
    conf.set("spark.dynamicAllocation.executorMemoryOverhead", "7g")
    return SparkSession.builder.appName("AIPS").config(conf=conf).getOrCreate()

def create_view_from_collection(collection, view_name, spark=None, log=False):
    return collection.create_view_from_collection(view_name, spark=spark or get_spark_session(), log=log)