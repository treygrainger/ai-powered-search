from pyspark.sql import SparkSession
from aips.environment import AIPS_ZK_HOST

def create_view_from_collection(collection, view_name, spark=None):
    if not spark:
        spark = SparkSession.builder.appName("AIPS").getOrCreate()
    opts = {"zkhost": AIPS_ZK_HOST, "collection": collection.name}    
    spark.read.format("solr").options(**opts).load().createOrReplaceTempView(view_name)