from pyspark.sql import SparkSession

from aips.environment import AIPS_ZK_HOST
from engines.opensearch.config import OPENSEARCH_URL

def create_view_from_collection(collection, view_name, spark=None):
    if not spark:
        spark = SparkSession.builder.appName("AIPS").getOrCreate()
    match collection.get_engine_name():
        case "solr":
            opts = {"zkhost": AIPS_ZK_HOST, "collection": collection.name}    
            spark.read.format("solr").options(**opts).load().createOrReplaceTempView(view_name)
        case "opensearch":
            opts = {"opensearch.nodes": OPENSEARCH_URL,
                    "opensearch.net.ssl": "false"}
                    #"opensearch.mapping.id": collection.id_field
            dataframe = spark.read.format("opensearch").options(**opts).load(collection.name)
            #print(dataframe.columns)
            #if "id" not in dataframe.columns:
            #    dataframe = dataframe.withColumnRenamed("_id", "id")
            dataframe.createOrReplaceTempView(view_name)
        case _:
            raise NotImplementedError(type(collection))