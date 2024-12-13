from pyspark.sql import SparkSession

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType 

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
            parse_id_udf = udf(lambda s: s["_id"], StringType())
            opts = {"opensearch.nodes": OPENSEARCH_URL,
                    "opensearch.net.ssl": "false",
                    "opensearch.read.metadata": "true"}
            dataframe = spark.read.format("opensearch").options(**opts).load(collection.name)
            if "_metadata" in dataframe.columns:
                dataframe = dataframe.withColumn("id", parse_id_udf(col("_metadata")))
                dataframe = dataframe.drop("_metadata")
            print(dataframe.columns)
            dataframe.createOrReplaceTempView(view_name)
        case _:
            raise NotImplementedError(type(collection))