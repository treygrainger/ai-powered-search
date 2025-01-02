from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType 
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from engines.opensearch.config import OPENSEARCH_URL

def get_spark_session():
    conf = SparkConf()
    conf.set("spark.driver.memory", "8g")
    conf.set("spark.executor.memory", "8g")
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.ui.port", "4040")
    conf.set("spark.dynamicAllocation.executorMemoryOverhead", "8g")
    spark = SparkSession.builder.appName("AIPS").config(conf=conf).getOrCreate()
    return spark

def create_view_from_collection(collection, view_name, spark=None):
    if not spark:
        spark = get_spark_session()
    match collection.get_engine_name():
        case "solr":
            opts = {"zkhost": collection.zk_host, "collection": collection.name}    
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
            dataframe.createOrReplaceTempView(view_name)
        case "weaviate":
            #Weaviate's current spark connector read functionality not yet implemented
            #Resort to batch paged reading
            fields = collection.get_collection_field_names()
            fields.append("id")
            request = {"return_fields": fields,
                       "limit": 1000}
            all_documents = []
            while True:
                docs = collection.search(**request)["docs"]
                all_documents.extend(docs)
                if len(docs) != request["limit"]:
                    break
                last_doc = docs[request["limit"] - 1]
                if "wv8_id" not in last_doc and "id" not in last_doc:
                    docs = collection.search(**{"return_fields": fields,
                        "limit": 1000, "log": True})["docs"]
                cursor_id = last_doc.get("wv8_id", last_doc["id"]) # id hack
                request["after"] = cursor_id
                
            dataframe = spark.createDataFrame(data=all_documents)
            dataframe.createOrReplaceTempView(view_name)
        case _:
            raise NotImplementedError(type(collection))
