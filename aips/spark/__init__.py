from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from aips.environment import AIPS_ZK_HOST
from engines.solr.SolrCollection import SolrCollection

from engines.opensearch.OpenSearchCollection import OpenSearchCollection
from engines.opensearch.config import OPENSEARCH_URL

from engines.weaviate.WeaviateCollection import WeaviateCollection

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
    if isinstance(collection, SolrCollection):
        opts = {"zkhost": AIPS_ZK_HOST, "collection": collection.name}    
        spark.read.format("solr").options(**opts).load().createOrReplaceTempView(view_name)
    elif isinstance(collection, OpenSearchCollection):
        opts = {"opensearch.nodes": OPENSEARCH_URL,
                "opensearch.net.ssl": "false"}
        spark.read.format("opensearch").options(**opts).load(collection.name).createOrReplaceTempView(view_name)
    elif isinstance(collection, WeaviateCollection):
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
            request["after"] = docs[request["limit"] - 1]["id"]
        print(len(all_documents))
        dataframe = spark.createDataFrame(data=all_documents)
        dataframe.createOrReplaceTempView(view_name)
    else:
        raise NotImplementedError(type(collection))