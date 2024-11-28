from pyspark.sql import SparkSession

from aips.environment import AIPS_ZK_HOST
from engines.solr.SolrCollection import SolrCollection

from engines.opensearch.OpenSearchCollection import OpenSearchCollection
from engines.opensearch.config import OPENSEARCH_URL

from engines.weaviate.WeaviateCollection import WeaviateCollection
from engines.weaviate.config import WEAVIATE_HOST, WEAVIATE_PORT

def create_view_from_collection(collection, view_name, spark=None):
    if not spark:
        spark = SparkSession.builder.appName("AIPS").getOrCreate()
    if isinstance(collection, SolrCollection):
        opts = {"zkhost": AIPS_ZK_HOST, "collection": collection.name}    
        spark.read.format("solr").options(**opts).load().createOrReplaceTempView(view_name)
    elif isinstance(collection, OpenSearchCollection):
        opts = {"opensearch.nodes": OPENSEARCH_URL,
                "opensearch.net.ssl": "false"}
        spark.read.format("opensearch").options(**opts).load(collection.name).createOrReplaceTempView(view_name)
    elif isinstance(collection, WeaviateCollection):
        opts = {"scheme": "http",
                "host": f"{WEAVIATE_HOST}:{WEAVIATE_PORT}",
                #"id": "id",
                "className": collection.name} #"vector": "vector"
        spark.read.format("io.weaviate.spark.Weaviate").options(**opts).load().createOrReplaceTempView(view_name)
    else:
        raise NotImplementedError(type(collection))