from pyspark.sql import SparkSession

from aips.environment import AIPS_ZK_HOST
from engines.solr.SolrCollection import SolrCollection

from engines.opensearch.OpenSearchCollection import OpenSearchCollection
from engines.opensearch.opensearch_environment import OPENSEARCH_URL


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
    else:
        raise NotImplementedError(type(collection))