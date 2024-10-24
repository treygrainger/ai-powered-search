from engines.opensearch.OpenSearchCollection import OpenSearchCollection
from engines.solr.SolrCollection import SolrCollection
from pyspark.sql.functions import collect_list, create_map
from aips.spark.dataframe import from_sql

def load_dataframe(product_collection, boost_collection):
    assert(type(product_collection) == type(boost_collection))
    if isinstance(product_collection, SolrCollection):
        query = """SELECT p.*, b.signals_boosts FROM (
                    SELECT doc, CONCAT_WS(',', COLLECT_LIST(CONCAT(query, '|', boost)))
                    AS signals_boosts FROM normalized_signals_boosts GROUP BY doc
                    ) b INNER JOIN products p ON p.upc = b.doc"""
        boosts_dataframe = from_sql(query)
    elif isinstance(product_collection, OpenSearchCollection):
        product_query = "SELECT * FROM products"
        boosts_query = "SELECT * FROM normalized_signals_boosts"

        grouped_boosts = from_sql(boosts_query).groupBy("doc") \
            .agg(collect_list(create_map("query", "boost"))[0].alias("signals_boost")) \
            .withColumnRenamed("doc", "upc")

        boosts_dataframe = from_sql(product_query).join(grouped_boosts, "upc")
    else:
        raise Exception(f"Index time boost not implemented for {type(product_collection)}")

    return boosts_dataframe