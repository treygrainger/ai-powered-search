from engines.opensearch.OpenSearchCollection import OpenSearchCollection
from engines.solr.SolrCollection import SolrCollection
from pyspark.sql.functions import collect_list, create_map
from aips.spark.dataframe import from_sql
from aips.spark import create_view_from_collection

def load_dataframe(boosted_products_collection, boosts_collection):
    create_view_from_collection(boosts_collection,
                                boosts_collection.name)    
    create_view_from_collection(boosted_products_collection,
                                boosted_products_collection.name)    
    assert(type(boosted_products_collection) == type(boosts_collection))
    if isinstance(boosted_products_collection, SolrCollection):
        query = f"""SELECT p.*, b.signals_boosts FROM (
                    SELECT doc, CONCAT_WS(',', COLLECT_LIST(CONCAT(query, '|', boost)))
                    AS signals_boosts FROM {boosts_collection.name} GROUP BY doc
                    ) b INNER JOIN {boosted_products_collection.name} p ON p.upc = b.doc"""
        boosts_dataframe = from_sql(query)
    elif isinstance(boosted_products_collection, OpenSearchCollection):
        product_query = f"SELECT * FROM {boosted_products_collection.name}"
        boosts_query = f"SELECT doc, boost, REPLACE(query, '.', '') AS query FROM {boosts_collection.name}"

        grouped_boosts = from_sql(boosts_query).groupBy("doc") \
            .agg(collect_list(create_map("query", "boost"))[0].alias("signals_boost")) \
            .withColumnRenamed("doc", "upc")

        boosts_dataframe = from_sql(product_query).join(grouped_boosts, "upc")
    else:
        raise Exception(f"Index time boost not implemented for {type(boosted_products_collection)}")

    return boosts_dataframe