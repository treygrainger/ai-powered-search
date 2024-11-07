from engines.opensearch.OpenSearchCollection import OpenSearchCollection
from engines.solr.SolrCollection import SolrCollection
from pyspark.sql.functions import collect_list, create_map
from aips.spark.dataframe import from_sql
from aips.spark import create_view_from_collection

#Note to Daniel: abstraction here isn't great. This code wasn't running, so I made to below changes to get it to run (at least in  Solr).
# it was passing in the two collections and then hard coding the view names ("products" and "normalized_signals_boosts")
# Since "products" is wrong, I've changed it to use product_collection.name and boost_collection.name to get the right
# non-hard-coded collections, BUT even this is a leaky abstraction because it depends on a view being created first, like
# is being done in listing 8.8 to make those collection names actually queryable with SQL.
# SO... ideally the code should create the views inside this function OR we should pass the view names into the function to
# have a have the separation of responsibilities correct here.
# All that said... I'm just going to leave it as is and assume the name of the collection is the name of the view 
# because then I can make the smallest number of changes to the manuscript to ship this.
# The method isn't really abstracted in other places, either (hard-coded field names assumed, etc.) and is only used for
#listing 8.8 anyway.
# Let me know if you'd like to handle differently. 
# -TG

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
        boosts_query = f"SELECT * FROM {boosts_collection.name}"

        grouped_boosts = from_sql(boosts_query).groupBy("doc") \
            .agg(collect_list(create_map("query", "boost"))[0].alias("signals_boost")) \
            .withColumnRenamed("doc", "upc")

        boosts_dataframe = from_sql(product_query).join(grouped_boosts, "upc")
    else:
        raise Exception(f"Index time boost not implemented for {type(boosted_products_collection)}")

    return boosts_dataframe