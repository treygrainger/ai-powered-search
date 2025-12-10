from engines.SparseSemanticSearch import SparseSemanticSearch

class WeaviateVespaSparseSemanticSearch(SparseSemanticSearch):
    def __init__(self):
        pass

    def location_distance(self, query, position):
        return False

    def create_geo_filter(self, coordinates, field, distance_KM):
        return False

    def popularity(self, query, position):
        return False
        
    def transform_query(self, query_tree):
        for i, item in enumerate(query_tree):
            match item["type"]:
                case "transformed":
                    continue
                case "skg_enriched":
                    enrichments = item["enrichments"]  
                    if "term_vector" in enrichments:
                        query_string = enrichments["term_vector"]
                        transformed_query = query_string
                case _:
                    transformed_query = item["surface_form"].replace('"', '\\"')
            query_tree[i] = {"type": "transformed",
                             "syntax": "vespa",
                             "query": transformed_query}                 
        return query_tree

    def generate_basic_query(self, query):
        return query