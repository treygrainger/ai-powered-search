from graphql_query import Argument
from engines.SparseSemanticSearch import SparseSemanticSearch

class WeaviateSearchSparseSemanticSearch(SparseSemanticSearch):
    def __init__(self):
        pass

    def location_distance(self, query, position):
        if len(query["query_tree"]) -1 > position:
            next_entity = query["query_tree"][position + 1]
            if next_entity["type"] == "city":
                query["query_tree"].pop(position + 1)
                query["query_tree"][position] = {
                    "type": "transformed",
                    "syntax": "weaviate",
                    "query": {"filters": self.create_geo_filter(next_entity['location_coordinates'],
                                                               "location_coordinates", 50)}}
                return True
        return False

    def create_geo_filter(self, coordinates, field, distance_KM):
        lat_lon = [Argument(name="latitude", value=coordinates.split(",")[0]),
                   Argument(name="longitude", value=coordinates.split(",")[1])]
        return [Argument(name="operator", value="WithinGeoRange"),
                Argument(name="valueGeoRange",
                         value=[Argument(name="geoCoordinates", value=lat_lon),
                                Argument(name="distance", 
                                         value=[Argument(name="max", value=distance_KM * 1000)])]),
                Argument(name="path", value=[f'"{field}"'])]

    def popularity(self, query, position):
        if len(query["query_tree"]) -1 > position:
            query["query_tree"][position] = {"type": "transformed",
                                             "syntax": "weaviate",
                                             "query": {"vector_search": {"popularity": [5]}}}
            return True
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
                             "syntax": "weaviate",
                             "query": transformed_query}                 
        return query_tree

    def generate_basic_query(self, query):
        return '"' + query.replace('"', '\\"') + '"'