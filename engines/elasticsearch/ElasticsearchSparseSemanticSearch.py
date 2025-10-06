from engines.SparseSemanticSearch import SparseSemanticSearch

def escape_quotes(text):
    return text.replace('"', '\\"')

class ElasticsearchSparseSemanticSearch(SparseSemanticSearch):
    def __init__(self):
        pass

    def location_distance(self, query, position):
        if len(query["query_tree"]) - 1 > position:
            next_entity = query["query_tree"][position + 1]
            if next_entity["type"] == "city":
                query["query_tree"].pop(position + 1)
                query["query_tree"][position] = {
                    "type": "transformed",
                    "syntax": "elasticsearch",
                    "query": self.create_geo_filter(next_entity["location_coordinates"],
                                            "location_coordinates", 50)}
                return True
        return False

    def create_geo_filter(self, coordinates, field, distance_KM):
        return {"geo_distance": {"distance": f"{distance_KM}km",
                                           field: {"lat": coordinates.split(",")[0],
                                                   "lon": coordinates.split(",")[1]}}}

    def popularity(self, query, position):
        if len(query["query_tree"]) - 1 > position:
            query["query_tree"][position] = {
                "type": "transformed",
                "syntax": "elasticsearch",
                "query": {"function_score": {"field_value_factor": {
                    "field": "stars_rating",
                    "factor": 1.5,
                    "missing": 0}}}}
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
                        if "category" in enrichments:
                            query_string += f' +doc_type:"{enrichments["category"]}"'
                        transformed_query = '"' + escape_quotes(item["surface_form"]) + '"'
                    else:
                        continue
                case "color":
                    transformed_query = f'+colors:"{item["canonical_form"]}"'
                case "known_item" | "event":
                    transformed_query = f'+name:"{item["canonical_form"]}"'
                case "city":
                    transformed_query = f'+city:"{item["canonical_form"]}"'
                case "brand":
                    transformed_query = f'+brand:"{item["canonical_form"]}"'
                case _:
                    transformed_query = '"' + escape_quotes(item["surface_form"]) + '"'
            query_tree[i] = {"type": "transformed",
                             "syntax": "elasticsearch",
                             "query": transformed_query}                 
        return query_tree

    def generate_basic_query(self, query):
        return '"' + escape_quotes(query) + '"'
