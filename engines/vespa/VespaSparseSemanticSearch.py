from engines.SparseSemanticSearch import SparseSemanticSearch

class VespaSparseSemanticSearch(SparseSemanticSearch):
    def __init__(self):
        pass

    def location_distance(self, query, position):
        #geoLocation(mainloc, 37.416383, -122.024683, "20 miles")
        if len(query["query_tree"]) - 1 > position:
            next_entity = query["query_tree"][position + 1]
            if next_entity["type"] == "city":
                query["query_tree"].pop(position + 1)
                coordinates = next_entity['location_coordinates']
                lat, lon = coordinates.split(",")
                query["query_tree"][position] = {
                    "type": "transformed",
                    "syntax": "vespa",
                    "query": f'geoLocation(location_coordinates, {lat}, {lon}, "50 km")'}
                return True
        return False

    def popularity(self, query, position):
        if len(query["query_tree"]) - 1 > position:
            query["query_tree"][position] = {
                "type": "transformed",
                "syntax": "vespa",
                "query": "if(isNan(attribute(stars_rating)) == 1, 0, attribute(stars_rating)) * 3"}
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
                             "syntax": "vespa",
                             "query": transformed_query}                 
        return query_tree

    def generate_basic_query(self, query):
        return query