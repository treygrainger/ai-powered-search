from engines.SparseSemanticSearch import SparseSemanticSearch

def escape_quotes(text):
    return text.replace('"', '\\"')

class SolrSparseSemanticSearch(SparseSemanticSearch):
    def __init__(self):
        pass

    def location_distance(self, query, position):
        if len(query["query_tree"]) -1 > position:
            next_entity = query["query_tree"][position + 1]
            if next_entity["type"] == "city":
                query["query_tree"].pop(position + 1)
                query["query_tree"][position] = {
                    "type": "transformed",
                    "syntax": "solr",
                    "query": self.create_geo_filter(next_entity['location_coordinates'],
                                            "location_coordinates", 50)}
                return True
        return False

    def create_geo_filter(self, coordinates, field, distance_KM):
        clause = f'!geofilt d={distance_KM} sfield="{field}" pt="{coordinates}"'
        return "+{" + clause + '}'

    def popularity(self, query, position):
        if len(query["query_tree"]) -1 > position:
            query["query_tree"][position] = {
                "type": "transformed",
                "syntax": "solr",
                "query": '+{!func v="mul(if(stars_rating,stars_rating,0),20)"}'}
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
                        transformed_query = '+{!edismax v="' + escape_quotes(query_string) + '"}'
                    else:
                        continue
                case "color":
                    transformed_query = f'+colors_s:"{item["canonical_form"]}"'
                case "known_item" | "event":
                    transformed_query = f'+name_s:"{item["canonical_form"]}"'
                case "city":
                    transformed_query = f'+city:"{str(item["canonical_form"])}"'
                case "brand":
                    transformed_query = f'+brand_s:"{item["canonical_form"]}"'
                case _:
                    transformed_query = "+{!edismax v=\"" + escape_quotes(item["surface_form"]) + "\"}"
            query_tree[i] = {"type": "transformed",
                            "syntax": "solr",
                            "query": transformed_query}                 
        return query_tree

    def generate_basic_query(self, query):
        return '+{!edismax mm=100% v="' + escape_quotes(query) + '"}'