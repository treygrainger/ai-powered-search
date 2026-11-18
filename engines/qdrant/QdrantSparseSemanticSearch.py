from engines.SparseSemanticSearch import SparseSemanticSearch


class QdrantSparseSemanticSearch(SparseSemanticSearch):
    def __init__(self):
        pass

    def location_distance(self, query, position):
        if len(query["query_tree"]) - 1 > position:
            next_entity = query["query_tree"][position + 1]
            if next_entity["type"] == "city":
                query["query_tree"].pop(position + 1)
                lat, lon = next_entity["location_coordinates"].split(",")
                query["query_tree"][position] = {
                    "type": "transformed",
                    "syntax": "qdrant",
                    "query": {
                        "filter": {
                            "geo_radius": {
                                "field": "location_coordinates",
                                "lat": float(lat),
                                "lon": float(lon),
                                "radius_km": 50,
                            }
                        }
                    },
                }
                return True
        return False

    def popularity(self, query, position):
        if len(query["query_tree"]) - 1 > position:
            query["query_tree"][position] = {
                "type": "transformed",
                "syntax": "qdrant",
                "query": {"rerank": "popularity"},
            }
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
                        transformed_query = enrichments["term_vector"]
                    else:
                        transformed_query = item.get("surface_form", "")
                case _:
                    transformed_query = item["surface_form"].replace('"', '\\"')
            query_tree[i] = {
                "type": "transformed",
                "syntax": "qdrant",
                "query": transformed_query,
            }
        return query_tree

    def generate_basic_query(self, query):
        return query
