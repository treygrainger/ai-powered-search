from aips import get_semantic_knowledge_graph, get_sparse_semantic_search

semantic_functions = get_sparse_semantic_search()

def create_geo_filter(coordinates, field, distance_in_KM):
    return semantic_functions.create_geo_filter(coordinates, field, distance_in_KM)

def popularity(query, position):
    return semantic_functions.popularity(query, position)

def location_distance(query, position):
    return semantic_functions.location_distance(query, position)

def to_queries(query_tree):
    return [node["query"] for node in query_tree]
 
def process_semantic_functions(query_tree):
    position = 0
    while position < len(query_tree):
        node = query_tree[position]
        if node["type"] == "semantic_function":
            query = {"query_tree": query_tree} 
            command_successful = eval(node["semantic_function"])
            if not command_successful:
                node["type"] = "invalid_semantic_function"
        position += 1
    return query_tree 

def get_enrichments(collection, keyword, limit=4):
    enrichments = {}
    nodes_to_traverse = [{"field": "content",
                          "values": [keyword],
                          "default_operator": "OR"},
                         [{"name": "related_terms",
                           "field": "content",
                           "limit": limit},
                          {"name": "doc_type",
                           "field": "doc_type",
                           "limit": 1}]]
    skg = get_semantic_knowledge_graph(collection)
    traversals = skg.traverse(*nodes_to_traverse)
    if "traversals" not in traversals["graph"][0]["values"][keyword]:
        return enrichments
    
    nested_traversals = traversals["graph"][0]["values"][keyword]["traversals"]
    
    doc_types = list(filter(lambda t: t["name"] == "doc_type",
                            nested_traversals))
    if doc_types:
        enrichments["category"] = next(iter(doc_types[0]["values"]))
        
    related_terms = list(filter(lambda t: t["name"] == "related_terms",
                                nested_traversals))
    if related_terms:
        term_vector = ""
        for term, data in related_terms[0]["values"].items():
            term_vector += f'{term}^{round(data["relatedness"], 4)} '
        enrichments["term_vector"] = term_vector.strip()
    
    return enrichments

def enrich(collection, query_tree):
    query_tree = process_semantic_functions(query_tree)    
    for item in query_tree:
        if item["type"] == "keyword":
            enrichments = get_enrichments(collection, item["surface_form"])
            if enrichments:
                item["type"] = "skg_enriched"
                item["enrichments"] = enrichments
    return query_tree