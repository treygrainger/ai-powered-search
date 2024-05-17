from aips import get_semantic_knowledge_graph, get_sparse_semantic_search

semantic_functions = get_sparse_semantic_search()

def create_geo_filter(coordinates, field, distance_in_KM):
    return semantic_functions.create_geo_filter(coordinates, field, distance_in_KM)

def popularity(query, position):
    return semantic_functions.popularity(query, position)

def location_distance(query, position):
    return semantic_functions.location_distance(query, position)

def to_query_string(query_tree):
    return " ".join([node["query"] for node in query_tree])

def process_semantic_functions_new(query_tree):
    position = 0
    while position < len(query_tree):
        node = query_tree[position]
        if node["type"] == "semantic_function":
            commaned_is_resolved = False
            if node["semantic_function"]:
                query = {"query_tree": query_tree}
                commaned_is_resolved = eval(node["semantic_function"])
            if not commaned_is_resolved:
                query_tree.pop(position)
        position += 1
    return query_tree 
 
def process_semantic_functions(query_tree):
    position = 0
    while position < len(query_tree):
        item = query_tree[position]        
        # process commands. For now, going left to right and then sorting by priority when ambiguous commands occur; 
        # consider other weighting options later.
        if (item['type'] == "semantic_function"):
            commandIsResolved = False
    
            command = item['semantic_function']

            if (command):
                query = {"query_tree": query_tree} #pass by-ref
                commandIsResolved = eval(item['semantic_function']) #Careful... there is code in the docs that is being eval'd. 
                #MUST ENSURE THESE DOCS ARE SECURE, OTHERWISE THIS WILL INTRODUCE A POTENTIAL SECURITY THREAT (CODE INJECTION)
            
            #else:
                #Alert ("Error: " + query.query_tree.canonical_form + " has no command function.");
            
            if (False == commandIsResolved):
                #Bad command. Just remove for now... could alternatively keep it and run as a keyword
                query_tree.pop(position) #.splice(position,1)  

        position += 1

    return query_tree 

def get_enrichments(collection, keyword):
    enrichments = {}
    nodes_to_traverse = [{"field": "content",
                          "values": [keyword],
                          "default_operator": "OR"},
                         [{"name": "related_terms",
                           "field": "content",
                           "limit": 3},
                          {"name": "doc_type",
                           "field": "doc_type",
                           "limit": 1}]]
    skg = get_semantic_knowledge_graph(collection)
    traversals = skg.traverse(*nodes_to_traverse)
    nested_traversals = traversals["graph"][0]["values"][keyword]["traversals"]
    
    doc_types = list(filter(lambda t: t["name"] == "doc_type",
                            nested_traversals))
    if doc_types:
        enrichments["category"] = next(iter(doc_types[0]["values"]))
        
    term_vector = ""
    related_terms = list(filter(lambda t: t["name"] == "related_terms",
                                nested_traversals))
    if related_terms:
        for term, data in related_terms[0]["values"].items():
            term_vector += f'{term}^{round(data["relatedness"], 4)} '
    enrichments["term_vector"] = term_vector.strip()
    
    return enrichments

def enrich(collection, query_tree):
    query_tree = process_semantic_functions(query_tree)    
    for i in range(len(query_tree)):
        item = query_tree[i]
        if item["type"] == "keyword":
            enrichments = get_enrichments(collection, item["surface_form"])
            query_tree[i] = {"type": "skg_enriched", 
                             "enrichments": enrichments}                    
    return query_tree