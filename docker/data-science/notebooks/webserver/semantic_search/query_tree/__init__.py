from semantic_search.engine.semantic_knowledge_graph import *

def escape_quotes_in_query(query):
    return query.replace('"', '\\"')

def to_query_string(query_tree):
    return " ".join([node["query"] for node in query_tree])

def enrich(query_tree):
    query_tree = process_semantic_functions(query_tree)    
    for i in range(len(query_tree)):
        item = query_tree[i]
        if item["type"] == "keyword":
            skg_response = traverse_skg(item["surface_form"])
            enrichments = parse_skg_response(skg_response)
            query_tree[i] = {"type": "skg_enriched", 
                             "enrichments": enrichments}                    
    return query_tree

def transform_query(query_tree):
    for i in range(len(query_tree)):
        item = query_tree[i]
        additional_query = ""
        match item["type"]:
            case "solr":
                pass
            case "skg_enriched":
                enrichments = item["enrichments"]
                query_string = ""
                
                if "term_vector" in enrichments:
                    query_string = enrichments["term_vector"]
                if "category" in enrichments and len(query_string) > 0:
                    query_string += f' +doc_type:"{enrichments["category"]}"'
                if (len(query_string) == 0):
                    query_string = item["surface_form"]
                    
                additional_query = '{!edismax v="' + escape_quotes_in_query(query_string) + '"}'
            case "color":
                additional_query = f'+colors_s:"{item["canonical_form"]}"'
            case "known_item" | "event":
                additional_query = f'+name_s:"{item["canonical_form"]}"'
            case "city":
                additional_query = f'+city_t:"{str(item["name"])}"'
            case "brand":
                additional_query = f'+brand_s:"{item["canonical_form"]}"'
            case _:
                additional_query = "+{!edismax v=\"" + escape_quotes_in_query(item["surface_form"]) + "\"}"
        if additional_query:
            query_tree[i] = {"type": "solr", "query": additional_query}                    
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
                commandIsResolved = eval(item['semantic_function']); #Careful... there is code in the docs that is being eval'd. 
                #MUST ENSURE THESE DOCS ARE SECURE, OTHERWISE THIS WILL INTRODUCE A POTENTIAL SECURITY THREAT (CODE INJECTION)
            
            #else:
                #Alert ("Error: " + query.query_tree.canonical_form + " has no command function.");
            
            if (False == commandIsResolved):
                #Bad command. Just remove for now... could alternatively keep it and run as a keyword
                query_tree.pop(position) #.splice(position,1)  

        position += 1

    return query_tree 