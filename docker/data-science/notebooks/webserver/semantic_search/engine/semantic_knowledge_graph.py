import sys
sys.path.append("..")
from aips import *

def traverse_skg(collection, keyword):
    query = {
        "params": {"fore": keyword, "back": "*:*", "df": "text_t"},
        "query": "*:*", "limit": 0,
        "facet": {
            "term_needing_vector": {
                "type": "query", "query": keyword,
                "facet": {
                    "related_terms": {
                        "type": "terms", "field": "text_t",
                        "limit": 3, "sort": {"r1": "desc"},
                        "facet": {"r1": "relatedness($fore,$back)"}},
                "doc_type": {
                    "type": "terms", "field": "doc_type",
                    "limit": 1, "sort": {"r2": "desc"},
                    "facet": {"r2": "relatedness($fore,$back)"}}}}}}    
    return collection.search(query)

def parse_skg_response(skg_response):
    parsed = {}
    related_term_nodes = {}
    if "facets" in skg_response and "term_needing_vector" in skg_response["facets"]:    
        if ("doc_type" in skg_response["facets"]["term_needing_vector"] and 
            "buckets" in skg_response["facets"]["term_needing_vector"]["doc_type"] and
            len(skg_response["facets"]["term_needing_vector"]["doc_type"]["buckets"]) > 0):
            parsed["category"] = skg_response["facets"]["term_needing_vector"]["doc_type"]["buckets"][0]["val"] #just top one for now
    
        if ("related_terms" in skg_response["facets"]["term_needing_vector"] 
            and "buckets" in skg_response["facets"]["term_needing_vector"]["related_terms"] 
          and len(skg_response["facets"]["term_needing_vector"]["related_terms"]["buckets"]) > 0): #at least one entry    
            related_term_nodes = skg_response["facets"]["term_needing_vector"]["related_terms"]["buckets"]
                
    term_vector = ""
    for related_term_node in related_term_nodes:
        if len(term_vector) > 0: term_vector += " " 
        term_vector += related_term_node["val"] + "^" + "{:.4f}".format(related_term_node["r1"]["relatedness"])
    
    parsed["term_vector"] = term_vector

    return parsed