import sys
sys.path.append('..')
from aips import *
import requests, json
from semantic_search.engine.structured_search import *

def get_category_and_term_vector_solr_response(keyword):
    query = {
        "params": { "fore": keyword, "back": "*:*", "df": "text_t" },
        "query": "*:*", "limit": 0,
        "facet": {
            "term_needing_vector": {
                "type": "query", "query": keyword,
                "facet": {
                    "related_terms" : {
                        "type" : "terms", "field" : "text_t", "limit": 3, "sort": { "r1": "desc" },
                        "facet" : { "r1" : "relatedness($fore,$back)" }},
                    "doc_type" : {
                        "type" : "terms", "field" : "doc_type", "limit": 1, "sort": { "r2": "desc" },
                        "facet" : { "r2" : "relatedness($fore,$back)"  }}}}}}

    response = structured_search(query)
    return json.loads(response)