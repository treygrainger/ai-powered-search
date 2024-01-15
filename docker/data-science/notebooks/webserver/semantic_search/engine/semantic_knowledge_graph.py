import sys
sys.path.append('..')
from aips import *
import requests, json
from semantic_search.engine.structured_search import *

def traverse_skg(keyword):
    query = {
      "params": { "fore": keyword, "back": "*:*", "df": "text_t" },
      "query": "*:*", "limit": 0,
      "facet": {
        "term_needing_vector": {
          "type": "query", "query": keyword,
          "facet": {
            "related_terms" : {
              "type" : "terms", "field" : "text_t",
              "limit": 3, "sort": { "r1": "desc" },
              "facet" :
                { "r1" : "relatedness($fore,$back)" }},
            "doc_type" : {
              "type" : "terms", "field" : "doc_type",
              "limit": 1, "sort": { "r2": "desc" },
              "facet" :
                { "r2" : "relatedness($fore,$back)" }}}}}}

    response = structured_search(query)
    return json.loads(response)


def parse_skg_response(skg_response):
    parsed = {}
    relatedTermNodes = {}

    if ('facets' in skg_response and 'term_needing_vector' in skg_response['facets']):
    
        if ('doc_type' in skg_response['facets']['term_needing_vector'] 
          and 'buckets' in skg_response['facets']['term_needing_vector']['doc_type'] 
          and len(skg_response['facets']['term_needing_vector']['doc_type']['buckets']) > 0 ):

            parsed['category'] = skg_response['facets']['term_needing_vector']['doc_type']['buckets'][0]['val'] #just top one for now
    
        if ('related_terms' in skg_response['facets']['term_needing_vector'] 
          and 'buckets' in skg_response['facets']['term_needing_vector']['related_terms'] 
          and len(skg_response['facets']['term_needing_vector']['related_terms']['buckets']) > 0 ): #at least one entry
    
            relatedTermNodes = skg_response['facets']['term_needing_vector']['related_terms']['buckets']
    
    termVector = ""
    for relatedTermNode in relatedTermNodes:
        if (len(termVector) > 0):  termVector += " " 
        termVector += relatedTermNode['val'] + "^" + "{:.4f}".format(relatedTermNode['r1']['relatedness'])
    
    parsed['term_vector'] = termVector

    return parsed

def determine_category_and_related_terms(keyword):
    return parse_skg_response(traverse_skg(keyword))