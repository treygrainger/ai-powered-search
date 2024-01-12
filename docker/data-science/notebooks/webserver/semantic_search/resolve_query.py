import sys
sys.path.append('..')
from aips import *
import requests
from semantic_search.query_tree.process_semantic_functions import *
from semantic_search.engine.get_category_and_term_vector_solr_response import *
from semantic_search.engine.parse_category_and_term_vector_from_solr_response import *
from semantic_search.query_tree.escape_quotes_in_query import *

def resolve_query(query_tree):
    query_tree = process_semantic_functions(query_tree)
        
    # Now process everything that is not yet resolved
    for position in range(len(query_tree)):
        item = query_tree[position];         
        if (item["type"] != "solr"): #already resolved
            if (item["type"] == "keyword"):  
                #TODO: this currently looks up ALL unknown keywords in the SKG, which isn't very smart
                #need to switch to looking up meaningful phrases in next pass. This is mostly for testing
                #at the moment, so putting up with the noise temporarily.
                categoryAndTermVector = None
                #TODO: figure out way (probably timestamp-based) to guarantee processing in order given current async nature
                solrResponse = get_category_and_term_vector_solr_response(item["surface_form"])
                categoryAndTermVector = parse_category_and_term_vector_from_solr_response(solrResponse)       
                
                #if (latestAsyncRequestID != categoryAndTermVector.asyncRequestID){
                #  return null;
                #}

                queryString = ""
                if ("term_vector" in categoryAndTermVector):
                    queryString = categoryAndTermVector["term_vector"]
                
                if ("category" in categoryAndTermVector):
                    if (len(queryString) > 0):
                        queryString += " "
                        queryString += "+doc_type:\"" + categoryAndTermVector["category"] + "\""
                    
                    
                if (len(queryString) == 0):
                    queryString = item["surface_form"] #just keep the input as a keyword

                query_tree[position] = { "type":"solr", "query": "+{!edismax v=\"" + escape_quotes_in_query(queryString) + "\"}" }              
            elif (item["type"] == "color"):
                solrQuery = "+colors_s:\"" + item["canonical_form"] + "\""
                query_tree[position] = {"type":"solr", "query": solrQuery}
            elif (item["type"] == "known_item" or item["type"] == "city" or item["type"] == "event"):
                solrQuery = "+name_s:\"" + item["canonical_form"] + "\""
                query_tree[position] = {"type":"solr", "query": solrQuery}
            elif item["type"] == "city":
                solrQuery = "+city_t:\"" + str(item["name"]) + "\"" 
                query_tree[position] = {"type":"solr", "query": solrQuery}
            elif (item["type"] == "brand"):
                solrQuery = "+brand_s:\"" + item["canonical_form"] + "\""
                query_tree[position] = {"type":"solr", "query": solrQuery}
            else:
                print(item)
                #note ensure all item types either 1) have a surface form, or 2) are handled specially above
                query_tree[position] = {"type":"solr", "query": "+{!edismax v=\"" + escape_quotes_in_query(item["surface_form"]) + "\"}"}
                
                
    return query_tree