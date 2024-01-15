import sys
sys.path.append('..')
from aips import *
import requests, json
from semantic_search.engine.tag_query import *
from semantic_search.resolve_query import *
from semantic_search.query_tree.query_tree_to_resolved_query import *

def process_semantic_query(query_bytes):
    text = query_bytes.decode('UTF-8')
    data = tag_query(query_bytes)
    tagged_response = json.loads(data)

    #loop through all documents (entities) returned
    doc_map={} # reset to empty
    if (tagged_response['response'] and tagged_response['response']['docs']):

        docs = tagged_response['response']['docs']

        for doc in docs:
            doc_map[doc['id']] = doc

        #for (d=0; d<Object.keys(docs).length; d++) {
        #  let doc = docs[d];
        #  doc_map[doc.id]=doc;
        #}
        
        #sort doc_map by popularity so first most popular always wins
        #def popularity_sort(doc_a, doc_b){
        #  return a.popularity - b.popularity;
        #}
        
        #//doc_map.sort(popularity_sort);
      #}

    query_tree = []
    tagged_query = ""
    transformed_query =""
      
    if (tagged_response['tags'] is not None):
        tags = tagged_response['tags'] 
        #//var lastStart = 0;
        lastEnd = 0
        metaData = {}
        for tag in tags:                
            #tag = tags[key]
            matchText = tag['matchText']
            

            doc_ids = tag['ids']          
            
            #pick top-ranked docid
            best_doc_id = None

            for doc_id in doc_ids:
                if (best_doc_id):
                    if (doc_map[doc_id]['popularity'] > doc_map[best_doc_id]['popularity']):
                        best_doc_id = doc_id
                else:
                    best_doc_id = doc_id


            best_doc = doc_map[best_doc_id]

            #store the unknown text as keywords
            nextText = text[lastEnd:tag['startOffset']].strip()
            if (len(nextText) > 0):  #not whitespace
                query_tree.append({ "type":"keyword", "known":False, "surface_form":nextText, "canonical_form":nextText })          
                tagged_query += " " + nextText
                transformed_query += " " + "{ type:keyword, known: false, surface_form: \"" + nextText + "\"}" 
            
            
            # store the known entity as entity
            query_tree.append(best_doc)  #this is wrong. Need the query tree to have _all_
            # interpretations available and then loop through them to resolve. TODO = fix this.

            tagged_query += " {" + matchText + "}"          
            #//transformed_query += " {type: " + best_doc.type + ", canonical_form: \"" + best_doc.canonical_form + "\"}";  
            transformed_query += json.dumps(best_doc)             
            lastEnd = tag['endOffset'] 
        

        
        if (lastEnd < len(text)):
            finalText = text[lastEnd:len(text)].strip()
            if (len(finalText) > 0):
                query_tree.append({ "type":"keyword", "known":False, "surface_form":finalText, "canonical_form":finalText })
                
                tagged_query += " " + finalText
                transformed_query += " " + "{ type:keyword, known: false, surface_form: \"" + finalText + "\"}" 
                  


    #finalquery = {"query_tree": query_tree}
    #let query = {query_tree: query_tree}; //so we can pass byref        
        
    final_query = resolve_query(query_tree)
    #if (query != null){ //short circuit if new request has been issued
    resolved_query = query_tree_to_resolved_query(query_tree)      
                    
            #UI.updateResolvedQuery(resolved_query)
        #}

    response = {
        "tagged_query": tagged_query,
        "transformed_query": transformed_query,
        "resolved_query": resolved_query,
        "tagger_data": tagged_response
    }

    return response