import sys
sys.path.append('..')
from aips import *
import json
from webserver.semantic_search.query_tree import *
from webserver.semantic_search.engine.text_tagger import TextTagger

def generate_tagged_query(query, tagger_data):
    last_end = 0
    tagged_query = ""
    for tag in tagger_data["tags"]:
        next_text = query[last_end:tag["startOffset"]].strip()
        if len(next_text) > 0:
            tagged_query += " " + next_text
        tagged_query += " {" + tag["matchText"] + "}"
        last_end = tag["endOffset"]
    if last_end < len(query):
        final_text = query[last_end:len(query)].strip()
        if len(final_text):
            tagged_query += " " + final_text
    return tagged_query            
                
def generate_query_tree(query, tagger_data):
    query_tree = []    
    last_end = 0
    doc_map =  {}
    for doc in tagger_data["response"]["docs"]:
        doc_map[doc["id"]] = doc
        
    for tag in tagger_data["tags"]:
        best_doc_id = None
        for doc_id in tag["ids"]:
            if best_doc_id:
                if (doc_map[doc_id]["popularity"] > 
                    doc_map[best_doc_id]["popularity"]):
                    best_doc_id = doc_id
            else:
                best_doc_id = doc_id
        best_doc = doc_map[best_doc_id]
        
        next_text = query[last_end:tag["startOffset"]].strip()
        if len(next_text) > 0:
            query_tree.append({
                "type": "keyword", "known": False,
                "surface_form": next_text,
                "canonical_form": next_text})
        query_tree.append(best_doc)
        last_end = tag["endOffset"]

    if last_end < len(query):
        final_text = query[last_end:len(query)].strip()
        if len(final_text) > 0:
            query_tree.append({ 
                "type": "keyword", "known": False, 
                "surface_form": final_text,
                "canonical_form": final_text})
    return query_tree

def process_semantic_query(collection, query):
    query_bytes = bytes(query, "UTF-8")
    tagger_data = TextTagger("entities").tag_query(query_bytes)
    
    tagged_query = generate_tagged_query(query, tagger_data)
    query_tree = generate_query_tree(query, tagger_data)
    parsed_query = json.dumps(query_tree)
    enriched_query_tree = enrich(collection, query_tree)
    transformed = transform_query(enriched_query_tree)
    
    return {
        "tagged_query": tagged_query,
        "parsed_query": parsed_query, 
        "transformed_query": to_query_string(transformed),
        "tagger_data": tagger_data
    }
    
def process_basic_query(query):
    return {"transformed_query": '+{!edismax mm=100% v="' + escape_quotes_in_query(query) + '"}'}