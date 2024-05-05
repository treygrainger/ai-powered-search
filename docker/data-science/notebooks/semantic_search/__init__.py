import sys
sys.path.append('..')
import json
from aips import get_knowledge_graph, get_semantic_functions
from .query_tree import enrich, to_query_string

def generate_tagged_query(extracted_entities):
    query = extracted_entities["query"]
    last_end = 0
    tagged_query = ""
    for tag in extracted_entities["tags"]:
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
                
def generate_query_tree(extracted_entities):
    query = extracted_entities["query"]
    query_tree = []    
    last_end = 0
    doc_map =  {}
    for doc in extracted_entities["entities"]:
        doc_map[doc["id"]] = doc
        
    for tag in extracted_entities["tags"]:
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
    knowledge_graph = get_knowledge_graph("entities")
    semantic_functions = get_semantic_functions()
    entities = knowledge_graph.extract_entities(query)
    tagged_query = generate_tagged_query(entities)
    query_tree = generate_query_tree(entities)
    enriched_query = " ".join([str(q) for q in query_tree])
    enriched_query_tree = enrich(collection, query_tree)
    transformed = semantic_functions.transform_query(enriched_query_tree)
    
    return {
        "tagged_query": tagged_query,
        "parsed_query": enriched_query,
        "transformed_query": to_query_string(transformed),
        "tagger_data": entities
    }

def process_basic_query(query):
    semantic_functions = get_semantic_functions()
    return {"transformed_query": semantic_functions.generate_basic_query(query)}