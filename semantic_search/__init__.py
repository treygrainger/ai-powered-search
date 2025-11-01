import sys
sys.path.append('../..')
from aips import get_entity_extractor, get_sparse_semantic_search
from .query_tree import enrich, to_queries

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
    entities = {entity["id"]: entity for entity
                in extracted_entities["entities"]}
    query_tree = []    
    last_end = 0
    
    for tag in extracted_entities["tags"]:
        best_entity = entities[tag["ids"][0]]
        for entity_id in tag["ids"]:
            if (entities[entity_id]["popularity"] > 
                best_entity["popularity"]):
                best_entity = entities[entity_id]
        
        next_text = query[last_end:tag["startOffset"]].strip()
        if next_text:
            query_tree.append({"type": "keyword",
                               "surface_form": next_text,
                               "canonical_form": next_text})
        query_tree.append(best_entity)
        last_end = tag["endOffset"]

    if last_end < len(query):
        final_text = query[last_end:len(query)].strip()
        if final_text:
            query_tree.append({"type": "keyword",
                               "surface_form": final_text,
                               "canonical_form": final_text})
    return query_tree

def process_semantic_query(collection, entities_collection, query):
    extractor = get_entity_extractor(entities_collection)
    semantic_functions = get_sparse_semantic_search()
    entities = extractor.extract_entities(query)
    tagged_query = generate_tagged_query(entities)
    query_tree = generate_query_tree(entities)
    enriched_query = " ".join([str(q) for q in query_tree])
    enriched_query_tree = enrich(collection, query_tree)
    transformed = semantic_functions.transform_query(enriched_query_tree)
    transformed_query = " ".join(to_queries(transformed))
    
    return {
        "tagged_query": tagged_query,
        "parsed_query": enriched_query,
        "transformed_query": transformed_query,
        "tagger_data": entities
    }

def process_basic_query(query):
    semantic_functions = get_sparse_semantic_search()
    return {"transformed_query": semantic_functions.generate_basic_query(query)}