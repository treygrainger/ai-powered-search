import sys
sys.path.append('..')
from aips import *
import json
from webserver.semantic_search.engine import tag_query
from webserver.semantic_search.query_tree import *

def process_semantic_query(query_bytes):
    text = query_bytes.decode('UTF-8')
    tagger_data = json.loads(tag_query(query_bytes))
    
    query_tree, tagged_query, enriched_query = generate_query_representations(text, tagger_data)
        
    enriched_query_tree = enrich(query_tree)
    transformed = transform_query(enriched_query_tree)
    query_string = to_query_string(transformed)      

    response = {
        "tagged_query": tagged_query,
        "enriched_query": enriched_query, 
        "transformed_query": query_string,
        "tagger_data": tagger_data
    }

    return response

def generate_query_tree(text, tagger_data):
    query_tree, _, _ = generate_query_representations(text, tagger_data)
    return query_tree

def generate_query_representations(text, tagger_data):
    query_tree, tagged_query, enriched_query, doc_map = [], "", "", {}

    if (tagger_data['response'] and tagger_data['response']['docs']):
        for doc in tagger_data['response']['docs']:
            doc_map[doc['id']] = doc

    if (tagger_data['tags'] is not None):
        tags, lastEnd, metaData = tagger_data['tags'], 0, {}

        for tag in tags:
            matchText, doc_ids, best_doc_id = \
              tag['matchText'], tag['ids'], None

            for doc_id in doc_ids:
                if (best_doc_id):
                    if (doc_map[doc_id]['popularity'] >
                        doc_map[best_doc_id]['popularity']):
                        best_doc_id = doc_id
                else: #2
                    best_doc_id = doc_id
            best_doc = doc_map[best_doc_id]

            nextText = text[lastEnd:tag['startOffset']].strip()
            if (len(nextText) > 0):
                query_tree.append({ 
                  "type":"keyword", "known":False, 
                  "surface_form":nextText, "canonical_form":nextText })
                tagged_query += " " + nextText
                enriched_query += " { type:keyword, known: false," \
                             + " surface_form: \"" + nextText + "\"}"
            query_tree.append(best_doc)

            tagged_query += " {" + matchText + "}"
            enriched_query += json.dumps(best_doc)
            lastEnd = tag['endOffset']

        if (lastEnd < len(text)):
            finalText = text[lastEnd:len(text)].strip()
            if (len(finalText) > 0):
                query_tree.append({ 
                  "type":"keyword", "known":False, 
                  "surface_form":finalText,
                  "canonical_form":finalText })
                tagged_query += " " + finalText
                enriched_query += " { type:keyword, known: false," \
                             + " surface_form: \"" + finalText + "\"}"

    return query_tree, tagged_query, enriched_query

def process_basic_query(query_bytes):
    text = query_bytes.decode('UTF-8')
    response = {
        "resolved_query": '+{!edismax mm=100% v="' + escape_quotes_in_query(text) + '"}'
    }
    return response