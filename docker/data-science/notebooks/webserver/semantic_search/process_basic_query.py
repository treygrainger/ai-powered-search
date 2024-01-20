import sys
sys.path.append('..')
from semantic_search.query_tree.escape_quotes_in_query import *

def process_basic_query(query_bytes):
    text = query_bytes.decode('UTF-8')
    response = {
        "resolved_query": '+{!edismax mm=100% v="' + escape_quotes_in_query(text) + '"}'
    }
    return response