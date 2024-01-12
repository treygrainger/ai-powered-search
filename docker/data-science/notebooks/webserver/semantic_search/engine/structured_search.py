import sys
sys.path.append('..')
from aips import *
import requests, json

def structured_search(json_query):
    x = json.dumps(json_query)
    return requests.post(SOLR_URL + '/reviews/select', json=json_query).text