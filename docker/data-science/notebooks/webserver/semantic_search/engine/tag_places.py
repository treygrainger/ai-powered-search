import sys
sys.path.append('..')
from aips import *
import requests, json

def tag_places(post_body):
    x = json.dumps(post_body)
    return requests.post(SOLR_URL + '/reviews/select', json=post_body).text