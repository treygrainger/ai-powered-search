import sys
sys.path.append('..')
import urllib.parse
import json
from aips import *
import requests

def keyword_search(text):     
    q = urllib.parse.quote(text)
    #print(q)
    #q=text.replace("+", "%2B") #so it doesn't get interpreted as space
    qf="text_t"
    defType="lucene"
     
    return requests.get(SOLR_URL + "/reviews/select?q=" + q + "&qf=" + qf + "&defType=" + defType).text

def query_solr(collection, query):   
    response = requests.post(SOLR_URL + '/' + collection + '/select',
          {
            "type": 'POST',
            "data": json.puts(query),
            "dataType": 'json',
            "contentType": 'application/json'          
          })

    return response

def tag_places(post_body):
    return requests.post(SOLR_URL + '/reviews/select', json=post_body).text