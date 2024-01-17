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

#Temporary attempt to create a functional tag request
def tag_request(query):
    return {
        "query": query,
        "params": {
            "matchText": "true",
			"json.nl": "map",
			"field": "name_tag",
			"echoParams": "all",
			"fl": "id,type,canonical_form,name,country:countrycode_s,admin_area:admin_code_1_s,popularity,*_p,semantic_function",
			"sort": "popularity desc"
		}
    }