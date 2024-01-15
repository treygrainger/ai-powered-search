import sys
sys.path.append('..')
from aips import *
import requests

def query_solr(collection,query):   
    response = requests.post(SOLR_URL + '/' + collection + '/select',
          {
            "type": 'POST',
            "data": json.puts(query),
            "dataType": 'json',
            "contentType": 'application/json'          
          });   

    return response