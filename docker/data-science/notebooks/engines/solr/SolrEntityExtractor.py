import requests
from aips.environment import SOLR_URL

def transform_response(query, response):
    return {"query": query,
            "tags": response["tags"],
            "entities": response["response"]["docs"]}
    
class SolrEntityExtractor:
    def __init__(self, collection_name):
        self.collection_name = collection_name
    
    def extract_entities(self, query, url_params=""):
        response = requests.post(f"{SOLR_URL}/{self.collection_name}/tag?{url_params}", query).json()
        return transform_response(query, response)