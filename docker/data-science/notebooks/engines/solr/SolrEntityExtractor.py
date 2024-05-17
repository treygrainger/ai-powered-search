import requests
from aips.environment import SOLR_URL
from engines.EntityExtractor import EntityExtractor

def transform_response(query, response):
    return {"query": query,
            "tags": response["tags"],
            "entities": response["response"]["docs"]}
    
class SolrEntityExtractor(EntityExtractor):
    def __init__(self, collection_name):
        super().__init__(collection_name)
    
    def extract_entities(self, query):
        response = requests.post(f"{SOLR_URL}/{self.collection_name}/tag", query).json()
        return transform_response(query, response)