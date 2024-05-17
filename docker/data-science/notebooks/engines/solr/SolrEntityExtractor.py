import requests
from aips.environment import SOLR_URL
from engines.solr.SolrCollection import SolrCollection
from engines.EntityExtractor import EntityExtractor

def transform_response(query, response):
    return {"query": query,
            "tags": response["tags"],
            "entities": response["response"]["docs"]}
    
class SolrEntityExtractor(EntityExtractor):
    def __init__(self, collection):
        if not isinstance(collection, SolrCollection):
            raise TypeError("Only supports a SolrCollection")
        super().__init__(collection)
    
    def extract_entities(self, query):
        response = requests.post(f"{SOLR_URL}/{self.collection.name}/tag", query).json()
        return transform_response(query, response)