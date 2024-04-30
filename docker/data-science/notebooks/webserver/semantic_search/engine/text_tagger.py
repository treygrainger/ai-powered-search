import requests
from aips.environment import SOLR_URL

class TextTagger:
    def __init__(self, collection_name):
        self.collection_name = collection_name

    def tag_query(self, query, url_params=""):
        return requests.post(f"{SOLR_URL}/{self.collection_name}/tag?{url_params}", query).json()