import json
import requests
from ..Engine import Engine
from .opensearch_environment import OPENSEARCH_URL, SCHEMAS
from .OpenSearchCollection import OpenSearchCollection

STATUS_URL = f"{OPENSEARCH_URL}/_cluster/health"

class OpenSearchEngine(Engine):
    def __init__(self):
        pass

    def health_check(self):
        return requests.get(STATUS_URL).json()["status"] == "green"
    
    def print_status(self, response):
        #print(json.dumps(response, indent=2))
        "Prints the resulting status of a search engine request"
        pass

    def create_collection(self, name):
        print(f'Wiping "{name}" collection')
        response = requests.delete(f"{OPENSEARCH_URL}/{name}").json()

        print(f'Creating "{name}" collection')
        collection = self.get_collection(name)
        request = SCHEMAS[name]["schema"]
        response = requests.put(f"{OPENSEARCH_URL}/{name}", json=request).json()
        
        self.print_status(response)
        return collection

    def get_collection(self, name):
        "Returns initialized object for a given collection"
        return OpenSearchCollection(name, SCHEMAS[name]["id_field"])

    def set_search_defaults(self, collection, default_parser="edismax"):
        pass
        request = {
            "update-requesthandler": {
                "name": "/select",
                "class": "solr.SearchHandler",
                "defaults": {"defType": default_parser,
                                "indent": True}
            }
        }
        return requests.post(f"{OPENSEARCH_URL}/{collection.name}/config", json=request)