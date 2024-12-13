import json

import requests

from engines.Engine import Engine
from engines.opensearch.config import OPENSEARCH_URL, SCHEMAS
from engines.opensearch.OpenSearchCollection import OpenSearchCollection

STATUS_URL = f"{OPENSEARCH_URL}/_cluster/health"

class OpenSearchEngine(Engine):
    def __init__(self):
        super().__init__("OpenSearch")

    def health_check(self):
        status = requests.get(STATUS_URL).json()["status"] in ["green", "yellow"]
        if status:
            print("OpenSearch engine is online")
        return status
    
    def print_status(self, response):
        #print(json.dumps(response, indent=2))
        "Prints the resulting status of a search engine request"
        pass

    def create_collection(self, name, log=False):
        print(f'Wiping "{name}" collection')
        response = requests.delete(f"{OPENSEARCH_URL}/{name}").json()

        print(f'Creating "{name}" collection')
        collection = self.get_collection(name)
        request = SCHEMAS[name]["schema"] if name in SCHEMAS else {}
        response = requests.put(f"{OPENSEARCH_URL}/{name}", json=request).json()
        if log: 
            print("Schema:", json.dumps(request, indent=2))
        if log: 
            print("Status:", json.dumps(response, indent=2))
        return collection

    def get_collection(self, name):
        "Returns initialized object for a given collection"
        id_field = SCHEMAS.get(name, {}).get("id_field", "_id")
        return OpenSearchCollection(name, id_field)