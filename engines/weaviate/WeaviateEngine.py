import json

import requests

from engines.Engine import Engine
from engines.weaviate.config import WEAVIATE_URL, SCHEMAS
from engines.weaviate.WeaviateCollection import WeaviateCollection

STATUS_URL = f"{WEAVIATE_URL}/v1/.well-known/live"

#https://weaviate.io/developers/weaviate/introduction
#https://weaviate.io/developers/weaviate/concepts/search#search-process

class WeaviateEngine(Engine):
    def __init__(self):
        super().__init__("Weaviate")

    def health_check(self):
        return requests.get(STATUS_URL).json()["status"] == "green"
    
    def print_status(self, response):
        #print(json.dumps(response, indent=2))
        "Prints the resulting status of a search engine request"
        pass

    def create_collection(self, name, log=False):
        print(f'Wiping "{name}" collection')
        requests.delete(f"{WEAVIATE_URL}/v1/schema/{name.capitalize()}")

        print(f'Creating "{name}" collection')
        request = SCHEMAS[name]["schema"] if name in SCHEMAS else {}
        response = requests.post(f"{WEAVIATE_URL}/v1/schema", json=request)
        if log: 
            print("Schema:", json.dumps(request, indent=2))
            print("Status:", response)
            print("Response:", response)
        collection = self.get_collection(name)
        return collection

    def get_collection(self, name):
        "Returns initialized object for a given collection"
        #id_field = SCHEMAS.get(name, {}).get("id_field", "_id")
        name = name.capitalize()
        return WeaviateCollection(name)#, id_field)