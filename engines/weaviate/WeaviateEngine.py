import json

import requests

from engines.Engine import AdvancedFeatures, Engine
from engines.weaviate.config import WEAVIATE_URL, SCHEMAS
from engines.weaviate.WeaviateCollection import WeaviateCollection

STATUS_URL = f"{WEAVIATE_URL}/v1/.well-known/live"

#https://weaviate.io/developers/weaviate/introduction
#https://weaviate.io/developers/weaviate/concepts/search#search-process

class WeaviateEngine(Engine):
    def __init__(self):
        super().__init__("weaviate")

    def does_collection_exist(self, name):
        response = requests.get(f"{WEAVIATE_URL}/v1/schema/{name.capitalize()}")
        return response.status_code == 200
    
    def get_supported_advanced_features(self):
        return [AdvancedFeatures.LTR]

    def health_check(self):
        try:
            status = requests.get(STATUS_URL).json()["status"] == "green"
            print(f"Weaviate engine is {'online' if status else 'offline'}")
            return status
        except:
            print("Weaviate failed the health check.")
            return False
    
    def print_status(self, response):
        print(json.dumps(response, indent=2))

    def create_collection(self, name, log=False):
        print(f'Wiping "{name}" collection')
        requests.delete(f"{WEAVIATE_URL}/v1/schema/{name.capitalize()}")

        print(f'Creating "{name}" collection')
        request = SCHEMAS[name]["schema"] if name in SCHEMAS else {}
        response = requests.post(f"{WEAVIATE_URL}/v1/schema", json=request)
        if log: 
            print("Schema:", json.dumps(request, indent=2))
            print("Status:", response)
            print("Response:", response.json())
        collection = self.get_collection(name)
        return collection

    def get_collection(self, name):
        "Returns initialized object for a given collection"
        #id_field = SCHEMAS.get(name, {}).get("id_field", "_id")
        name = name.capitalize()
        return WeaviateCollection(name)#, id_field)