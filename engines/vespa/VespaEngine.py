import json

import requests

from engines.Engine import AdvancedFeatures, Engine
from engines.vespa.config import VESPA_URL
from engines.vespa.VespaCollection import VespaCollection

STATUS_URL = f"{VESPA_URL}/state/v1/health"

#https://docs.vespa.ai/en/reference/query-api-reference.html
#https://docs.vespa.ai/en/writing/document-v1-api-guide.html

class VespaEngine(Engine):
    def __init__(self):
        super().__init__("vespa")

    def does_collection_exist(self, name):
        try:
            self.get_collection(name).get_document_count()
            return True
        except:
            return False
    
    def get_supported_advanced_features(self):
        return [AdvancedFeatures.LTR]

    def health_check(self):
        try:
            response = requests.get(STATUS_URL)
            status = response.status_code == 200
            print(f"Vespa engine is {'online' if status else 'offline'}")
            return status
        except Exception as ex:
            print(f"Vespa failed the health check: {ex}")
            return False
    
    def print_status(self, response):
        print(json.dumps(response, indent=2))

    def create_collection(self, name, log=False):
        return self.get_collection(name)

    def get_collection(self, name):
        return VespaCollection(name)