import io
import json
import os
from pathlib import Path
import time  
import shutil
import zipfile

import requests

from engines.Engine import AdvancedFeatures, Engine
from engines.vespa.config import VESPA_URL, VESPA_CONFIG_URL
from engines.vespa.VespaCollection import VespaCollection

STATUS_URL = f"{VESPA_URL}/state/v1/health"
APP_INFO_URL = f"{VESPA_CONFIG_URL}/application/v2/tenant/default/application/default"
DEPLOY_URL = f"{VESPA_CONFIG_URL}/application/v2/tenant/default/prepareandactivate"

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
      
    def generate_zip_binary(self, directory="./engines/vespa/build/application/"): 
        zip_file = shutil.make_archive("any.zip", 'zip', directory)
        with open(zip_file, mode='rb') as file:
            fileContent = file.read()
        os.remove(zip_file)
        return fileContent 

    def is_application_deployed(self, log=False):
        try:
            response = requests.get(APP_INFO_URL)
            if log: print(response)
            return response.status_code == 200
        except:
            return False
    
    def deploy_application(self, force_deploy=True, log=False):
        if not self.is_application_deployed() or force_deploy:
            response = requests.post(DEPLOY_URL, headers={"Content-Type": "application/zip"},
                                     data=self.generate_zip_binary())
            if log: print(response, response.json())
            session_id = response.json()["session-id"]
            response = requests.put(f"{VESPA_CONFIG_URL}/application/v2/tenant/default/session/{session_id}/active")
            if log: print(response, response.json())
            
    def create_collection(self, name, log=False):
        self.deploy_application(log=log)
        return self.get_collection(name)

    def get_collection(self, name):
        return VespaCollection(name)