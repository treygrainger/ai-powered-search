import io
import json
import os
from pathlib import Path
import time  
import shutil
import zipfile

import requests

from engines.Engine import AdvancedFeatures, Engine
import engines.vespa.config as config #import VESPA_URL, VESPA_CONFIG_URL
from engines.vespa.VespaCollection import VespaCollection

STATUS_URL = f"{config.VESPA_URL}/state/v1/health"
APP_STATUS_URL = f"{config.VESPA_URL}/ApplicationStatus"
APP_INFO_URL = f"{config.VESPA_CONFIG_URL}/application/v2/tenant/default/application/default"
DEPLOY_URL = f"{config.VESPA_CONFIG_URL}/application/v2/tenant/default/prepareandactivate"

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
        zip_file = shutil.make_archive("any.zip", "zip", directory)
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
    
    def execute_with_retry(self, http_fn, url, headers={}, data=None, message="Request", log=False):
        response = None
        while retries >= 0:
            response = None
            try:
                response = http_fn(url, headers=headers, data=data)
                if log: print(response, response.json())
            except:
                pass
            if response and response.status_code == 200:
                if log: print(f"{message}: Succeeded")
                return response
            time.sleep(5)
            retries -= 1
        if retries == -1:
            if log: print(f"{message}: Failed")
            response.raise_for_status()

    def deploy_application(self, force_deploy=False, log=False, retries=12):
        if not self.is_application_deployed(log=log) or force_deploy:
            self.execute_with_retry(requests.post, DEPLOY_URL, headers={"Content-Type": "application/zip"},
                                    data=self.generate_zip_binary(), message="Application Deployment", log=log)
            self.execute_with_retry(requests.get, APP_STATUS_URL, message="Application Initialization", log=log)
            
    def create_collection(self, name, force_rebuild=False, log=False):
        self.deploy_application(log=log)
        if force_rebuild:
            try:
                requests.delete(f"{self.vespa_url}/document/v1/{self.namespace}/{self.name}/docid?selection=true&cluster={self.namespace}")
            except:
                pass
        return self.get_collection(name)

    def get_collection(self, name):
        return VespaCollection(name)