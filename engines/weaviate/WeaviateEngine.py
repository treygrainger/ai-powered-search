from abc import ABC, abstractmethod
from engines.Engine import Engine
from engines.weaviate.WeaviateCollection import WeaviateCollection
import weaviate

class WeaviateEngine(Engine):
    def __init__(self):
        self.client = weaviate.connect_to_local(port=8090)
    
    def health_check(self):
        "Checks the state of the search engine returning a boolean"
        return self.client.is_ready()
    
    # @abstractmethod
    def print_status(self, response):
        "Prints the resulting status of a search engine request"
        pass

    # @abstractmethod
    def create_collection(self, name):
        "Create and initialize the schema for a collection, returns the initialized collection"
        pass

    # @abstractmethod
    def get_collection(self, name):
        "Returns initialized object for a given collection"
        pass
    
    # @abstractmethod
    def apply_schema_for_collection(self, collection):
        "Applies the appriorate schema for a given collection"
        pass

    # @abstractmethod
    def enable_ltr(self, collection):
        "Initializes LTR dependencies for a given collection"
        pass