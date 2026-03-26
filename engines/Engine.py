from abc import ABC, abstractmethod
from enum import Enum

class AdvancedFeatures(Enum):
    SKG = "SKG"
    TEXT_TAGGING = "TEXT_TAGGING"
    LTR = "LTR"

class Engine(ABC):
    def __init__(self, name):
        self.name = name

    @abstractmethod
    def get_supported_advanced_features(self):
        "Returns the list of implemented advanced features for the given engine"
        pass

    @abstractmethod
    def health_check(self):
        "Checks the state of the search engine returning a boolean"
        pass
    
    @abstractmethod
    def print_status(self, response):
        "Prints the resulting status of a search engine request"
        pass

    @abstractmethod
    def does_collection_exist(self, name):
        "Checks for the existance of the collection"
        pass
    
    def is_collection_healthy(self, name, expected_count, log=False):
        collection_exists = self.does_collection_exist(name)
        if log: print(f"Collection [{name}] exists? {collection_exists}")

        document_count = self.get_collection(name).get_document_count()
        if log: print(f"Documents [{document_count} / {expected_count}]")
        
        return collection_exists and document_count == expected_count    

    @abstractmethod
    def create_collection(self, name, force_rebuild=True):
        "Create and initialize the schema for a collection, returns the initialized collection"
        pass

    @abstractmethod
    def get_collection(self, name):
        "Returns initialized object for a given collection"
        pass