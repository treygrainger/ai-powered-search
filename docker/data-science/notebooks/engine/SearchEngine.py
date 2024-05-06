from abc import ABC, abstractmethod

class SearchEngine(ABC):
    def __init__(self):
        pass
    
    @abstractmethod
    def health_check(self):
        pass
    
    @abstractmethod
    def print_status(self, solr_response):
        pass

    @abstractmethod
    def create_collection(self, name):
        pass

    @abstractmethod
    def get_collection(self, name):
        pass
    
    @abstractmethod
    def apply_schema_for_collection(self, collection):
        pass

    @abstractmethod
    def enable_ltr(self, collection):
        pass
    
    @abstractmethod
    def random_document_request(self, query):
        pass

    @abstractmethod
    def generate_query_time_boost(query):
        pass
    
    @abstractmethod
    def spell_check(self, collection, query, log=False):
        pass