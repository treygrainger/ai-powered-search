from abc import ABC, abstractmethod

class Collection(ABC):
    def __init__(self, name):
        self.name = name
        
    @abstractmethod
    def commit(self):
        "Force the collection to commit all uncommited data into the collection"
        pass 
    
    @abstractmethod
    def write(self, dataframe):
        "Writes a pyspark dataframe containing documents into the collection"
        pass
    
    @abstractmethod
    def add_documents(self, docs, commit=True):
        "Adds a collection of documents into the collection"
        pass
    
    @abstractmethod
    def transform_request(self, **search_args):
        "Transforms a generic search request into a native search request"
        pass
    
    @abstractmethod
    def transform_response(self, search_response):
        "Transform a native search response into a generic search response"
        pass

    @abstractmethod
    def native_search(self, request=None):
        "Executes a search against the search engine given a native search request"
        pass
    
    @abstractmethod        
    def vector_search(self, **search_args):
        "Executes a vector search given a vector search request"
        pass

    @abstractmethod
    def search_for_random_document(self, query):
        "Searches for a random document matching the query"
        pass
    
    @abstractmethod
    def spell_check(self, query, log=False):
        "Execute a spellcheck against the collection"
        pass
    
    def search(self, **search_args):
        request = self.transform_request(**search_args)
        search_response = self.native_search(request=request)
        return self.transform_response(search_response)
    