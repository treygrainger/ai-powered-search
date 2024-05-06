from abc import ABC, abstractmethod

class Collection(ABC):
    def __init__(self, name):
        self.name = name
        
    @abstractmethod
    def commit(self):
        pass 
    
    @abstractmethod
    def write(self, dataframe):
        pass
    
    @abstractmethod
    def add_documents(self, docs, commit=True):
        pass
    
    @abstractmethod
    def transform_request(self, **search_args):
        pass
    
    @abstractmethod
    def transform_response(self, search_response):    
        pass

    @abstractmethod
    def native_search(self, request=None, data=None):
        pass
    
    @abstractmethod
    def vector_search(self, **search_args):
        pass
    
    def search(self, **search_args):
        request = self.transform_request(**search_args)
        search_response = self.native_search(request=request)
        return self.transform_response(search_response)