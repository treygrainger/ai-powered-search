from abc import ABC, abstractmethod
class SparseSemanticSearch(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def location_distance(self, query, position):
        "A semantic function to create a location distance query. Applies a transformed query node it to the query tree."
        pass

    @abstractmethod
    def popularity(self, query, position):
        "A semantic function to create a popularity query. Applies a transformed query node it to the query tree."
        pass
       
    @abstractmethod 
    def transform_query(self, query_tree):
        "Transforms the query tree into an engine specific query tree"
        pass
    
    @abstractmethod
    def generate_basic_query(self, query):
        "Creates a basic engine specific search query"
        pass