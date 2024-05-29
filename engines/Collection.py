from abc import ABC, abstractmethod
from xmlrpc.client import boolean
import aips.environment as env
import json

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
    def transform_lexical_request(self, **search_args):
        "Transforms a generic lexical search request into a native search request"
        pass
    
    @abstractmethod
    def transform_vector_request(self, **search_args):
        "Transforms a generic vector search request into a native search request"
        pass    
    
    @abstractmethod
    def transform_lexical_response(self, search_response):
        "Transform a native lexical search response into a generic search response"
        pass

    @abstractmethod
    def transform_vector_response(self, search_response):
        "Transform a native vector search response into a generic search response"
        pass

    @abstractmethod
    def native_search(self, request=None):
        "Executes a search against the search engine given a native search request"
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
        """
        Searches the collection
        :param str query: The main query for the search request
        :param str query_parser: The name of the query parser to use in the search
        :param list of str query_fields: the fields to query against
        :param list of str return_fields: the fields to return on each document
        :param list of tuple of str filters: A list of tuples (field, value) to filter the results by 
        :param int limit: The number of results to return
        :param list of tuple of str order_by: A list of tuples (field, ASC/DESC) to order the results by
        :param str rerank_query: A query to rerank the results by
        :param str default_operator: Sets the default operator of the search query (AND/OR)
        :param str min_match: Specificies the minimum matching constraints for matching documents
        :param str query_boosts: A boost query to boost documents at query time
        :param tuple of str index_time_boosts: An index time boost
        :param boolean explain: Enables debugging on the request
        :param boolean log: Enables logging for the query
        :param boolean highlight: Returns results with highlight information (if supported)
        """
        request = self.transform_request(**search_args)
        if "log" in search_args or env.get("PRINT_REQUESTS", False):
            print(json.dumps(request, indent=2))
        search_response = self.native_search(request=request)
        if "log" in search_args:
            print(json.dumps(search_response, indent=2))
        return self.transform_response(search_response)
    
    @abstractmethod        
    def vector_search(self, **search_args):
        "Executes a vector search given a vector search request"
        pass
    
    @abstractmethod        
    def hybrid_search(self, lexical_search_args, vector_search_args, algorithm):
        "Executes a vector search given a vector search request"
        pass