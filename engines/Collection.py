from abc import ABC, abstractmethod
import numbers
import aips.environment as env
import json

DEFAULT_SEARCH_SIZE = 10
DEFAULT_NEIGHBORS = 10

def is_vector_search(search_args):
    return "query" in search_args and \
           isinstance(search_args["query"], list) and \
           len(search_args["query"]) == len(list(filter(lambda o: isinstance(o, numbers.Number),
                                                        search_args["query"])))
class Collection(ABC):
    def __init__(self, name):
        self.name = name
        
    @abstractmethod 
    def get_engine_name(self):
        "Returns the name of the search engine for the collection"
        pass  

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
    def get_document_count(self):
        "Returns the number of documents in the index"
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

    def hybrid_search(self, searches=[], limit=None, algorithm="rrf", algorithm_params={}):
        hybrid_search_results = None
        match algorithm:
            case "rrf":
                search_results = [self.search(**request)["docs"]
                                  for request in searches]

                hybrid_search_scores = reciprocal_rank_fusion(search_results,
                                                              algorithm_params.get("k"))
                scored_docs = merge_search_results(search_results, 
                                                   hybrid_search_scores)    
                return {"docs": scored_docs[:limit]}
            case "lexical_vector_rerank":
                lexical_search_request = searches[0]
                searches[1]["k"] = algorithm_params.get("k", 10) #TODO: should probably default to "limit" instead of 10
                lexical_search_request["rerank_query"] = searches[1]
                return self.search(**lexical_search_request)
        return hybrid_search_results

def merge_search_results(search_results, scores):
    merged_results = {}
    for results in search_results:
        for doc in results:
            if doc["id"] in merged_results:
                merged_results[doc["id"]] = {**doc, **merged_results[doc["id"]]}
            else: 
                merged_results[doc["id"]] = doc
    return [{**merged_results[id], "score": score}
            for id, score in scores.items()]
    
    
def reciprocal_rank_fusion(search_results, k=None):
    if k is None: k = 60
    scores = {}
    for ranked_docs in search_results:
        for rank, doc in enumerate(ranked_docs, 1):
            scores[doc["id"]] = scores.get(doc["id"], 0)  + (1.0 / (k + rank))
    sorted_scores = dict(sorted(scores.items(), key=lambda item: item[1], reverse=True))
    return sorted_scores