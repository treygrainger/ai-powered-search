import random
from re import search
import requests
from engines.Collection import Collection, is_vector_search, DEFAULT_SEARCH_SIZE, DEFAULT_NEIGHBORS
from engines.opensearch.config import OPENSEARCH_URL
import time
import json
from pyspark.sql import Row
from aips.spark import get_spark_session
import numbers

def generate_vector_search_request(search_args):
    vector = search_args.pop("query")
    query_fields = search_args.pop("query_fields", [])
    if not isinstance(query_fields, list):
        raise TypeError("query_fields must be a list")
    elif len(query_fields) == 0:
        raise ValueError("You must specificy at least one field in query_fields")
    else: 
        field = query_fields[0]
    k = search_args.pop("k", DEFAULT_NEIGHBORS)
    if "limit" in search_args: 
        if int(search_args["limit"]) > k:
            k = int(search_args["limit"]) #otherwise will only get k results
    request = {"query": {"knn": {field: {"vector": vector,
                                         "k": k}}},
               "size": search_args.get("limit", DEFAULT_SEARCH_SIZE)}             
    if "log" in search_args:
        print("Search Request:")
        print(json.dumps(request, indent="  "))
    return request
        
def create_filter(field, value):
    if value != "*":
        key = "terms" if isinstance(value, list) else "term"
        return {key: {field: value}}
    else:
        return {"exists": {"field": field}}
    
def query_string_clause(search_args):
    "Returns the query string from the args or from concatenating query clause strings"

    def retrive_strings(c): return c if isinstance(c, str) else ""

    query_string = "*"
    if "query" in search_args:
        if isinstance(search_args["query"], list):
            query_string = " ".join(list(map(retrive_strings, search_args["query"])))
        else:
            query_string = search_args["query"]
    return query_string.strip()

def should_clauses(search_args):
    return list(filter(lambda c: isinstance(c, dict) and "geo_distance" not in c,
                       search_args.get("query", [])))

def must_clauses(search_args):
    return list(filter(lambda c: isinstance(c, dict) and "geo_distance" in c,
                       search_args.get("query", [])))

class OpenSearchCollection(Collection):
    def __init__(self, name, id_field="_id"):
        super().__init__(name)
        self.id_field = id_field
        
    def commit(self):
        time.sleep(2)

    def write(self, dataframe, overwrite=True):
        opts = {"opensearch.nodes": OPENSEARCH_URL,
                "opensearch.net.ssl": "false",
                "opensearch.batch.size.entries": 500}
        if self.id_field != "_id":
            opts["opensearch.mapping.id"] = self.id_field
        mode = "overwrite" if overwrite else "append"
        dataframe.write.format("opensearch").options(**opts).mode(mode).save(self.name)
        self.commit()
        print(f"Successfully written {dataframe.count()} documents")
    
    def add_documents(self, docs, commit=True):
        dataframe = get_spark_session().createDataFrame(Row(**d) for d in docs)
        self.write(dataframe, overwrite=False)
    
    def transform_request(self, **search_args):
        #Of the many approaches to implement all these features, the most
        #straightforward and effective is to use the query_string functionality
        #https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html
        #Does multimatching, supports default operator, does dis_max by default.
        if is_vector_search(search_args):
            return generate_vector_search_request(search_args)
        
        query = query_string_clause(search_args)
        should = should_clauses(search_args)
        must = must_clauses(search_args)
        query_fields = {"fields": search_args["query_fields"]} if "query_fields" in search_args else {}
        query_clause = {"query_string": {"query": query,
                        "boost": 0.454545454,
                        "default_operator": search_args.get("default_operator", "OR")} | query_fields}
        must.append(query_clause)
        request = {"query": {"bool": {"must": must}},
                   "size": DEFAULT_SEARCH_SIZE}
        
        for name, value in search_args.items():
            match name:
                case "return_fields":
                    request["fields"] = value
                case "filters":
                    request["query"]["bool"]["filter"] = [create_filter(f, v) for (f, v) in value]
                case "limit":
                    request["size"] = value
                case "order_by":
                    request["sort"] = [{column if column != "score" else "_score": {"order": sort}}
                                       for (column, sort) in value] 
                case "query_boosts":
                    boost_field = value[0] if isinstance(value, tuple) else "upc"
                    boost_string = value[1] if isinstance(value, tuple) else value
                    boosts = [(b.split("^")[0], b.split("^")[1])
                              for b in boost_string.split(" ")]
                    clauses = [{"match": {boost_field: {"query": b[0],
                                                        "boost": b[1]}}}
                               for b in boosts]
                    should.extend(clauses)
                case "min_match":
                    raise Exception("To be implemented (Only used in ch05)")
                case "index_time_boost":
                    should.append({"rank_feature": {"field": f"{value[0]}.{value[1]}"}})
                case "explain":
                    request["explain"] = value
                case "hightlight":
                    request["highlight"] = {"fields": {value: {}}}
                case "ubi":
                    request["ext"] = {"ubi": value}
                case _:
                    pass

        request["query"]["bool"]["should"] = should

        if "log" in search_args:
            print("Search Request:")
            print(json.dumps(request, indent="  "))
        return request
    
    def transform_response(self, search_response):
        def format_doc(doc):
            formatted = doc["_source"] | {"id": doc["_id"],
                                          "score": doc["_score"]}
            if "_explanation" in doc:
                formatted["[explain]"] = doc["_explanation"]
            return formatted
            
        if "hits" not in search_response:
            raise ValueError(search_response)
        response = {"docs": [format_doc(d)
                             for d in search_response["hits"]["hits"]]}
        if "highlighting" in search_response:
            response["highlighting"] = search_response["highlighting"]
        return response
        
    def native_search(self, request=None, data=None):
        return requests.post(f"{OPENSEARCH_URL}/{self.name}/_search", json=request, data=data).json()

    def search(self, **search_args):
        request = self.transform_request(**search_args)
        search_response = self.native_search(request=request)
        return self.transform_response(search_response)
    
    def spell_check(self, query, log=False):
        request = {
            "suggest": {
                "spell-check" : {
                "text" : query,
                "term" : {
                    "field" : "_text_",
                    "suggest_mode" : "missing"                    
                }
            }
            }
        }
        if log: print(json.dumps(request, indent=2))
        response = self.native_search(request=request)
        if log: print(json.dumps(response, indent=2))
        suggestions = {}
        if len(response["suggest"]["spell-check"]):
            suggestions = {term["text"]: term["freq"] for term
                            in response["suggest"]["spell-check"][0]["options"]}
        return suggestions