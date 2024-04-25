import requests
from env import *
import time
import json
from pyspark.sql import SparkSession

class SolrCollection:
    def __init__(self, name):
        #response = requests.get(f"{SOLR_COLLECTIONS_URL}/?action=LIST")
        #print(response)
        #collections = response.json()["collections"]
        #if name.lower() not in [s.lower() for s in collections]:
        #    raise ValueError(f"Collection name invalid. '{name}' does not exists.")
        self.name = name
        
    def commit(self):
        requests.post(f"{SOLR_URL}/{self.name}/update?commit=true&waitSearcher=true")
        time.sleep(2)

    def write(self, dataframe):
        opts = {"zkhost": AIPS_ZK_HOST, "collection": self.name,
                "gen_uniq_key": "true", "commit_within": "5000"}
        dataframe.write.format("solr").options(**opts).mode("overwrite").save()
        self.commit()
    
    def add_documents(self, docs, commit=True):
        print(f"\nAdding Documents to '{self.name}' collection")
        response = requests.post(f"{SOLR_URL}/{self.name}/update?commit=true", json=docs).json()
        if commit:
            self.commit()
        return response
    
    def transform_request(self, **search_args):
        request = {
            "query": "*:*",
            "limit": 10,
            "params": {}     
        }
        
        for name, value in search_args.items():
            match name:
                case "q":
                    request.pop("query")
                    request["params"]["q"] = value
                case "query":
                    request["query"] = value if value else "*:*"
                case "query_parser":
                    request["params"]["defType"] = value
                case "query_fields":
                    request["params"]["qf"] = value
                case "return_fields":
                    request["fields"] = value
                case "filters":
                    request["filter"] = []
                    for f in value:
                        filter_value = f'({" ".join(f[1])})' if type(f[1]) is list else f[1] 
                        request["filter"].append(f"{f[0]}:{filter_value}")
                case "limit":
                    request["limit"] = value
                case "order_by":
                    request["sort"] = ", ".join([f"{column} {sort}" for (column, sort) in value])  
                case "facet":
                    request["facet"] = value
                case "rerank_query":
                    request["params"]["rq"] = value
                case "default_operator":
                    request["params"]["q.op"] = value
                case "min_match":
                    request["params"]["mm"] = value
                case "query_boosts":
                    request["params"]["boost"] = "sum(1,query($boost_query))"
                    if type(value) is tuple:
                        value = f"{value[0]}:({value[1]})"
                    request["params"]["boost_query"] = value
                case "index_time_boost":
                    request["params"]["boost"] = f'payload({value[0]}, "{value[1]}", 1, first)'
                case "explain":
                    if "fields" not in request:
                        request["fields"] = []
                    request["fields"].append("[explain style=html]")
                case "hightlight":
                    request["params"]["hl"] = True
                case _:
                    request["params"][name] = value
        if "log" in search_args:
            print("Search Request:")
            print(json.dumps(request, indent="  "))
        return request
    
    def transform_response(self, search_response):    
        response = {"docs": search_response["response"]["docs"]}
        if "highlighting" in search_response:
            response["highlighting"] = search_response["highlighting"]
        return response
        
    def native_search(self, request=None, data=None):
        return requests.post(f"{SOLR_URL}/{self.name}/select", json=request, data=data).json()
    
    def search(self, **search_args):
        request = self.transform_request(**search_args)
        search_response = self.native_search(request=request)
        return self.transform_response(search_response)
    
    def vector_search(self, **search_args):
        field = search_args["query_field"]
        k = search_args["k"] if "k" in search_args else 10
        query_vector = search_args["query_vector"]
        request = {
            "query": "{!knn " + f'topK={k} f={field}' + "}" + str(query_vector),
            "limit": 5,
            "fields": ["id", "score", "title"]
        }        
        for name, value in search_args.items():
            match name:
                case "limit":
                    request["limit"] = value
                case "rerank_query":
                    rq = "{" + f'!rerank reRankQuery=$rq_query reRankDocs={value["rerank_quantity"]} reRankWeight=1' + "}"
                    request["params"]["rq"] = rq
                    request["params"]["rq_query"] = "{!knn f=" + value["query_field"] + " topK=10}" + value["query_vector"]
        return self.native_search(request=request)["response"]["docs"]