import random
import requests
from engines.Collection import Collection
from engines.opensearch.opensearch_environment import OPENSEARCH_URL
import time
import json
from pyspark.sql import Row, SparkSession

class OpenSearchCollection(Collection):
    def __init__(self, name, id_field=None):
        if not id_field:
            id_field="_id"
        super().__init__(name)
        self.id_field = id_field
        
    def commit(self):
        time.sleep(2)

    def write(self, dataframe):
        opts = {"opensearch.nodes": OPENSEARCH_URL,
                "opensearch.net.ssl": "false"}
        if self.id_field != "_id":
            opts["opensearch.mapping.id"] = self.id_field
        dataframe.write.format("opensearch").options(**opts).mode("overwrite").save(self.name)
        self.commit()
        print(f"Successfully written {dataframe.count()} documents")
    
    def add_documents(self, docs, commit=True):
        spark = SparkSession.builder.appName("AIPS").getOrCreate()
        dataframe = spark.createDataFrame(Row(**d) for d in docs)
        self.write(dataframe)
    
    def transform_request(self, **search_args):
        request = {
            "query": "*:*",
            "size": 10
            }
        query_fields = search_args.get("query_fields", "*")
        for name, value in search_args.items():
            match name:
                case "query_fields":
                    pass
                case "query":
                    #
                    queries = [{"match": {f: {"query": value, "boost": 0.454545454}}}
                               for f in query_fields]
                    request["query"] = {"dis_max": {"queries": queries}}
                case "return_fields":
                    request["fields"] = value
                case "filters":
                    request["filter"] = []
                    for f in value:
                        filter_value = f'({" ".join(f[1])})' if isinstance(f[1], list) else f[1] 
                        request["filter"].append(f"{f[0]}:{filter_value}")
                case "limit":
                    request["size"] = value
                case "order_by":
                    request["sort"] = [{column if column != "score" else "_score":
                                        {"order": sort}} for (column, sort) in value] 
                    #request["sort"] =  ",".join([f"{field}:{dir}" for (field, dir) in value])
                    #request["sort"] = request["sort"].replace("score", "_score")
                #    request["sort"] = [{"score": {"order": "asc"}}]
                case "rerank_query":
                    request["params"]["rq"] = value
                case "default_operator":
                    request["default_operator"] = value
                case "min_match":
                    request["params"]["mm"] = value
                case "query_boosts":
                    request["params"]["boost"] = "sum(1,query($boost_query))"
                    if isinstance(value, tuple):
                        value = f"{value[0]}:({value[1]})"
                    request["params"]["boost_query"] = value
                case "index_time_boost":
                    request["params"]["boost"] = f'payload({value[0]}, "{value[1]}", 1, first)'
                case "explain":
                    request["explain"] = value
                case "hightlight":
                    request["highlight"] = {"fields": {value: {}}}
                case _:
                    pass                    
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
    
    def vector_search(self, **search_args):
        field = search_args["query_field"]
        k = search_args["k"] if "k" in search_args else 10
        query_vector = search_args["query_vector"]
        request = {
            "query": "{!knn " + f'topK={k} f={field}' + "}" + str(query_vector),
            "limit": 5,
            "fields": ["*"],
            "params": {}
        }
        for name, value in search_args.items():
            match name:
                case "log":
                    request["params"]["debugQuery"] = True
                case "limit":
                    request["limit"] = value
                case "rerank_query":
                    rq = "{" + f'!rerank reRankQuery=$rq_query reRankDocs={value["rerank_quantity"]} reRankWeight=1' + "}"
                    request["params"]["rq"] = rq
                    request["params"]["rq_query"] = "{!knn f=" + value["query_field"] + " topK=10}" + value["query_vector"]
        response = self.native_search(request=request)
        if "log" in search_args:
            print(response)
        return self.transform_response(response)
    
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