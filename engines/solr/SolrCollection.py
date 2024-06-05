import random
from numpy import isin
import requests
from engines.Collection import Collection
from aips.environment import SOLR_URL, AIPS_ZK_HOST
import time
import json

class SolrCollection(Collection):
    def __init__(self, name):
        #response = requests.get(f"{SOLR_COLLECTIONS_URL}/?action=LIST")
        #print(response)
        #collections = response.json()["collections"]
        #if name.lower() not in [s.lower() for s in collections]:
        #    raise ValueError(f"Collection name invalid. '{name}' does not exists.")
        super().__init__(name)
        
    def commit(self):
        requests.post(f"{SOLR_URL}/{self.name}/update?commit=true&waitSearcher=true")
        time.sleep(5)

    def write(self, dataframe):
        opts = {"zkhost": AIPS_ZK_HOST, "collection": self.name,
                "gen_uniq_key": "true", "commit_within": "5000"}
        dataframe.write.format("solr").options(**opts).mode("overwrite").save()
        self.commit()
        print(f"Successfully written {dataframe.count()} documents")
    
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
        #handle before standard handling search arg to prevent conflicting request params
        if "query" in search_args and isinstance(search_args["query"], list):
            vector = search_args.pop("query")
            #note: need to make more robust so it doesn't blow up when no field specified and to handle more than one field
            #just making it work for now by using the first field.
            field = search_args.pop("query_fields")[0]
            k = search_args.pop("k", 10)
            if "limit" in search_args: 
                if int(search_args["limit"]) > k:
                    k = int(search_args["limit"]) #otherwise will only get k results
            request["query"] = "{!knn " + f'f={field} topK={k}' + "}" + str(vector)
            request["params"]["defType"] = "lucene"
        
        for name, value in search_args.items():
            match name:
                case "q":
                    request.pop("query")
                    request["params"]["q"] = value
                case "query":
                    request["query"] = value if value else "*:*"
                case "rerank_query":
                    rq = "{" + f'!rerank reRankQuery=$rq_query reRankDocs={value["rerank_quantity"]} reRankWeight=1' + "}"
                    k = str(value.pop("k", 10))
                    request["params"]["rq"] = rq
                    request["params"]["rq_query"] = "{!knn f=" + value["query_fields"][0] + " topK=" + k + "}" + str(value["query"])
                case "quantization_size":
                    pass
                case "query_fields":
                    request["params"]["qf"] = value
                case "return_fields":
                    request["fields"] = value
                case "filters":
                    request["filter"] = []
                    for f in value:
                        filter_value = f'({" ".join(f[1])})' if isinstance(f[1], list) else f[1] 
                        request["filter"].append(f"{f[0]}:{filter_value}")
                case "limit":
                    request["limit"] = value
                case "order_by":
                    request["sort"] = ", ".join([f"{column} {sort}" for (column, sort) in value])  
                case "default_operator":
                    request["params"]["q.op"] = value
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
                    if "fields" not in request:
                        request["fields"] = []
                    request["fields"].append("[explain style=html]")
                case "hightlight":
                    request["params"]["hl"] = True
                case _:
                    request["params"][name] = value
        return request
    
    def transform_response(self, search_response):    
        response = {"docs": search_response["response"]["docs"]}
        if "highlighting" in search_response:
            response["highlighting"] = search_response["highlighting"]
        return response
        
    def native_search(self, request=None, data=None):
        return requests.post(f"{SOLR_URL}/{self.name}/select", json=request, data=data).json()
            
    def hybrid_search(self, lexical_search_args, vector_search_args, algorithm):
        pass

    def search_for_random_document(self, query):
        draw = random.random()
        request = {"query": query,
                   "limit": 1,
                   "order_by": [(f"random_{draw}", "DESC")]}
        return self.search(**request)
    
    def spell_check(self, query, log=False):
        request = {"query": query,
                   "params": {"q.op": "and", "indent": "on"}}
        if log:
            print("Solr spellcheck basic request syntax: ")
            print(json.dumps(request, indent="  "))
        response = requests.post(f"{SOLR_URL}/{self.name}/spell", json=request).json()
        return {r["collationQuery"]: r["hits"]
                for r in response["spellcheck"]["collations"]
                if r != "collation"}