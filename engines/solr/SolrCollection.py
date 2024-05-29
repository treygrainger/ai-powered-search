import random
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

    
    def search(self, **search_args):
        request = self.transform_lexical_request(**search_args)
        search_response = self.native_search(request=request)
        return self.transform_response(search_response)
    
    def vector_search(self, **search_args):
        request = self.transform_vector_request(**search_args)
        search_response = self.native_search(request=request)
        return self.transform_response(search_response)
    
    
    def transform_lexical_request(self, **search_args):
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
                        filter_value = f'({" ".join(f[1])})' if isinstance(f[1], list) else f[1] 
                        request["filter"].append(f"{f[0]}:{filter_value}")
                case "limit":
                    request["limit"] = value
                case "order_by":
                    request["sort"] = ", ".join([f"{column} {sort}" for (column, sort) in value])  
                case "rerank_query":
                    request["params"]["rq"] = value
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
        if "log" in search_args:
            print("Search Request:")
            print(json.dumps(request, indent="  "))
        return request
    

    def transform_vector_request(self, **search_args):
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
        if "log" in search_args:
            print("Vector Search Request:")
            print(json.dumps(request, indent="  "))
        return request
 
    def native_search(self, request=None, data=None):
        return requests.post(f"{SOLR_URL}/{self.name}/select", json=request, data=data).json()

    def transform_lexical_response(self, lexical_response):
        transform_response(self, lexical_response)

    def transform_vector_response(self, vector_response):
        transform_response(self, vector_response)
        
    def transform_response(self, search_response):    
        response = {"docs": search_response["response"]["docs"]}
        if "highlighting" in search_response:
            response["highlighting"] = search_response["highlighting"]
        return response
        

    def hybrid_search(self, lexical_search_args, vector_search_args, algorithm):
        #algorithm = {name: "rrf", k:60}
        hybrid_search_results = None
        match algorithm.get("name"):
            case "rrf":
                k = 60
                if algorithm.has("k"): k = algorithm.get("k")
                
                lexical_search_results = self.search(**lexical_search_args)
                vector_search_results = self.vector_search(**vector_search_args)
                hybrid_search_results = reciprocal_rank_fusion(lexical_search_results, vector_search_results, k)
            case "rerank_lexical_with_vector":
                pass
        return hybrid_search_results
    
    def reciprocal_rank_fusion(k, *query_results):
        # k is a ranking constant
        # q is a query in the set of queries
        # d is a document in the result set of q
        # result(q) is the result set of q
        # rank( result(q), d ) is d's rank within the result(q) starting from 1

        score = 0.0
        for result_set in query_results:
            if d in result(q):
                score += 1.0 / ( k + rank( result(q), d ) )
        return score

# where
# k is a ranking constant
# q is a query in the set of queries
# d is a document in the result set of q
# result(q) is the result set of q
# rank( result(q), d ) is d's rank within the result(q) starting from 1
    
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