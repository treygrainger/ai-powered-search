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
        is_vector_search = "query" in search_args and isinstance(search_args["query"], list)
        if is_vector_search:
            vector = search_args.pop("query")
            query_fields = search_args.pop("query_fields", [])
            if not isinstance(query_fields, list):
                raise TypeError("query_fields must be a list")
            elif len(query_fields) == 0:
                raise ValueError("You must specificy at least one field in query_fields")
            else: 
                field = query_fields[0]
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
                    rerank_count = value.pop("rerank_count", 500)
                    rq = "{" + f'!rerank reRankQuery=$rq_query reRankDocs={rerank_count} reRankWeight=1' + "}"
                    k = str(value.pop("k", 10))
                    query_field = value["query_fields"][0]
                    query = str(value["query"])
                    request["params"]["rq"] = rq
                    request["params"]["rq_query"] = "{!knn f=" + query_field + " topK=" + k + "}" + query
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

        #This de-normalizes to fix vector scores. 
        #Currently checks request for existance of {!knn, maybe theres a better indicator
        #  as this catches more complex queries. Currently any query using the knn parser
        #  will be de-normalized. Though it doesn't really matter to denormalize those more
        #  complex searches as the rank won't change and score is not considered.
        if ("params" in search_response["responseHeader"] and
            "json" in search_response["responseHeader"]["params"] and
            search_response["responseHeader"]["params"]["json"].find("{!knn") != -1):
            for doc in response["docs"]:
                doc["score"] = 2 * doc.get("score", 1) - 1                

        return response
        
    def native_search(self, request=None, data=None):
        response = requests.post(f"{SOLR_URL}/{self.name}/select", json=request, data=data).json()
        return response
    
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