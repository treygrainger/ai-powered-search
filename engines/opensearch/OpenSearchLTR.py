from random import random
from re import search
from engines.opensearch.config import OPENSEARCH_URL
from engines.opensearch.OpenSearchCollection import OpenSearchCollection
from engines.LTR import LTR
import json
import requests
from IPython.display import display

#https://github.com/o19s/elasticsearch-ltr-demo/tree/master/train

class OpenSearchLTR(LTR):
    def __init__(self, collection):
        if not isinstance(collection, OpenSearchCollection):
            raise TypeError("Only supports a OpenSearchCollection")
        super().__init__(collection)
    
    def enable_ltr(self, log=False):        
        ltr_info = requests.get(f"{OPENSEARCH_URL}/_ltr").json()
        if len(ltr_info.get("stores", {})) > 0:
            if log: display("LTR already initialized")
        else:
            if log: display("Enabling/Refreshing LTR store")
            response = requests.delete(f"{OPENSEARCH_URL}/_ltr")
            if log: display(response.json())
            response = requests.put(f"{OPENSEARCH_URL}/_ltr")
            if log: display(response.json())

    def generate_feature(self, feature_name, template, 
                         params=["keywords"]):
        return {"name": feature_name,
                "template": template,
                "params": params}
        
    def generate_query_feature(self, feature_name, field_name, constant_score=False, value="{{keywords}}"): 
        match_clause = {"match": {field_name: value}}   
        if constant_score:
            match_clause = {"constant_score": {"filter": match_clause,
                                               "boost": 1.0}}
        return self.generate_feature(feature_name, match_clause)
        
    def generate_field_value_feature(self, feature_name, field_name):
        feature = {"function_score": {
            "functions": [
                {"filter": {"bool": {"must": [{"exists": {"field": field_name}}]}},
                 "field_value_factor": {"field": field_name, "missing": 0}}]}}
        return self.generate_feature(feature_name, feature)
    
    def generate_fuzzy_query_feature(self, feature_name, field_name):
        query_clause = {"match_phrase": {f"{field_name}_fuzzy": {"query": "{{keywords}}", "slop": 3}}}
        return self.generate_feature(feature_name, query_clause)
    
    def generate_bigram_query_feature(self, feature_name, field_name):
        query_clause = {"match": {f"{field_name}_ngram": {"query": "{{keywords}}",
                                                          "analyzer": "standard"}}}
        return self.generate_feature(feature_name, query_clause)
        
    def generate_field_length_feature(self, feature_name, field_name):
        #Unused except in Ammendum
        return None
    
    def delete_feature_store(self, name, log=False):
        if log: display(f"Deleting features {name}")     
        response = requests.delete(f"{OPENSEARCH_URL}/_ltr/_featureset/{name}").json()
        if log: display(response)
        return response

    def upload_features(self, features, model_name, log=False):
        if log: display(f"Uploading Features {model_name}")
        feature_request = {"featureset": {"name": model_name,
                                          "features": features}}
        response = requests.post(f"{OPENSEARCH_URL}/_ltr/_featureset/{model_name}",
                                 json=feature_request)
        if log: display(response.json())
        return response

    def delete_model(self, model_name, log=False):
        if log: display(f"Delete model {model_name}")
        response = requests.delete(f"{OPENSEARCH_URL}/_ltr/_model/{model_name}").json()
        if log: display(json.dumps(response, indent=2))
        return response
    
    def upload_model(self, model, log=False):
        model_name = model["model"]["name"]
        if log: display(f'Upload model {model_name}')
        response = requests.post(f"{OPENSEARCH_URL}/_ltr/_featureset/{model_name}/_createmodel", json=model)
        if log: display(response.json())
        return response
    
    def upsert_model(self, model, log=False):
        self.delete_model(model["name"], log=log)
        self.upload_model(model, log=log)

    def get_logged_features(self, model_name, doc_ids, options={},
                            id_field="id", fields=None, log=False):
        keywords = options.get("keywords", "*")
        named_query = "logged_featureset"
        request = {"size": 100,
                   "query": {"bool": {"must": [{"terms": {id_field: doc_ids}}],
                                      "should": [{"sltr": {
                                                  "_name": named_query,
                                                  "featureset": f"{model_name}",
                                                  "params": {"keywords": keywords}}}]}},
                   "ext": {"ltr_log": {"log_specs": {"name": "main",
                                                     "named_query": named_query,
                                                     "missing_as_zero": True}}}}
        if log:
            display(request)
        response = self.collection.native_search(request=request)
        if log:
            display(response)
        documents = []
        for doc in response["hits"]["hits"]:
            transformed_doc = doc["_source"] | {"score": doc["_score"]}
            features = {feature["name"]: feature["value"]
                        for feature in doc['fields']['_ltrlog'][0]['main']}
            transformed_doc["[features]"] = features
            documents.append(transformed_doc)
        return documents
    
    def generate_model(self, model_name, feature_names, means, std_devs, weights):
        model_definition = {"type": "model/linear",
                            "feature_normalizers": {},
                            "definition": {}}
        for i, name in enumerate(feature_names):
            feature_definition = {"standard": {"mean": means[i],
                                  "standard_deviation": std_devs[i]}}
            model_definition["feature_normalizers"][name] = feature_definition
            model_definition["definition"][name] = weights[i]
        linear_model = {"model": {"name": model_name,
                                  "model": model_definition}}
        return linear_model
    
    def get_explore_candidate(self, query, explore_vector, feature_config, log=False):        
        query = ""
        for feature_name, config in feature_config.items():
            if feature_name in explore_vector:
                if explore_vector[feature_name] == 1.0:
                    query += f' +{config["field"]}:({config["value"]})'
                elif explore_vector[feature_name] == -1.0:
                    query += f' -{config["field"]}:({config["value"]})'
        request = {
            "query": {
                "function_score" : {
                    "query": {"query_string" : {"query": query}},
                    "random_score" : {}
                }
            },
            "fields": ["upc", "name", "manufacturer", "short_description",
                       "long_description", "has_promotion"],
            "size": 1
        }
        if log:
            display(request)
        response = self.collection.native_search(request)
        if log:
            display(response)
        candidate_docs = response["hits"]["hits"]
        if log and not candidate_docs:
            display(f"No exploration candidate matching query {query}")
        return [candidate_docs[0]["_source"] | {"score": candidate_docs[0]["_score"]}] 

    def search_with_model(self, model_name, **search_args):
        log = search_args.get("log", False)
        rerank_count = search_args.get("rerank_count", 10000) #10k is max
        return_fields = search_args.get("return_fields", ["upc", "name", "manufacturer",
                                                          "short_description", "long_description"])
        rerank_query = search_args.get("rerank_query", None)
        query = search_args.get("query", None)
        if rerank_query:
            request = {
                "query": {"multi_match": {"query": query}},
                "rescore": {"query": {"rescore_query" : {
                    "sltr": {"params": {"keywords": rerank_query},
                             "model": model_name}}},
                    "window_size": rerank_count},
                "size": 10,
                "fields": return_fields}
            if "query_fields" in search_args:
                request["query"]["multi_match"]["fields"] = search_args["query_fields"]
        else:
            request = {"query": {"sltr": {"params": {"keywords": query},
                                          "model": model_name}},
                       "size": 10,
                       "fields": return_fields}

        if log: display(f"search_with_model() request: {request}")
        response = self.collection.native_search(request)            
        if log: display(f"search_with_model() response: {response}")

        documents = []
        for doc in response["hits"]["hits"]:
            transformed_doc = doc["_source"] | {"score": doc["_score"]}
            documents.append(transformed_doc)
        return {"docs": documents}