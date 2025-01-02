from random import random
from engines.weaviate.WeaviateCollection import WeaviateCollection
from engines.LTR import LTR
import json
import requests
import pickle
from ltr.model_store import ModelStore

class WeaviateLTR(LTR):

    def __init__(self, collection):
        if not isinstance(collection, WeaviateCollection):
            raise TypeError("Only supports a WeaviateCollection")
        self.model_store = ModelStore("weaviate_ltr.cfg")
        super().__init__(collection)

    def enable_ltr(self, log=True):
        pass
    
    def generate_feature(self, feature_name, params, 
                         feature_type=""):
        return {
            "name": feature_name,
            "type": feature_type,
            "params": params
        }
        
    def generate_query_feature(self, feature_name, field_name, constant_score=False, value="$keywords"): 
        config = {"request": {"query": value, "query_fields": field_name}}
        if constant_score:
           config["constant_score"] = 1
        return self.generate_feature(feature_name, config, "query")
    
    def generate_fuzzy_query_feature(self, feature_name, field_name):
        #will use a field built with bigrams
        config = {"request": {"query": "$keywords", "query_fields": field_name}}
        return self.generate_feature(feature_name, config, "query")
    
    def generate_bigram_query_feature(self, feature_name, field_name):
        #use a bigram field packed with bigrams and searched with by grams
        config = {"request": {"query": "$keywords", "query_fields": field_name}}
        return self.generate_feature(feature_name, config, "query")
        
    def generate_field_value_feature(self, feature_name, field_name):
        config = {"request": {"query": "$keywords", "query_fields": field_name}}
        return self.generate_feature(feature_name, config, "query")
        
    def generate_field_length_feature(self, feature_name, field_name):
        pass

    def delete_feature_store(self, name, log=False):
        self.model_store.delete_feature_store(name, log)
    
    def upload_features(self, features, model_name, log=False):
        self.model_store.upload_features(features, model_name, log)
    
    def delete_model(self, model_name, log=False):
        self.model_store.delete_model(model_name, log)
    
    def upload_model(self, model, log=False):
        self.model_store.upload_model(model, log)
    
    def upsert_model(self, model, log=False):
        self.delete_model(model["name"], log=log)
        self.upload_model(model, log=log)

    def get_logged_features(self, model_name, doc_ids, options={},
                            id_field="upc", fields=None, log=False):
        model_features = self.model_store.load_features_for_model(model_name, log)
        if "keywords" not in options:
            raise Exception("keywords are required to log features")
        request = {"filters": [("upc", doc_ids)], "limit": 500}
        if log:
            request["log"] = True
        logged_docs = self.collection.search(**request)["docs"]
        for d in logged_docs: 
            d["[features]"] = {}

        for feature in model_features:
            match feature["type"]:
                case "query":
                    feature_request = request | feature["params"]["request"]
                    if feature_request["query"] == "$keywords":
                        feature_request["query"] = options["keywords"] #.replace("$keywords", )
                    scored_docs = self.collection.search(**feature_request)["docs"]
                    keyed_docs = {d[id_field]: d for d in scored_docs}
                    for d in logged_docs:
                        feature_score = keyed_docs.get(d[id_field], {"score": 0})["score"]
                        if feature_score != 0:
                            feature_score = feature.get("constant_score", feature_score)
                        d["[features]"][feature["name"]] = float(feature_score)
        return logged_docs
    
    def generate_model(self, model_name, feature_names, means, std_devs, weights):
        linear_model = {"name": model_name,
                        "features": {}}
        for i, name in enumerate(feature_names):
            linear_model["features"][name] = {"avg": float(means[i]),
                                              "std": float(std_devs[i]),
                                              "weight": float(weights[i])}    
        return linear_model
    
    def get_explore_candidate(self, query, explore_vector, feature_config, log=False):
        request = {"query": query or "*",
                   "limit": 1,
                   "return_fields": ["upc", "name", "manufacturer", "short_description", "long_description", "has_promotion"],
                   "order_by": [(f"random_{random()}", "DESC")]}
        if log: request["log"] = log
        docs = self.collection.search(**request)["docs"]
        if log and not docs:
            print(f"No exploration candidate matching query {query}")
        return docs    

    def search_with_model(self, model_name, **search_args):
        ltr_model_data = self.model_store.load_model(model_name)
        limit = search_args.get("limit", 25)
        search_args["limit"] =  limit * 10
        response = self.collection.search(**search_args)
        id_field = "upc" #collection.get_id
        keyed_docs = {d[id_field]: d for d in response["docs"]}
        query_options = {"keywords": search_args.get("query", "*")}
        logged_docs = self.get_logged_features(model_name, list(keyed_docs.keys()),
                                               query_options, id_field)
        for doc in logged_docs:
            doc["ltr_score"] = 0
            for name, values in ltr_model_data["features"].items():
                doc["ltr_score"] += ((doc["[features]"][name] - values["avg"])
                                      / values["std"]) * values["weight"]
        sorted_docs = sorted(logged_docs, key=lambda d: d["ltr_score"], reverse=True)
        docs = [keyed_docs[d[id_field]] for d in sorted_docs][:limit]
        response["docs"] = docs
        return response