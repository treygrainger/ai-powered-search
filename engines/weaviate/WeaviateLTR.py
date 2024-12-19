from random import random
from engines.weaviate.config import SOLR_URL
from engines.weaviate.WeaviateCollection import WeaviateCollection
from engines.LTR import LTR
import json
import requests

class WeaviateLTR(LTR):
    def __init__(self, collection):
        if not isinstance(collection, WeaviateCollection):
            raise TypeError("Only supports a WeaviateCollection")
        super().__init__(collection)
    
    def enable_ltr(self, log=True):
        pass
    
    def generate_feature(self, feature_name, params, 
                         feature_type=""):
        return {
            "name": feature_name,
            "class": feature_type,
            "params": params
        }
        
    def generate_query_feature(self, feature_name, field_name, constant_score=False, value="(${keywords})"): 
        query = f"{field_name}:{value}"
        if constant_score:
            query += "^=1"
        return self.generate_feature(feature_name, {"q": query})
    
    def generate_fuzzy_query_feature(self, feature_name, field_name):
        return self.generate_query_feature(feature_name, f"{field_name}_ngram")
    
    def generate_bigram_query_feature(self, feature_name, field_name):
        query = ""
        return self.generate_feature(feature_name, {"q": query})
        
    def generate_field_value_feature(self, feature_name, field_name):
        query = ""
        return self.generate_feature(feature_name, {"q": query})
        
    def generate_field_length_feature(self, feature_name, field_name):
        pass

    def delete_feature_store(self, name, log=False):
        pass
    
    def upload_features(self, features, model_name, log=False):
        pass
    
    def delete_model(self, model_name, log=False):
        pass
    
    def upload_model(self, model, log=False):
        pass
    
    def upsert_model(self, model, log=False):
        self.delete_model(model["name"], log=log)
        self.upload_model(model, log=log)

    def get_logged_features(self, model_name, doc_ids, options={},
                            id_field="id", fields=None, log=False):
        pass
        request = {}
        if log:
            request["log"] = True
        docs = self.collection.search(**request)["docs"]
        for d in docs:
            d["[features]"] = {f.split("=")[0] : float(f.split("=")[1])
                               for f in d["[features]"].split(",")}
        return docs
    
    def generate_model(self, model_name, feature_names, means, std_devs, weights):
        linear_model = {
            "store": model_name,
            "class": "org.apache.solr.ltr.model.LinearModel",
            "name": model_name,
            "features": [],
            "params": { "weights": {} }
        }        
        for i, name in enumerate(feature_names):
            config = {
                "name": name,
                "norm": {
                    "class": "org.apache.solr.ltr.norm.StandardNormalizer",
                    "params": {
                        "avg": str(means[i]),
                        "std": str(std_devs[i])
                    }
                }
            }
            linear_model["features"].append(config)
            linear_model["params"]["weights"][name] =  weights[i]         
        return linear_model
    
    def get_explore_candidate(self, query, explore_vector, feature_config, log=False):
        pass 

    def search_with_model(self, model_name, **search_args):
        pass 