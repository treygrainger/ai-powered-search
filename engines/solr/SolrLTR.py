from random import random
from re import search
from engines.solr.config import SOLR_URL
from engines.solr.SolrCollection import SolrCollection
from engines.LTR import LTR
import json
import requests

class SolrLTR(LTR):
    def __init__(self, collection):
        if not isinstance(collection, SolrCollection):
            raise TypeError("Only supports a SolrCollection")
        super().__init__(collection)
    
    def enable_ltr(self, log=True):
        delete_parser = {"delete-queryparser": "ltr"}
        response = requests.post(f"{SOLR_URL}/{self.collection.name}/config", json=delete_parser)

        delete_transformer = {"delete-transformer": "features"}
        response = requests.post(f"{SOLR_URL}/{self.collection.name}/config", json=delete_transformer)
        
        print(f"Adding LTR QParser for {self.collection.name} collection")
        add_parser = {
            "add-queryparser": {
                "name": "ltr",
                "class": "org.apache.solr.ltr.search.LTRQParserPlugin"
                }
            }
        response = requests.post(f"{SOLR_URL}/{self.collection.name}/config", json=add_parser)        

        print(f"Adding LTR Doc Transformer for {self.collection.name} collection")
        add_transformer =  {
            "add-transformer": {
                "name": "features",
                "class": "org.apache.solr.ltr.response.transform.LTRFeatureLoggerTransformerFactory",
                "fvCacheName": "QUERY_DOC_FV"
                }
            }
        response = requests.post(f"{SOLR_URL}/{self.collection.name}/config", json=add_transformer)
    
    def generate_feature(self, feature_name, params, 
                         feature_type="org.apache.solr.ltr.feature.SolrFeature"):
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
        query = "{" + f"!edismax qf={field_name} pf2={field_name}" +"}(${keywords})"
        return self.generate_feature(feature_name, {"q": query})
        
    def generate_field_value_feature(self, feature_name, field_name):
        query = "{!func}" + field_name
        return self.generate_feature(feature_name, {"q": query})
        
    def generate_field_length_feature(self, feature_name, field_name):
        params = {"field": field_name}
        return self.generate_feature(feature_name, params,
                                     feature_type="org.apache.solr.ltr.feature.FieldLengthFeature")
    
    def delete_feature_store(self, name, log=False):
        return requests.delete(f"{SOLR_URL}/{self.collection.name}/schema/feature-store/{name}").json()

    def upload_features(self, features, model_name, log=False):
        if log: print(f"Uploading {model_name} features to {self.collection.name} collection.")        
        for feature in features:
            feature["store"] = model_name
        response = requests.put(f"{SOLR_URL}/{self.collection.name}/schema/feature-store", json=features).json()
        if log: print(json.dumps(response, indent=2))
        requests.get(f"{SOLR_URL}/admin/collections?action=RELOAD&name={self.collection.name}&wt=xml")
        return response

    def delete_model(self, model_name, log=False):
        if log: print(f"Delete model {model_name}")
        response = requests.delete(f"{SOLR_URL}/{self.collection.name}/schema/model-store/{model_name}").json()
        if log: print(json.dumps(response, indent=2))
        return response
    
    def upload_model(self, model, log=False):
        if log: print(f'Uploading model {model["name"]}')
        response = requests.put(f"{SOLR_URL}/{self.collection.name}/schema/model-store", json=model).json()
        if log: print(json.dumps(response, indent=2))
        requests.get(f"{SOLR_URL}/admin/collections?action=RELOAD&name={self.collection.name}&wt=xml")
        return response    
    
    def upsert_model(self, model, log=False):
        self.delete_model(model["name"], log=log)
        self.upload_model(model, log=log)

    def get_logged_features(self, model_name, doc_ids, options={},
                            id_field="id", fields=None, log=False):
        efi = " ".join(f'efi.{k}="{v}"' for k, v in options.items())
        if not fields:
            fields = [id_field]
        fields.append(f"[features store={model_name} {efi}]")
        request = {"query": f"{id_field}:({' '.join(doc_ids)})" if doc_ids else "*", 
                   "return_fields": fields,
                   "limit": 1000}
        if log:
            request["log"] = True
        docs = self.collection.search(**request)["docs"]
        # Clean up features to consistent format
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
        query = ""
        for feature_name, config in feature_config.items():
            if feature_name in explore_vector:
                if explore_vector[feature_name] == 1.0:
                    query += f' +{config["field"]}:({config["value"]})'
                elif explore_vector[feature_name] == -1.0:
                    query += f' -{config["field"]}:({config["value"]})'
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
        parser_type = "edismax"
        log = search_args.get("log", False)
        rerank_count = search_args.get("rerank_count", 9999999)
        return_fields = search_args.get("return_fields", ["upc", "name", "manufacturer",
                                                          "short_description", "long_description"])         
        
        rerank_query = search_args.get("rerank_query", None)
        query = search_args.get("query", None)
        if query:
            if not rerank_query:
                parser_type = "lucene"   
                query = "{" + f'!ltr reRankDocs={rerank_count} model={model_name} efi.keywords="{query}"' + "}"
        else:
            value = rerank_query if rerank_query else query
            query = "{" + f'!ltr reRankDocs={rerank_count} model={model_name} efi.keywords="{value}"' + "}"
            if rerank_query:                
                rerank_query = None

        request = {
            "query": query,
            "limit": search_args.get("limit", 5),
            "fields": return_fields,
            "params": {"defType": parser_type}
        }
        if "query_fields" in search_args:
            request["params"]["qf"] = search_args["query_fields"]
        if rerank_query:
            rq = "{" + f'!ltr reRankDocs={rerank_count} model={model_name} efi.keywords="{rerank_query}"' + "}"
            request["params"]["rq"] = rq

        if log: print(f"search_with_model() request: {request}")
        response = self.collection.native_search(request)            
        if log: print(f"search_with_model() response: {response}")

        docs = response["response"]["docs"]    
        return {"docs": docs}
