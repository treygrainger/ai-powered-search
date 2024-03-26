from env import SOLR_URL
import requests

class SolrLTR:
    def __init__(self):
        pass    
    
    def generate_feature(self, feature_name, store_name, params, 
                         feature_type="org.apache.solr.ltr.feature.SolrFeature"):
        return {
            "name": feature_name,
            "store": store_name,
            "class": feature_type,
            "params": params
        }
        
    def generate_query_feature(self, feature_name, store_name, field_name, constant_score=False, value="(${keywords})"): 
        query = f"{field_name}:{value}"
        if constant_score:
            query += "^=1"
        return self.generate_feature(feature_name, store_name, {"q": query})
    
    def generate_advanced_query_feature(self, feature_name, store_name, field):
        query = "{" + f"!edismax qf={field} pf2={field}" +"}(${keywords})"
        return self.generate_feature(feature_name, store_name, {"q": query})
        
    def generate_field_value_feature(self, feature_name, store_name, field_name):
        query = "{!func}" + field_name
        return self.generate_feature(feature_name, store_name, {"q": query})
        
    def generate_field_length_feature(self, feature_name, store_name, field_name):
        params = {"field": field_name}
        return self.generate_feature(feature_name, store_name, params,
                                     feature_type="org.apache.solr.ltr.feature.FieldLengthFeature")
    
    def log_query(self, collection, featureset, ids, options={}, id_field="id", fields=None, log=False):
        efi = " ".join(f'efi.{k}="{v}"' for k, v in options.items())
        if not fields:
            fields = id_field
        params = {
            "fl": f"{fields},[features store={featureset} {efi}]",
            "q": "{{!terms f={}}}{}".format(id_field, ",".join(ids)) if ids else "*:*",
            "rows": 1000,
            "wt": "json"
        }
        if log:
            print("Search Request:")
            print(params)
        resp = collection.search(data=params)
        docs = resp["response"]["docs"]
        # Clean up features to consistent format
        for d in docs:
            features = list(map(lambda f : float(f.split("=")[1]), d["[features]"].split(",")))
            d["ltr_features"] = features

        return docs
    
    def generate_ltr_model(self, feature_store, model_name, feature_names, means, std_devs, weights):
        linear_model = {
            "store": "movies",
            "class": "org.apache.solr.ltr.model.LinearModel",
            "name": "movie_model",
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
    
    def basic_ltr_query(self, model_name, query, fields):
        return {
            "fields": fields,
            "limit": 5,
            "params": {
                "q": "{" + f'!ltr reRankDocs=60000 model={model_name} efi.keywords="{query}"' + "}"
            }
        }
        
    def ltr_query(self, model_name, query, fields, qf="title overview", rerank=500):
        return {
            "fields": fields,
            "limit": 5,
            "params": {
                "rq": "{" + f'!ltr reRankDocs={rerank} model={model_name} efi.keywords="{query}"' + "}",
                "qf": qf,
                "defType": "edismax",
                "q": "harry potter"
            }
        }
    