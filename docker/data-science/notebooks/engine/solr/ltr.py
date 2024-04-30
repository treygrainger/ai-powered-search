from aips.environment import SOLR_URL
import json
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
    
    def generate_fuzzy_query_feature(self, feature_name, store_name, field_name):
        return self.generate_query_feature(feature_name, store_name, f"{field_name}_ngram")
    
    def generate_bigram_query_feature(self, feature_name, store_name, field):
        query = "{" + f"!edismax qf={field} pf2={field}" +"}(${keywords})"
        return self.generate_feature(feature_name, store_name, {"q": query})
        
    def generate_field_value_feature(self, feature_name, store_name, field_name):
        query = "{!func}" + field_name
        return self.generate_feature(feature_name, store_name, {"q": query})
        
    def generate_field_length_feature(self, feature_name, store_name, field_name):
        params = {"field": field_name}
        return self.generate_feature(feature_name, store_name, params,
                                     feature_type="org.apache.solr.ltr.feature.FieldLengthFeature")
    
    def delete_feature_store(self, collection, name):
        return requests.delete(f"{SOLR_URL}/{collection.name}/schema/feature-store/{name}").json()

    def upload_features(self, collection, features):
        return requests.put(f"{SOLR_URL}/{collection.name}/schema/feature-store", json=features).json()

    def delete_model_store(self, collection, model_name):
        return requests.delete(f"{SOLR_URL}/{collection.name}/schema/model-store/{model_name}").json()
    
    def upload_model(self, collection, model):
        response = requests.put(f"{SOLR_URL}/{collection.name}/schema/model-store", json=model).json()
        requests.get(f"{SOLR_URL}/admin/collections?action=RELOAD&name={collection.name}&wt=xml")
        return response    
    
    def log_query(self, collection, featureset, doc_ids, options={}, id_field="id", fields=None, log=False):
        efi = " ".join(f'efi.{k}="{v}"' for k, v in options.items())
        if not fields:
            fields = [id_field]
        fields.append(f"[features store={featureset} {efi}]")
        request = {
            "query": f"id:({' '.join(doc_ids)})" if doc_ids else "*", 
            "return_fields": fields,
            "limit": 1000
        }
        if log:
            print("Search Request:")
            print(json.dumps(collection.transform_request(**request), indent="  "))
        resp = collection.search(**request)
        docs = resp["docs"]
        # Clean up features to consistent format
        for d in docs:
            features = list(map(lambda f : float(f.split("=")[1]), d["[features]"].split(",")))
            d["ltr_features"] = features

        return docs
    
    def generate_model(self, feature_store, model_name, feature_names, means, std_devs, weights):
        linear_model = {
            "store": feature_store,
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
    
    def generate_query(self, model_name, query, return_fields,
                       query_fields=["title", "overview"], rerank=None, log=False):
        request = {"return_fields": return_fields, "limit": 5}
        if log:
            request["log"] = True
        rerank_query = "{" + f'!ltr reRankDocs={rerank if rerank else 60000} model={model_name} efi.keywords="{query}"' + "}"
        if rerank:
            request["query"] = query
            request["rerank_query"] = rerank_query
            request["query_fields"] = query_fields
        else:
            request["query"] = rerank_query
            request["query_parser"] = "lucene"
        return request