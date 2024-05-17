from aips.environment import SOLR_URL
from engines.solr.SolrCollection import SolrCollection
from engines.LTR import LTR
import json
import requests

class SolrLTR(LTR):
    def __init__(self, collection):
        if not isinstance(collection, SolrCollection):
            raise TypeError("Only supports a SolrCollection")
        super().__init__(collection)
    
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
    
    def generate_bigram_query_feature(self, feature_name, field):
        query = "{" + f"!edismax qf={field} pf2={field}" +"}(${keywords})"
        return self.generate_feature(feature_name, {"q": query})
        
    def generate_field_value_feature(self, feature_name, field_name):
        query = "{!func}" + field_name
        return self.generate_feature(feature_name, {"q": query})
        
    def generate_field_length_feature(self, feature_name, field_name):
        params = {"field": field_name}
        return self.generate_feature(feature_name, params,
                                     feature_type="org.apache.solr.ltr.feature.FieldLengthFeature")
    
    def delete_feature_store(self, name):
        return requests.delete(f"{SOLR_URL}/{self.collection.name}/schema/feature-store/{name}").json()

    def upload_features(self, features, model_name):
        for feature in features:
            feature["store"] = model_name
        return requests.put(f"{SOLR_URL}/{self.collection.name}/schema/feature-store", json=features).json()

    def delete_model(self, model_name):
        return requests.delete(f"{SOLR_URL}/{self.collection.name}/schema/model-store/{model_name}").json()
    
    def upload_model(self, model):
        response = requests.put(f"{SOLR_URL}/{self.collection.name}/schema/model-store", json=model).json()
        requests.get(f"{SOLR_URL}/admin/collections?action=RELOAD&name={self.collection.name}&wt=xml")
        return response    
    
    def get_logged_features(self, model_name, doc_ids, options={},
                            id_field="id", fields=None, log=False):
        efi = " ".join(f'efi.{k}="{v}"' for k, v in options.items())
        if not fields:
            fields = [id_field]
        fields.append(f"[features store={model_name} {efi}]")
        request = {
            "query": f"{id_field}:({' '.join(doc_ids)})" if doc_ids else "*", 
            "return_fields": fields,
            "limit": 1000
        }
        if log:
            print("Search Request:")
            print(json.dumps(self.collection.transform_request(**request), indent="  "))
        resp = self.collection.search(**request)
        docs = resp["docs"]
        # Clean up features to consistent format
        for d in docs:
            features = list(map(lambda f : float(f.split("=")[1]), d["[features]"].split(",")))
            d["ltr_features"] = features

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

    ["upc", "name", "manufacturer", "score"]

    def search_with_model(self, model_name, **search_args):
        parser_type = "edismax"
        log = "log" in search_args
        query = search_args["query"] if "query" in search_args else "*"
        limit = search_args["limit"] if "limit" in search_args else 10
        query_fields = search_args["query_fields"] if "query_fields" in search_args else "*"
        return_fields = search_args["return_fields"] if "return_fields" in search_args else "*"
        if "rerank" not in search_args:
            parser_type = "lucene"
            query = "{" + f'!ltr reRankDocs=60000 model={model_name} efi.keywords="{query}"' + "}"
        request = {
            "query": query,
            "limit": limit,
            "fields": return_fields,
            "params": {"defType": parser_type,
                       "qf": query_fields}
        }
        if "rerank" in search_args:
            fuzzy_kws = "~" + " ~".join(query.split())
            squeezed_kws = "".join(query.split())
            rerank_query = (
                "{!ltr reRankDocs=" + str(search_args["rerank"]) + " reRankWeight=10.0 model=" 
                    + model_name +" efi.fuzzy_keywords=\"" + fuzzy_kws +
                    "\" " + "efi.squeezed_keywords=\"" + squeezed_kws + "\" " +
                    "efi.keywords=\"" + query + "\"}")
            request["params"]["rq"] = rerank_query

        if log:
            print("search_with_model: search request:")
            print(request)

        resp = self.collection.native_search(request)
            
        if log:
            print("search_with_model: search response:")
            print(resp)
            
        search_results = resp["response"]["docs"]

        for rank, result in enumerate(search_results):
            result["rank"] = rank
            
        return {"docs": search_results}