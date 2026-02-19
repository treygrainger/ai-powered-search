import random
from aips.search_request_functions import generate_fuzzy_text
from engines.vespa.VespaCollection import VespaCollection
from engines.LTR import LTR
from ltr.model_store import ModelStore

class VespaLTR(LTR):

    def __init__(self, collection):
        if not isinstance(collection, VespaCollection):
            raise TypeError("Only supports a VespaCollection")
        self.model_store = ModelStore("vespa_ltr.cfg")
        super().__init__(collection)

    def enable_ltr(self, log=True):
        pass
    
    def generate_feature(self, feature_name, params, feature_type=""):
        return {"name": feature_name, "type": feature_type, "params": params}
        
    def generate_query_feature(self, feature_name, field_name, constant_score=False, value="$keywords"): 
        config = {"request": {"query": value, "query_fields": field_name}}
        if constant_score:
           config["constant_score"] = 1
        return self.generate_feature(feature_name, config, "query")
    
    def generate_fuzzy_query_feature(self, feature_name, field_name):
        #will use a field built with bigrams
        config = {"request": {"query": "$keywords", "query_fields": field_name}}
        return self.generate_feature(feature_name, config, "fuzzy_query")
    
    def generate_bigram_query_feature(self, feature_name, field_name):
        #use a bigram field packed with bigrams and searched with by grams
        config = {"request": {"query": "$keywords", "query_fields": field_name}}
        return self.generate_feature(feature_name, config, "bigram_query")
        
    def generate_field_value_feature(self, feature_name, field_name):
        return self.generate_feature(feature_name, {"field_name": field_name}, "field_value")

    def delete_feature_store(self, name, log=False):
        self.model_store.delete_feature_store(name, log)
    
    def get_features(self, model_name, log=False):
        return self.model_store.load_features_for_model(model_name, log)
    
    def upload_features(self, features, model_name, log=False):
        return self.model_store.upload_features(features, model_name, log)
    
    def delete_model(self, model_name, log=False):
        return self.model_store.delete_model(model_name, log)
    
    def upload_model(self, model, log=False):
        return self.model_store.upload_model(model, log)
    
    def upsert_model(self, model, log=False):
        self.delete_model(model["name"], log=log)
        self.upload_model(model, log=log)
    
    def get_explore_candidate(self, query, explore_vector, feature_config, log=False):
        query = ""
        for feature_name, config in feature_config.items():
            if feature_name in explore_vector:
                if explore_vector[feature_name] == 1.0:
                    query += f' {config["field"]}:({config["value"]})'
                elif explore_vector[feature_name] == -1.0:
                    query += f' -{config["field"]}:({config["value"]})'
        request = {"query": query or "*",
                   "limit": 100,
                   "return_fields": ["upc", "name", "manufacturer", "short_description", "long_description", "has_promotion"]}
        if log: request["log"] = log
        docs = self.collection.search(**request)["docs"]
        return [random.choice(docs)]
    
    def generate_model(self, model_name, feature_names, means, std_devs, weights):
        linear_model = {"name": model_name,
                        "features": {}}
        for i, name in enumerate(feature_names):
            linear_model["features"][name] = {"avg": float(means[i]),
                                              "std": float(std_devs[i]),
                                              "weight": float(weights[i])}
        return linear_model
    
    def get_logged_features(self, model_name, doc_ids, options={},
                            id_field="id", fields="*", log=False):
        feature_keys = {"title_bm25": "bm25(title)",
                        "overview_bm25": "bm25(overview)",
                        "release_year": "attributeMatch(release_year)",
                        "short_description_bm25": "bm25(short_description)",
                        "short_description_constant": "fieldMatch(short_description)",
                        "short_description_match": "fieldMatch(short_description)",
                        "long_description_bm25": "bm25(long_description)",
                        "long_description_match": "fieldMatch(long_description)",
                        "manufacturer_match": "fieldMatch(manufacturer_match)",
                        "has_promotion": "fieldMatch(has_promotion)",
                        "name_match": "fieldMatch(name)",
                        "name_fuzzy": "fieldMatch(name_fuzzy)",
                        "name_bigram": "fieldMatch(name_bigram)",
                        "name_bm25": "bm25(name)",
                        "name_constant": "fieldMatch(name)",
                        "short_description_bigram": "fieldMatch(short_description_bigram)"}
        model_features = self.model_store.load_features_for_model(model_name, log)
        if "keywords" not in options:
            raise Exception("keywords are required to log features")
        
        id_clause = " ".join(f"{id_field}:{id}" for id in doc_ids)
        additional_queries = []
        if "products" in self.collection.name:
            additional_queries = [f'name_fuzzy:"{options["keywords"]}"',
                                  f'name_bigram:"{options["keywords"]}"',
                                  f'short_description_bigram:"{options["keywords"]}"',
                                  "has_promotion:true"]
        request = {"query": [id_clause, options["keywords"]] + additional_queries,
                   "return_fields": fields,
                   "limit": 400,
                   "ranking_profile": "with_features"} #400 is maximum returnable docs in Vespa}
        if log:
            request["log"] = True

        logged_docs = self.collection.search(**request)["docs"]

        for d in logged_docs: 
            d["[features]"] = {}
            for feature in model_features:
                d["[features]"][feature["name"]] = d["summaryfeatures"][feature_keys[feature["name"]]]
            d.pop("summaryfeatures", None)
              
        return logged_docs

    def search_with_model(self, model_name, **search_args):
        id_field = "upc" if "products" in self.collection.name else "id"
        ltr_model_data = self.model_store.load_model(model_name)
        search_args["limit"] = 400
        response = self.collection.search(**search_args)
        keyed_docs = {str(d[id_field]): d for d in response["docs"]}
        query_options = {"keywords": search_args.get("query", "*")}
        logged_docs = self.get_logged_features(model_name, list(keyed_docs.keys()),
                                               options=query_options, id_field=id_field)
        for doc in logged_docs:
            doc["ltr_score"] = 0
            for name, values in ltr_model_data["features"].items():
                doc["ltr_score"] += ((doc["[features]"][name] - values["avg"])
                                      / values["std"]) * values["weight"]
        sorted_docs = sorted(logged_docs, key=lambda d: d["ltr_score"], reverse=True)
        docs = [keyed_docs[d[id_field]] for d in sorted_docs][:search_args.get("limit", 25)]
        response["docs"] = docs
        return response