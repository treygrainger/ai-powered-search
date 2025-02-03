from abc import ABC, abstractmethod
from engines.LTR import LTR
from ltr.model_store import LocalModelStore

class LocalLTR(LTR):
    def __init__(self, collection,
                 model_store=LocalModelStore("common_models.cfg"),
                 ):
        self.model_store = model_store
        super().__init__(collection)
    
    @abstractmethod
    def enable_ltr(self, log=False):
        "Initializes LTR dependencies for a given collection"
        pass

    @abstractmethod
    def generate_feature(self, feature_name, params, feature_type):
        "Generates an LTR feature definition."
        pass    
        
    @abstractmethod
    def generate_query_feature(self, feature_name, field_name, constant_score=False, value="(${keywords})"): 
        "Generates an LTR query feature definition."
        pass
    
    @abstractmethod
    def generate_fuzzy_query_feature(self, feature_name, field_name):
        "Generates an LTR fuzzy query feature definition."
        pass
    
    @abstractmethod
    def generate_bigram_query_feature(self, feature_name, field_name):
        "Generates an LTR bigram query feature definition."
        pass
    
    @abstractmethod
    def generate_field_value_feature(self, feature_name, field_name):
        "Generates an LTR field value feature definition."
        pass
    
    @abstractmethod
    def generate_field_length_feature(self, feature_name, field_name):
        "Generates an LTR field length feature definition."
        pass
    
    def generate_model(self, model_name, feature_names, means, std_devs, weights):
        linear_model = {"name": model_name,
                        "features": {}}
        for i, name in enumerate(feature_names):
            linear_model["features"][name] = {"avg": float(means[i]),
                                              "std": float(std_devs[i]),
                                              "weight": float(weights[i])}
        return linear_model

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

    @abstractmethod
    def get_explore_candidate(self, query, explore_vector, feature_config, log=False):
        "Generates a exploration search request with the given criteria."
        pass

    @abstractmethod
    def get_logged_features(self, model_name, doc_ids, options={},
                            id_field="id", fields=None, log=False):
        "Deletes the model from the engine."
        pass

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
