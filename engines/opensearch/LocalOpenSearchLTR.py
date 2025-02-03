from engines.opensearch.OpenSearchLTR import OpenSearchLTR
from ltr.model_store import LocalModelStore
import json
import requests

class LocalOpenSearchLTR(OpenSearchLTR):
    #https://github.com/o19s/elasticsearch-ltr-demo/tree/master/train
    def __init__(self, collection):
        super().__init__(collection, LocalModelStore("common_models.cfg"))
    
    def enable_ltr(self, log=False):
        pass

    def generate_search_request(self, feature, keywords, doc_ids, id_field="upc"):
        return {"size": 100,
                "query": {"bool": {"filter": [{"terms": {id_field: doc_ids}}],
                                   "should": [feature["template"]]}}}
    
    def merge_feature_scores(self, msearch_response, features, id_field="upc"):
        feature_scores = {}
        responses = msearch_response.json()["responses"]
        for doc in responses[0]["hits"]["hits"]:
            feature_scores[doc[id_field]] = {}
            for i, feature in enumerate(features):
                score = responses[i]["hits"]["hits"][i]["_score"]
                feature_scores[doc[id_field]][feature["name"]] = score
        return feature_scores

    def get_logged_features(self, model_name, doc_ids, options={},
                            id_field="id", fields=None, log=False):
        keywords = options.get("keywords", "*")
        features = self.model_store.load_features_for_model(model_name, log=log)
        feature_queries = [self.generate_search_request(f, keywords, doc_ids, id_field)
                           for f in features]
        metadata = '{"index": "' + self.collection.name + '"}'
        request_body = "\n".join([f"{metadata}\n{json.dumps(q)}" for q in feature_queries])
        response = requests.post(f'{self.collection.opensearch_url}/_msearch', data=request_body)
        docs = [d["_source"] for d in response.json()["responses"][0]["hits"]["hits"]]
        feature_scores = self.merge_feature_scores(response, features)
        for doc in (docs):
            doc["[features]"] = feature_scores[doc[id_field]]
        return docs
    
    def generate_model(self, model_name, feature_names, means, std_devs, weights):
        linear_model = {"name": model_name,
                        "features": {}}
        for i, name in enumerate(feature_names):
            linear_model["features"][name] = {"avg": float(means[i]),
                                              "std": float(std_devs[i]),
                                              "weight": float(weights[i])}
        return linear_model

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
    