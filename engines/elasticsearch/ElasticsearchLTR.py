from random import random
from re import search
from engines.elasticsearch.config import ES_URL
from engines.elasticsearch.ElasticsearchCollection import ElasticsearchCollection
from engines.LTR import LTR
import requests
from IPython.display import display


class ElasticsearchLTR(LTR):
    def __init__(self, collection):
        if not isinstance(collection, ElasticsearchCollection):
            raise TypeError("Only supports an ElasticsearchCollection")
        super().__init__(collection)
        # Store feature sets and models in memory since we're not using the LTR plugin
        self.feature_sets = {}
        self.models = {}

    def enable_ltr(self, log=False):
        display("Enabling LTR using native Elasticsearch function_score capabilities")
        # No plugin needed, just verify Elasticsearch is available
        try:
            response = requests.get(f"{ES_URL}/_cluster/health")
            if response.status_code == 200:
                if log:
                    display("Elasticsearch is available. Native LTR functionality ready.")
                return True
            else:
                display(
                    f"Warning: Elasticsearch is not available. Status code: {response.status_code}"
                )
                return False
        except Exception as e:
            display(f"Error connecting to Elasticsearch: {e}")
            return False

    def generate_feature(self, feature_name, template, params=["keywords"]):
        return {"name": feature_name, "template": template, "params": params}

    def generate_query_feature(
        self, feature_name, field_name, constant_score=False, value="{{keywords}}"
    ):
        match_clause = {"match": {field_name: value}}
        if constant_score:
            match_clause = {"constant_score": {"filter": match_clause, "boost": 1.0}}
        return self.generate_feature(feature_name, match_clause)

    def generate_field_value_feature(self, feature_name, field_name):
        feature = {
            "function_score": {
                "functions": [
                    {
                        "filter": {"bool": {"must": [{"exists": {"field": field_name}}]}},
                        "field_value_factor": {"field": field_name, "missing": 0},
                    }
                ]
            }
        }
        return self.generate_feature(feature_name, feature)

    def generate_fuzzy_query_feature(self, feature_name, field_name):
        query_clause = {
            "match_phrase": {f"{field_name}_fuzzy": {"query": "{{keywords}}", "slop": 3}}
        }
        return self.generate_feature(feature_name, query_clause)

    def generate_bigram_query_feature(self, feature_name, field_name):
        query_clause = {
            "match": {f"{field_name}_ngram": {"query": "{{keywords}}", "analyzer": "standard"}}
        }
        return self.generate_feature(feature_name, query_clause)

    def generate_field_length_feature(self, feature_name, field_name):
        # Elasticsearch doesn't have a direct field length feature like OpenSearch
        # We can use a script to calculate field length
        script = {
            "script_score": {
                "script": {
                    "source": f"doc['{field_name}'].size() > 0 ? doc['{field_name}'].value.length() : 0",
                    "lang": "painless",
                }
            }
        }
        return self.generate_feature(feature_name, script)

    def delete_feature_store(self, name, log=False):
        if log:
            display(f"Deleting features {name}")
        if name in self.feature_sets:
            del self.feature_sets[name]
            if log:
                display(f"Feature set {name} deleted from memory")
            return {"acknowledged": True}
        return {"acknowledged": False, "error": "Feature set not found"}

    def upload_features(self, features, model_name, log=False):
        if log:
            display(f"Uploading Features {model_name}")
        self.feature_sets[model_name] = features
        if log:
            display(f"Feature set {model_name} stored in memory with {len(features)} features")
        return {"acknowledged": True}

    def delete_model(self, model_name, log=False):
        if log:
            display(f"Delete model {model_name}")
        if model_name in self.models:
            del self.models[model_name]
            if log:
                display(f"Model {model_name} deleted from memory")
            return {"acknowledged": True}
        return {"acknowledged": False, "error": "Model not found"}

    def upload_model(self, model, log=False):
        model_name = model["model"]["name"]
        if log:
            display(f"Upload model {model_name}")
        self.models[model_name] = model
        if log:
            display(f"Model {model_name} stored in memory")
        return {"acknowledged": True}

    def upsert_model(self, model, log=False):
        model_name = model["model"]["name"]
        self.delete_model(model_name, log=log)
        self.upload_model(model, log=log)

    def get_logged_features(
        self, model_name, doc_ids, options={}, id_field="id", fields=None, log=False
    ):  # fields parameter is required by the LTR interface but not used in this implementation
        keywords = options.get("keywords", "*")

        # Get the feature set for this model
        if model_name not in self.feature_sets:
            if log:
                display(f"Feature set {model_name} not found")
            return []

        features = self.feature_sets[model_name]

        # Create a query to fetch the documents
        request = {"size": 100, "query": {"terms": {id_field: doc_ids}}}

        if log:
            display(request)
        response = self.collection.native_search(request=request)
        if log:
            display(response)

        documents = []
        for doc in response["hits"]["hits"]:
            transformed_doc = doc["_source"] | {"score": doc["_score"]}

            # Calculate feature values for this document
            feature_values = {}
            for feature in features:
                # For demonstration, we'll compute a simple score based on the feature type
                # In a real implementation, you would compute actual feature values
                feature_name = feature["name"]
                template = feature["template"]

                # Calculate a feature value based on the template type
                if "match" in template:
                    field = list(template["match"].keys())[0]
                    if field in doc["_source"]:
                        # Simple TF-based score for match queries
                        value = (
                            1.0 if keywords.lower() in str(doc["_source"][field]).lower() else 0.0
                        )
                    else:
                        value = 0.0
                elif "function_score" in template:
                    # For field_value_factor features
                    if "functions" in template["function_score"]:
                        for function in template["function_score"]["functions"]:
                            if "field_value_factor" in function:
                                field = function["field_value_factor"]["field"]
                                if field in doc["_source"]:
                                    try:
                                        value = float(doc["_source"][field])
                                    except (ValueError, TypeError):
                                        value = 0.0
                                else:
                                    value = 0.0
                    else:
                        value = 0.0
                else:
                    # Default value for other feature types
                    value = 0.5

                feature_values[feature_name] = value

            transformed_doc["[features]"] = feature_values
            documents.append(transformed_doc)

        return documents

    def generate_model(self, model_name, feature_names, means, std_devs, weights):
        model_definition = {"type": "model/linear", "feature_normalizers": {}, "definition": {}}
        for i, name in enumerate(feature_names):
            feature_definition = {"standard": {"mean": means[i], "standard_deviation": std_devs[i]}}
            model_definition["feature_normalizers"][name] = feature_definition
            model_definition["definition"][name] = weights[i]
        linear_model = {"model": {"name": model_name, "model": model_definition}}
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
                "function_score": {"query": {"query_string": {"query": query}}, "random_score": {}}
            },
            "fields": [
                "upc",
                "name",
                "manufacturer",
                "short_description",
                "long_description",
                "has_promotion",
            ],
            "size": 1,
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
        rerank_count = search_args.get("rerank_count", 10000)  # 10k is max
        # return_fields is required by the LTR interface but not used in this implementation
        _ = search_args.get(
            "return_fields",
            ["upc", "name", "manufacturer", "short_description", "long_description"],
        )
        rerank_query = search_args.get("rerank_query", None)
        query = search_args.get("query", None)

        # Check if model exists
        if model_name not in self.models:
            if log:
                display(f"Model {model_name} not found")
            return {"docs": []}

        model = self.models[model_name]
        model_definition = model["model"]["model"]

        if rerank_query:
            # First perform a regular search
            base_request = {
                "query": {"multi_match": {"query": query}},
                "size": rerank_count,  # Get enough documents to rerank
            }

            if "query_fields" in search_args:
                base_request["query"]["multi_match"]["fields"] = search_args["query_fields"]

            if log:
                display(f"Base search request: {base_request}")

            base_response = self.collection.native_search(base_request)

            # Now rerank the results using our model
            docs = []
            for doc in base_response["hits"]["hits"][:rerank_count]:
                # Calculate the score using our model
                doc_score = 0.0

                # Get feature values for this document
                feature_values = {}
                for feature_name, weight in model_definition["definition"].items():
                    # For simplicity, we'll use a basic scoring approach
                    # In a real implementation, you would compute actual feature values
                    if feature_name.startswith("match_"):
                        field = feature_name.replace("match_", "")
                        if field in doc["_source"]:
                            value = (
                                1.0
                                if rerank_query.lower() in str(doc["_source"][field]).lower()
                                else 0.0
                            )
                        else:
                            value = 0.0
                    elif feature_name.startswith("value_"):
                        field = feature_name.replace("value_", "")
                        if field in doc["_source"]:
                            try:
                                value = float(doc["_source"][field])
                            except (ValueError, TypeError):
                                value = 0.0
                        else:
                            value = 0.0
                    else:
                        value = 0.5

                    # Apply normalization if available
                    if feature_name in model_definition["feature_normalizers"]:
                        normalizer = model_definition["feature_normalizers"][feature_name]
                        if "standard" in normalizer:
                            mean = normalizer["standard"]["mean"]
                            std_dev = normalizer["standard"]["standard_deviation"]
                            if std_dev > 0:
                                value = (value - mean) / std_dev

                    # Apply weight and add to score
                    doc_score += value * weight
                    feature_values[feature_name] = value

                transformed_doc = doc["_source"] | {"score": doc_score}
                docs.append(transformed_doc)

            # Sort by our calculated score
            docs.sort(key=lambda x: x["score"], reverse=True)

            # Return top 10 results
            return {"docs": docs[:10]}
        else:
            # Direct scoring with the model
            # Create a function_score query that applies our model
            script = {"source": "", "params": {"keywords": query, "weights": {}}}

            # Build the script that applies the model weights
            script_parts = []
            for feature_name, weight in model_definition["definition"].items():
                # Add feature calculation to script
                if feature_name.startswith("match_"):
                    field = feature_name.replace("match_", "")
                    script_parts.append(
                        f"float {feature_name} = doc.containsKey('{field}') && doc['{field}'].size() > 0 ? (doc['{field}'].value.toLowerCase().contains(params.keywords.toLowerCase()) ? 1.0 : 0.0) : 0.0;"
                    )
                elif feature_name.startswith("value_"):
                    field = feature_name.replace("value_", "")
                    script_parts.append(
                        f"float {feature_name} = doc.containsKey('{field}') && doc['{field}'].size() > 0 ? doc['{field}'].value : 0.0;"
                    )
                else:
                    script_parts.append(f"float {feature_name} = 0.5;")

                # Add normalization if available
                if feature_name in model_definition["feature_normalizers"]:
                    normalizer = model_definition["feature_normalizers"][feature_name]
                    if "standard" in normalizer:
                        mean = normalizer["standard"]["mean"]
                        std_dev = normalizer["standard"]["standard_deviation"]
                        if std_dev > 0:
                            script_parts.append(
                                f"{feature_name} = ({feature_name} - {mean}) / {std_dev};"
                            )

                # Store weight in params
                script["params"]["weights"][feature_name] = weight

            # Add the final score calculation
            weight_calc = " + ".join(
                [
                    f"params.weights.{name} * {name}"
                    for name in model_definition["definition"].keys()
                ]
            )
            script_parts.append(f"return {weight_calc};")

            # Combine all script parts
            script["source"] = "\n".join(script_parts)

            # Create the request
            request = {
                "query": {"function_score": {"query": {"match_all": {}}, "script_score": script}},
                "size": 10,
            }

            if log:
                display(f"search_with_model() request: {request}")

            response = self.collection.native_search(request)

            if log:
                display(f"search_with_model() response: {response}")

            documents = []
            for doc in response["hits"]["hits"]:
                transformed_doc = doc["_source"] | {"score": doc["_score"]}
                documents.append(transformed_doc)

            return {"docs": documents}
