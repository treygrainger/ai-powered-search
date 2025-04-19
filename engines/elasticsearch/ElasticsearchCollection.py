import requests
import json
import time
import numbers
import os
from engines.Collection import Collection

# Get host and port from environment variables with fallbacks
AIPS_ES_HOST = os.getenv("AIPS_ES_HOST") or "aips-elasticsearch"
AIPS_ES_PORT = os.getenv("AIPS_ES_PORT") or "9200"
ES_URL = f"http://{AIPS_ES_HOST}:{AIPS_ES_PORT}"


def is_vector_search(search_args):
    return (
        "query" in search_args
        and isinstance(search_args["query"], list)
        and len(search_args["query"])
        == len(list(filter(lambda o: isinstance(o, numbers.Number), search_args["query"])))
    )


class ElasticsearchCollection(Collection):
    def __init__(self, name):
        super().__init__(name)

    def get_engine_name(self):
        return "elasticsearch"

    def commit(self):
        requests.post(f"{ES_URL}/{self.name}/_refresh")
        time.sleep(1)

    def write(self, dataframe):
        # Convert dataframe to list of documents
        docs = dataframe.toJSON().collect()
        docs = [json.loads(doc) for doc in docs]

        # Bulk index documents
        bulk_data = []
        for doc in docs:
            # Add index action
            bulk_data.append({"index": {"_index": self.name}})
            # Add document
            bulk_data.append(doc)

        if bulk_data:
            response = requests.post(
                f"{ES_URL}/_bulk",
                headers={"Content-Type": "application/x-ndjson"},
                data="\n".join(json.dumps(item) for item in bulk_data) + "\n",
            )

            self.commit()
            print(f"Successfully written {len(docs)} documents")

    def add_documents(self, docs, commit=True):
        print(f"\nAdding Documents to '{self.name}' index")

        bulk_data = []
        for doc in docs:
            # Add index action
            bulk_data.append({"index": {"_index": self.name}})
            # Add document
            bulk_data.append(doc)

        response = requests.post(
            f"{ES_URL}/_bulk",
            headers={"Content-Type": "application/x-ndjson"},
            data="\n".join(json.dumps(item) for item in bulk_data) + "\n",
        ).json()

        if commit:
            self.commit()

        return response

    def transform_request(self, **search_args):
        request = {"query": {"match_all": {}}, "size": 10}

        # Handle vector search
        if is_vector_search(search_args):
            vector = search_args.pop("query")
            query_fields = search_args.pop("query_fields", [])
            if not isinstance(query_fields, list):
                raise TypeError("query_fields must be a list")
            elif len(query_fields) == 0:
                raise ValueError("You must specify at least one field in query_fields")
            else:
                field = query_fields[0]

            k = search_args.pop("k", 10)
            if "limit" in search_args:
                if int(search_args["limit"]) > k:
                    k = int(search_args["limit"])

            request["query"] = {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {
                        "source": f"cosineSimilarity(params.query_vector, '{field}') + 1.0",
                        "params": {"query_vector": vector},
                    },
                }
            }
            request["size"] = k

        # Process other search arguments
        for name, value in search_args.items():
            match name:
                case "query":
                    if value and not is_vector_search({"query": value}):
                        query_str = (
                            " ".join(value)
                            if isinstance(value, list) and isinstance(value[0], str)
                            else value
                        )
                        if query_str and query_str != "*:*":
                            request["query"] = {"query_string": {"query": query_str}}
                case "query_fields":
                    if "query" in request and "query_string" in request["query"]:
                        request["query"]["query_string"]["fields"] = value
                case "return_fields":
                    request["_source"] = value
                case "filters":
                    filter_clauses = []
                    for f in value:
                        filter_value = f[1]
                        if isinstance(filter_value, list):
                            filter_clauses.append({"terms": {f[0]: filter_value}})
                        elif isinstance(filter_value, bool):
                            filter_clauses.append({"term": {f[0]: filter_value}})
                        else:
                            filter_clauses.append({"term": {f[0]: filter_value}})

                    if filter_clauses:
                        if "query" in request and request["query"] != {"match_all": {}}:
                            request["query"] = {
                                "bool": {"must": request["query"], "filter": filter_clauses}
                            }
                        else:
                            request["query"] = {"bool": {"filter": filter_clauses}}
                case "limit":
                    request["size"] = value
                case "order_by":
                    request["sort"] = [
                        {column: {"order": sort.lower()}} for (column, sort) in value
                    ]
                case "default_operator":
                    if "query" in request and "query_string" in request["query"]:
                        request["query"]["query_string"]["default_operator"] = value.upper()
                case "min_match":
                    if "query" in request and "query_string" in request["query"]:
                        request["query"]["query_string"]["minimum_should_match"] = value
                case "highlight":
                    if value:
                        request["highlight"] = {
                            "fields": {
                                field: {} for field in search_args.get("query_fields", ["*"])
                            }
                        }

        return request

    def transform_response(self, search_response):
        docs = []
        for hit in search_response.get("hits", {}).get("hits", []):
            doc = hit.get("_source", {})
            doc["id"] = hit.get("_id")
            doc["score"] = hit.get("_score", 1.0)
            docs.append(doc)

        response = {"docs": docs}

        # Add highlighting if present
        if "highlight" in search_response:
            response["highlighting"] = {}
            for hit in search_response["hits"]["hits"]:
                if "highlight" in hit:
                    response["highlighting"][hit["_id"]] = hit["highlight"]

        return response

    def native_search(self, request=None, data=None):
        if data:
            response = requests.post(f"{ES_URL}/{self.name}/_search", data=data).json()
        else:
            response = requests.post(f"{ES_URL}/{self.name}/_search", json=request).json()
        return response

    def spell_check(self, query, log=False):
        # Elasticsearch doesn't have a direct equivalent to Solr's spellcheck
        # This is a simplified implementation using the _search endpoint with suggestions
        request = {
            "suggest": {
                "text": query,
                "simple_phrase": {
                    "phrase": {
                        "field": "_all",
                        "size": 5,
                        "gram_size": 3,
                        "direct_generator": [{"field": "_all", "suggest_mode": "always"}],
                        "highlight": {"pre_tag": "<em>", "post_tag": "</em>"},
                    }
                },
            }
        }

        if log:
            print("Elasticsearch spellcheck request syntax:")
            print(json.dumps(request, indent=2))

        try:
            response = requests.post(f"{ES_URL}/{self.name}/_search", json=request).json()
            suggestions = {}

            if "suggest" in response and "simple_phrase" in response["suggest"]:
                for suggestion in response["suggest"]["simple_phrase"]:
                    for option in suggestion.get("options", []):
                        suggestions[option["text"]] = option["score"]

            return suggestions
        except Exception as e:
            print(f"Spell check error: {e}")
            return {}
