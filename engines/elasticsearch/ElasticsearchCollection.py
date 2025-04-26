import requests
import json
import time
import numbers
from engines.Collection import Collection
from engines.elasticsearch.config import ES_URL


def is_vector_search(search_args):
    return (
        "query" in search_args
        and isinstance(search_args["query"], list)
        and len(search_args["query"])
        == len(list(filter(lambda o: isinstance(o, numbers.Number), search_args["query"])))
    )


class ElasticsearchCollection(Collection):
    def __init__(self, name, id_field="_id"):
        super().__init__(name)
        self.id_field = id_field

    def get_engine_name(self):
        return "elasticsearch"

    def commit(self):
        response = requests.post(f"{ES_URL}/{self.name}/_refresh")
        time.sleep(1)

    def write(self, dataframe):
        # Process in smaller batches to avoid memory issues
        batch_size = 10000
        total_count = dataframe.count()

        for i in range(0, total_count, batch_size):
            print(
                f"Processing batch {i//batch_size + 1} of {(total_count + batch_size - 1)//batch_size}"
            )

            # Take a batch of records using a window function
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number, col
            import pyspark.sql.functions as F

            # Add row numbers
            window = Window.orderBy(F.lit(1))
            df_with_row_num = dataframe.withColumn("row_num", row_number().over(window))

            # Filter to get the current batch
            batch_df = df_with_row_num.filter(
                (col("row_num") > i) & (col("row_num") <= i + batch_size)
            ).drop("row_num")

            # Convert batch to list of documents
            docs = batch_df.toJSON().collect()
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

            print(f"Processed {min(i + batch_size, total_count)} of {total_count} records")

        self.commit()

    def add_documents(self, docs, commit=True):
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

            # Use script_score for vector search in Elasticsearch 8.x
            request = {
                "query": {
                    "script_score": {
                        "query": {"match_all": {}},
                        "script": {
                            "source": f"cosineSimilarity(params.query_vector, '{field}') + 1.0",
                            "params": {"query_vector": vector},
                        },
                    }
                },
                "size": k,
            }

            # Add filters if specified
            if "filters" in search_args and search_args["filters"]:
                filter_clauses = []
                for field_name, value in search_args["filters"]:
                    if isinstance(value, list):
                        filter_clauses.append({"terms": {field_name: value}})
                    else:
                        filter_clauses.append({"term": {field_name: value}})

                # Convert to bool query with filter
                request["query"] = {"bool": {"must": request["query"], "filter": filter_clauses}}

            return request

        # Basic query
        query = search_args.get("query", "*")
        if isinstance(query, list):
            query = " ".join([q for q in query if isinstance(q, str)])

        # Create request with query_string
        request = {
            "query": {
                "query_string": {
                    "query": query,
                    "default_operator": search_args.get("default_operator", "OR"),
                }
            },
            "size": search_args.get("limit", 10),
        }

        # Add query fields if specified
        if "query_fields" in search_args:
            request["query"]["query_string"]["fields"] = search_args["query_fields"]

        # Add return fields if specified
        if "return_fields" in search_args:
            request["_source"] = search_args["return_fields"]

        # Add filters if specified
        if "filters" in search_args and search_args["filters"]:
            filter_clauses = []
            for field, value in search_args["filters"]:
                if isinstance(value, list):
                    filter_clauses.append({"terms": {field: value}})
                else:
                    filter_clauses.append({"term": {field: value}})

            # Convert to bool query with filter
            request["query"] = {"bool": {"must": request["query"], "filter": filter_clauses}}

        # Add sort if specified
        if "order_by" in search_args:
            request["sort"] = [
                {column if column != "score" else "_score": {"order": sort.lower()}}
                for column, sort in search_args["order_by"]
            ]

        # Add highlight if specified
        if search_args.get("highlight", False):
            request["highlight"] = {
                "fields": {field: {} for field in search_args.get("query_fields", ["*"])}
            }

        # Add min_match if specified
        if "min_match" in search_args:
            request["query"]["query_string"]["minimum_should_match"] = search_args["min_match"]

        # Add query_boosts if specified
        if "query_boosts" in search_args:
            # Convert the query_boosts string to a list of boosted terms
            boosts_str = search_args["query_boosts"]

            # Check if we need to parse the string
            if isinstance(boosts_str, str):
                # Parse the boost string format: "term1^boost1 term2^boost2"
                boost_terms = []
                for term_boost in boosts_str.split():
                    if "^" in term_boost:
                        term, boost = term_boost.split("^", 1)
                        # Remove quotes if present
                        term = term.strip('"')
                        boost = float(boost)
                        boost_terms.append((term, boost))
            else:
                # Assume it's already a list of (term, boost) tuples
                boost_terms = boosts_str

            # Convert to a bool query with should clauses for boosting
            if boost_terms:
                # If we already have a bool query, add to it
                if "bool" in request["query"]:
                    # Add should clauses for boosting
                    should_clauses = []
                    for term, boost in boost_terms:
                        should_clauses.append({"term": {"upc": {"value": term, "boost": boost}}})

                    # Add should clauses to the existing bool query
                    if "should" in request["query"]["bool"]:
                        request["query"]["bool"]["should"].extend(should_clauses)
                    else:
                        request["query"]["bool"]["should"] = should_clauses
                else:
                    # Create a new bool query
                    original_query = request["query"]
                    should_clauses = []
                    for term, boost in boost_terms:
                        should_clauses.append({"term": {"upc": {"value": term, "boost": boost}}})

                    request["query"] = {"bool": {"must": original_query, "should": should_clauses}}

        # Add explain if specified
        if "explain" in search_args:
            request["explain"] = search_args["explain"]

        return request

    def transform_response(self, search_response):
        def format_doc(doc):
            id = doc.get("id", doc[self.id_field])
            formatted = doc["_source"] | {"id": id, "score": doc["_score"]}
            if "_explanation" in doc:
                formatted["[explain]"] = doc["_explanation"]
            return formatted

        if "hits" not in search_response:
            raise ValueError(search_response)

        response = {"docs": [format_doc(d) for d in search_response["hits"]["hits"]]}

        # Add highlighting if present
        if "highlight" in search_response:
            response["highlighting"] = {}
            for hit in search_response["hits"]["hits"]:
                if "highlight" in hit:
                    response["highlighting"][hit[self.id_field]] = hit["highlight"]

        return response

    def native_search(self, request=None, data=None):
        return requests.post(f"{ES_URL}/{self.name}/_search", json=request, data=data).json()

    def spell_check(self, query, log=False):
        # Elasticsearch doesn't have a direct equivalent to Solr's spellcheck
        # This is a simplified implementation using the _search endpoint with suggestions
        request = {
            "suggest": {
                "text": query,
                "simple_phrase": {
                    "phrase": {
                        "field": "name",  # Using name field instead of _all which is deprecated
                        "size": 5,
                        "gram_size": 3,
                        "direct_generator": [{"field": "name", "suggest_mode": "always"}],
                        "highlight": {"pre_tag": "<em>", "post_tag": "</em>"},
                    }
                },
            }
        }

        # Silently ignore log parameter

        try:
            response = self.native_search(request=request)
            suggestions = {}

            if "suggest" in response and "simple_phrase" in response["suggest"]:
                for suggestion in response["suggest"]["simple_phrase"]:
                    for option in suggestion.get("options", []):
                        suggestions[option["text"]] = option["score"]

            return suggestions
        except Exception:
            # Silently handle errors
            return {}
