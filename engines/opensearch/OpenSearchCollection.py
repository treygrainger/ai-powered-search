import random
from re import search
import requests
from aips.spark import get_spark_session
from aips.spark.dataframe import from_sql
from engines.Collection import Collection
from engines.opensearch.config import OPENSEARCH_URL, SCHEMAS
import time
import json
from pyspark.sql import Row
import numbers
from pyspark.sql.types import StringType, StructType, ArrayType, StructField
from pyspark.sql.functions import col, udf, collect_list, create_map

def generate_vector_search_request(search_args):
    vector = search_args.pop("query")
    query_fields = search_args.pop("query_fields", [])
    if not isinstance(query_fields, list):
        raise TypeError("query_fields must be a list")
    elif len(query_fields) == 0:
        raise ValueError("You must specificy at least one field in query_fields")
    else: 
        field = query_fields[0]
    k = search_args.pop("k", 10)
    if "limit" in search_args: 
        if int(search_args["limit"]) > k:
            k = int(search_args["limit"]) #otherwise will only get k results
    request = {"query": {"knn": {field: {"vector": vector,
                                         "k": k}}},
               "size": search_args.get("limit", 5)}             
    if "log" in search_args:
        print("Search Request:")
        print(json.dumps(request, indent="  "))
    return request

def create_filter(field, value):
    if value != "*":
        key = "terms" if isinstance(value, list) else "term"
        return {key: {field: value}}
    else:
        return {"exists": {"field": field}}
    
def query_string_clause(search_args):
    def retrieve_strings(c): return c if isinstance(c, str) else ""

    query_string = "*"
    if "query" in search_args:
        if isinstance(search_args["query"], list):
            query_string = " ".join(list(map(retrieve_strings, search_args["query"])))
        else:
            query_string = search_args["query"]
            if "{!func}" in search_args["query"]: #3.12 hack          
                query_string = query_string.replace("{!func}query(", "").replace(")", "")
    return query_string.strip()

def should_clauses(search_args):
    return list(filter(lambda c: isinstance(c, dict) and "geo_distance" not in c,
                       search_args.get("query", [])))

def must_clauses(search_args):
    return list(filter(lambda c: isinstance(c, dict) and "geo_distance" in c,
                       search_args.get("query", [])))

def is_vector_search(search_args):
    return "query" in search_args and \
           isinstance(search_args["query"], list) and \
           len(search_args["query"]) == len(list(filter(lambda o: isinstance(o, numbers.Number),
                                                        search_args["query"])))

def get_query_fields(search_args):
    if "query_fields" in search_args:
        fields = search_args["query_fields"]
        if isinstance(fields, str):
            fields = [fields]
        return {"fields": fields}
    return {}

class OpenSearchCollection(Collection):
    def __init__(self, name, id_field="_id"):
        super().__init__(name)
        self.id_field = id_field
        
    def get_engine_name(self):
        return "opensearch"
      
    def get_document_count(self):        
        response = requests.get(f"{OPENSEARCH_URL}/{self.name}/_count").json()
        return response.get("count", 0)
    
    def commit(self):
        response = requests.post(f"{OPENSEARCH_URL}/{self.name}/_flush")

    def write(self, dataframe, overwrite=True):
        opts = {"opensearch.nodes": OPENSEARCH_URL,
                "opensearch.net.ssl": "false",
                "opensearch.batch.size.entries": 500}
                #"opensearch.write.field.as.array.include": "image_embedding"}
        if self.id_field != "_id":
            opts["opensearch.mapping.id"] = self.id_field
        mode = "overwrite" if overwrite else "append"
        dataframe.write.format("opensearch").options(**opts).mode(mode).save(self.name)
        self.commit()
        print(f"Successfully written {dataframe.count()} documents")
    
    def add_documents(self, docs, commit=True):
        spark = get_spark_session()
        dataframe = spark.createDataFrame(Row(**d) for d in docs)
        self.write(dataframe, overwrite=False)
    
    def transform_request(self, **search_args):
        #Of the many approaches to implement all these features, the most
        #straightforward and effective is to use the query_string functionality
        #https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html
        #Does multimatching, supports default operator, does dis_max by default.
        if is_vector_search(search_args):
            return generate_vector_search_request(search_args)
        
        query = query_string_clause(search_args)
        should = should_clauses(search_args)
        must = must_clauses(search_args)
        query_fields = get_query_fields(search_args)
        query_clause = {"query_string": {"query": query,
                                         "boost": 0.454545454,
                                         "default_operator": search_args.get("default_operator", "OR")} | query_fields}
        must.append(query_clause)
        request = {"query": {"bool": {"must": must}},
                   "size": 10}
        
        for name, value in search_args.items():
            match name:
                case "search_after":
                    request["search_after"] = value
                case "sort":
                    request["sort"] = [{s[0]: s[1]} for s in value]
                case "return_fields":
                    request["_source"] = value
                case "filters":
                    request["query"]["bool"]["filter"] = [create_filter(f, v) for (f, v) in value]
                case "limit":
                    request["size"] = value
                case "order_by":
                    request["sort"] = [{column if column != "score" else "_score": {"order": sort}}
                                       for (column, sort) in value] 
                case "query_boosts":
                    boost_field = value[0] if isinstance(value, tuple) else "upc"
                    boost_string = value[1] if isinstance(value, tuple) else value
                    boosts = [(b.split("^")[0], b.split("^")[1])
                              for b in boost_string.split(" ")]
                    clauses = [{"match": {boost_field: {"query": b[0],
                                                        "boost": b[1]}}}
                               for b in boosts]
                    should.extend(clauses)
                case "min_match":
                    pass #ch5
                case "index_time_boost":
                    should.append({"rank_feature": {"field": f"{value[0]}.{value[1]}"}})
                case "explain":
                    request["explain"] = value
                case "hightlight":
                    request["highlight"] = {"fields": {value: {}}}
                case "ubi":
                    request["ext"] = {"ubi": value}
                case _:
                    pass

        request["query"]["bool"]["should"] = should

        if "log" in search_args:
            print("Search Request:")
            print(json.dumps(request, indent="  "))
        return request
    
    def transform_response(self, search_response):
        def format_doc(doc):
            id = doc.get("id", doc["_id"])
            formatted = doc["_source"] | {"id": id,
                                          "score": doc["_score"]}
            if "_explanation" in doc:
                formatted["[explain]"] = doc["_explanation"]
            if "sort" in doc:
                formatted["sort"] = doc["sort"]
            return formatted
            
        if "hits" not in search_response:
            raise ValueError(search_response)
        response = {"docs": [format_doc(d)
                             for d in search_response["hits"]["hits"]]}
        if "highlighting" in search_response:
            response["highlighting"] = search_response["highlighting"]
        return response
        
    def native_search(self, request=None, data=None):
        return requests.post(f"{OPENSEARCH_URL}/{self.name}/_search", json=request, data=data).json()

    def search(self, **search_args):
        request = self.transform_request(**search_args)
        search_response = self.native_search(request=request)
        return self.transform_response(search_response)
    
    def spell_check(self, query, log=False):
        request = {"suggest": {"spell-check" : {"text" : query,
                                                "term" : {"field" : "_text_",
                                                          "suggest_mode" : "missing"}}}}
        if log: print(json.dumps(request, indent=2))
        response = self.native_search(request=request)
        if log: print(json.dumps(response, indent=2))
        suggestions = {}
        if len(response["suggest"]["spell-check"]):
            suggestions = {term["text"]: term["freq"] for term
                           in response["suggest"]["spell-check"][0]["options"]}
        return suggestions
    
    def create_view_from_collection(self, view_name, spark, log=False):
        if self.name == "tmdb_with_embeddings":
            search_after = None
            documents = []
            while True:
                request = {"query": "*",
                           "return_fields": ["image_id", "movie_id", "title", "image_embedding"],
                           "limit": 500,
                           "sort": [("_id", "asc")]}
                if search_after:
                    request["search_after"] = search_after
                response = self.search(**request)
                documents.extend(response["docs"])
                if len(response["docs"]) != 500:
                    break
                search_after = response["docs"][-1]["sort"]
            for d in documents:
                d["image_embedding"] = [f'{str(float(s))}' for s in d["image_embedding"]]
                
            schema = StructType([StructField("image_id", StringType()),
                                 StructField("movie_id", StringType()),
                                 StructField("title", StringType()),
                                 StructField("image_embedding", ArrayType(StringType()))])
            dataframe = spark.createDataFrame(documents, schema=schema)
            dataframe.createOrReplaceTempView(view_name)
        else:
            parse_id_udf = udf(lambda s: s["_id"], StringType())
            opts = {"opensearch.nodes": OPENSEARCH_URL,
                    "opensearch.net.ssl": "false"}
            if SCHEMAS.get(self.name.lower(), {}).get("id_field", "") == "_id":
                opts["opensearch.read.metadata"] = "true"
            dataframe = spark.read.format("opensearch").options(**opts).load(self.name)
            if "_metadata" in dataframe.columns and \
                SCHEMAS.get(self.name.lower(), {}).get("id_field", "") == "_id":
                dataframe = dataframe.withColumn("id", parse_id_udf(col("_metadata")))
                dataframe = dataframe.drop("_metadata")
            dataframe.createOrReplaceTempView(view_name)

    def load_index_time_boosting_dataframe(self, boosts_collection_name, boosted_products_collection_name):
        product_query = f"SELECT * FROM {boosted_products_collection_name}"
        boosts_query = f"SELECT doc, boost, REPLACE(query, '.', '') AS query FROM {boosts_collection_name}"

        grouped_boosts = from_sql(boosts_query).groupBy("doc") \
            .agg(collect_list(create_map("query", "boost"))[0].alias("signals_boosts")) \
            .withColumnRenamed("doc", "upc")
        return from_sql(product_query).join(grouped_boosts, "upc")
