import asyncio
import math
from multiprocessing import Process
from concurrent.futures import ThreadPoolExecutor
import datetime
import functools
import os
import random
import threading
from typing import Any, Coroutine
import httpx
import requests
from aips.spark import get_spark_session
from aips.spark.dataframe import from_sql
from engines.Collection import Collection, is_vector_search, DEFAULT_SEARCH_SIZE, DEFAULT_NEIGHBORS
import engines.vespa.config as config
import time
import json
from pyspark.sql import Row
from pyspark.sql.functions import collect_list, create_map
import dateutil.parser 

def parse_datetime_string(value):
    if not isinstance(value, str):
        return None
    try: 
        return datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None

def format_document_for_writing(doc):
    #{k: int(v.timestamp()) if isinstance(v, datetime.datetime) else v for k, v in doc.items()}
    for field, value in doc.items():
        if field == "location_coordinates":
            if not isinstance(value, dict):
                (lat, lon) = (value or "0,0").split(",")
                doc[field] = {"lat": float(lat), "lng": float(lon)}
        value_as_datetime = parse_datetime_string(value)
        if value_as_datetime:
            value = value_as_datetime
        if isinstance(value, datetime.datetime):
            doc[field] = int(value.timestamp())
    return doc

def get_document_id(doc):
    return doc.get("id", doc.get("upc", str(hash(json.dumps(doc, sort_keys=True)))))

def format_query_value(value):
    if isinstance(value, str) and value.lower() not in ["true", "false"]:
        value = f'"{value}"'
    return value

def write_batch_asyncio(collection, client, documents):    
    async def write_async(doc):
        response = None
        try:
            doc = format_document_for_writing(doc)
            doc_id = get_document_id(doc)
            json_document = {"fields": doc}
            url = f"{collection.vespa_url}/document/v1/{collection.namespace}/{collection.name}/docid/{doc_id}"                    
            response = await client.post(url, json=json_document, headers={"Content-Type": "application/json"})
            response.raise_for_status()
        except Exception as ex:
            print("Error writing document: " + str(ex))
            if response:
                print(response)
                print(response.json())
            print(doc)
            raise 

    async def async_write_batch(docs, batch_size=25):
        for i in range(int(len(docs) / batch_size) + 1):
            await asyncio.gather(*[write_async(d) for d in docs[i * batch_size:(i + 1) * batch_size]])

    new_loop = asyncio.new_event_loop()
    try:
        return new_loop.run_until_complete(async_write_batch(documents))
    finally:
        new_loop.close()
            
def write_batch(collection, client, documents):    
    for i, doc in enumerate(documents):
        response = None
        try:
            doc = format_document_for_writing(doc)
            doc_id = get_document_id(doc)
            json_document = {"fields": doc}
            url = f"{collection.vespa_url}/document/v1/{collection.namespace}/{collection.name}/docid/{doc_id}"                    
            response = client.post(url, json=json_document, headers={"Content-Type": "application/json"})
            response.raise_for_status()
        except Exception as ex:
            print("Error writing document: " + str(ex))
            if response:
                print(response)
                print(response.json())
            print(doc)
            raise  

class VespaCollection(Collection):
    def __init__(self, name, vespa_url=config.VESPA_URL):
        self.vespa_url = vespa_url
        self.namespace = config.DEFAULT_NAMESPACE
        super().__init__(name)
 
    def write(self, dataframe, overwrite=False):        
        print(f"\nWriting {dataframe.count()} documents to '{self.name}' collection")

        if overwrite:
            url = f"{self.vespa_url}/document/v1/{self.namespace}/{self.name}/docid?selection=true&cluster={self.namespace}"
            response = requests.delete(url)
        
        docs = [d.asDict() for d in dataframe.collect()]

        client = httpx.Client(http2=True, http1=False, verify=True)
        procs = []
        workers = os.cpu_count() * (8 if len(docs) > 100000 else 2)
        workload_size = math.ceil(len(docs) / workers)

        for i in range(workers):
            p = Process(target=write_batch, args=(self, client, docs[i * workload_size:(i + 1) * workload_size]))
            p.start()
            procs.append(p)

        for p in procs:
            p.join()

        print(f"Successfully written {dataframe.count()} documents")
        self.commit()

    def commit(self):
        pass

    def get_document_count(self):
        try:
            request = {"yql": f"select * from {self.name} where true limit 0", "hits": 0}
            response = requests.post(f"{self.vespa_url}/search/", json=request, 
                                     headers={"Content-Type": "application/json"})
            result = response.json()
            if response.status_code != 200:
                return 0
            return result.get("root", {}).get("fields", {}).get("totalCount", 0)
        except Exception as ex:
            print(f"Error getting document count: {ex}")
            return 0

    def get_engine_name(self):
        return "vespa"
        
    def is_query_by_id(self, search_args):
        return "query_fields" in search_args and \
               len(search_args.get("query_fields", [])) == 1 and \
               search_args["query_fields"][0] in ["id", "upc"]
    
    def is_bm25_search(self, search_args):
        return "query" in search_args and \
            search_args["query"] not in ["", "*", None] and \
            not is_vector_search(search_args)
    
    def generate_filter_clause(self, search_args):
        conditions = []
        if "filters" in search_args and len(search_args["filters"]) > 0:
            for filter_item in search_args["filters"]:
                field, value = filter_item[0], filter_item[1]
                
                operator = "=" if not field.startswith("-") else "!="
                field = field.lstrip("-")
                
                if isinstance(value, list):
                    or_conditions = " OR ".join([f'{field} = {format_query_value(v)}' for v in value])
                    conditions.append(f"({or_conditions})")
                elif value == "*":
                    conditions.append(f'{field} matches ".*"')
                else:
                    conditions.append(f'{field} {operator} {format_query_value(value)}')
        
        return " AND ".join(conditions) if conditions else None
        
    def generate_order_by_clause(self, search_args):
        if "order_by" not in search_args:
            return None
            
        order_clauses = []
        for column, direction in search_args["order_by"]:            
            if column == "score":
                column = "'[relevance]'"
            order_clauses.append(f"{column} {direction.upper()}")        
        return ", ".join(order_clauses) if order_clauses else None

    def generate_return_fields(self, search_args):
        if "return_fields" in search_args:
            fields = [f for f in search_args["return_fields"] if f != "score"]
            return fields if fields else ["*"]
        return ["*"]
        
    def generate_query_fields(self, search_args):
        if "query_fields" in search_args:
            fields = search_args["query_fields"]
            if isinstance(fields, str):
                return [fields]
            return fields
        return None
    
    def parse_query_functions(self, query):
        # '{!func}query("one") {!func}query("two") {!func}query("three")
        if isinstance(query, str) and query.find("{!func}") != -1:
            query = query.replace('{!func}query("', "").replace(')"', "")
        return query

    def parse_simple_query_weights(self, query):
        simple_query = ""
        if "^" in query:
            for query_part in query.split(" "):
                if "^" in query_part:
                    token, weight = query_part.split("^")
                    if "." in weight:
                        weight = int(float(weight) * 100)
                    query_part = f"{token}!{weight}"
                simple_query += f"{query_part} "
        return simple_query.rstrip()

    def get_index_of_first_query(self, query):
        if query and isinstance(query, list):
            for i, q in enumerate(query):
                q = q["query"] if isinstance(q, dict) else q
                if "(" not in q: 
                    return i
            return 0
        return None

    def generate_bm25_query(self, search_args):
        query = search_args.get("query") or ""
        if isinstance(query, list):
            i = self.get_index_of_first_query(query)
            query = query[i] if isinstance(query[i], str) else query[i]["query"]
        query = self.parse_query_functions(query)
        if query == "*":
            query = ""            
        return self.parse_simple_query_weights(query)

    def add_documents(self, docs, commit=True):
        spark = get_spark_session()
        dataframe = spark.createDataFrame(Row(**d) for d in docs)
        self.write(dataframe, overwrite=False)

    def transform_request(self, **search_args):
        #https://docs.vespa.ai/en/reference/api/query.html
        select_fields = ", ".join(self.generate_return_fields(search_args))
        limit = search_args.get("limit", DEFAULT_SEARCH_SIZE)        
        where_conditions = []
        
        request = {"hits": limit, "offset": search_args.get("offset") or 0}
        
        if "ranking_profile" in search_args:
            request["ranking"] = search_args["ranking_profile"]

        if self.is_query_by_id(search_args):
            field = search_args["query_fields"][0]
            values = ", ".join(f"'{s}'" for s in search_args["query"].split(" "))
            where_conditions.append(f'{field} in ({values})')
        elif is_vector_search(search_args):
            field = search_args["query_fields"][0]
            request["ranking"] = f"{self.name}_vector_similarity"
            vector_string = ",".join(str(s) for s in search_args["query"])
            request["input.query(query_vector)"] = f"[{vector_string}]"
            where_conditions.append("{targetHits: 100}" + f"nearestNeighbor({field}, query_vector)")
        else:
            #elif self.is_bm25_search(search_args):
            #"yql": "select * from reviews where content contains ({weight:65}\"banchan\" or content contains ({weight:54}\"bulgogi\"))"
            query = self.generate_bm25_query(search_args)
            query_fields = self.generate_query_fields(search_args)
            if query:
                request["model.queryString"] = query
                clauses = ["userQuery()"]
                if "query_boosts" in search_args:
                    request["query_boosts"] = self.parse_query_functions(search_args["query_boosts"])
                    clauses.append("userQuery(@query_boosts)")
                if isinstance(search_args["query"], list):
                    main_query_i = self.get_index_of_first_query(search_args["query"])
                    for i, q in enumerate(search_args["query"]):
                        if i != main_query_i:
                            q = self.parse_query_functions(q["query"] if isinstance(q, dict) else q)
                            request[f"query_clause_{i}"] = q
                            clauses.append(f"userQuery(@query_clause_{i})")
                where_conditions.append("rank(" + ", ".join(clauses) + ")")
        
        filter_clause = self.generate_filter_clause(search_args)
        if filter_clause:
            where_conditions.append(filter_clause)
        if not where_conditions:
            where_conditions.append("true")
        
        where_clause = " AND ".join(where_conditions)
        request["yql"] = f"select {select_fields} from {self.name} where {where_clause}"
        
        order_by_clause = self.generate_order_by_clause(search_args)
        if order_by_clause:
            request["yql"] += f" order by {order_by_clause}"

        if search_args.get("explain", False):
            request["tracelevel"] = 1
        
        return request
    
    def transform_response(self, search_response):
        docs = []
        if "root" in search_response and "children" in search_response["root"]:
            for child in search_response["root"]["children"]:
                doc = child.get("fields", {}).copy()                
                doc["score"] = child.get("relevance", 0.0)
                vespa_id = child.get("id", "")
                if "::" in vespa_id:
                    doc["id"] = vespa_id.split("::")[-1]
                elif "id" not in doc:
                    doc["id"] = vespa_id
                    
                for field in doc.keys(): #flatten complex types
                    if isinstance(doc[field], dict) and "tensor" in doc[field].get("type", ""):
                        doc[field] = doc[field]["values"]
                
                docs.append(doc)
        return {"docs": docs}
        
    def native_search(self, request=None, data=None):
        response = requests.post(f"{self.vespa_url}/search?queryProfile=standard", json=request,
                                 headers={"Content-Type": "application/json"})        
        return response.json()
    
    def spell_check(self, query, log=False):
        #https://docs.vespa.ai/en/ranking/reranking-in-searcher.html
        #https://hunspell.github.io/
        return {}
    
    def create_view_from_collection(self, view_name, spark, log=False):        
        request = {"query": "*", "return_fields": "*", "limit": 10000, "offset": 0}
        #if log:
        #    request["log"] = True
        all_documents = []
        try:
            while True:
                if log:
                    print(f'Fetching from offset {request["offset"]}...')                
                docs = self.search(**request)["docs"]
                all_documents.extend(docs)                
                if len(docs) < request["limit"]:
                    break
                request["offset"] += request["limit"]                    
        except Exception as ex:
            print(f"Create view exception: {ex}")
        
        if log:
            print(f"Loaded {len(all_documents)} docs from Vespa")
        
        if all_documents:
            dataframe = spark.createDataFrame(data=all_documents)
            dataframe.createOrReplaceTempView(view_name)
        else:
            print(f"Warning: No documents found in collection {self.name}")
    
    def load_index_time_boosting_dataframe(self, boosts_collection_name, boosted_products_collection_name):
        product_query = f"SELECT * FROM {boosted_products_collection_name}"
        boosts_query = f"SELECT doc, boost, REPLACE(query, '.', '') AS query FROM {boosts_collection_name}"

        grouped_boosts = from_sql(boosts_query).groupBy("doc") \
            .agg(collect_list(create_map("query", "boost"))[0].alias("signals_boosts")) \
            .withColumnRenamed("doc", "upc")
        return from_sql(product_query).join(grouped_boosts, "upc")
