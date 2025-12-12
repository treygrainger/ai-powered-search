import asyncio
from concurrent.futures import ThreadPoolExecutor
import random
import threading
from typing import Any, Coroutine
import httpx
import requests
from aips.spark import get_spark_session
from engines.Collection import Collection, is_vector_search, DEFAULT_SEARCH_SIZE, DEFAULT_NEIGHBORS
import engines.vespa.config as config
import time
import json
from pyspark.sql import Row
  
def run_coroutine_sync(coroutine, timeout=30):
    def run_in_new_loop():
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        try:
            return new_loop.run_until_complete(coroutine)
        finally:
            new_loop.close()

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coroutine)

    if threading.current_thread() is threading.main_thread():
        if not loop.is_running():
            return loop.run_until_complete(coroutine)
        else:
            with ThreadPoolExecutor() as pool:
                future = pool.submit(run_in_new_loop)
                return future.result(timeout=timeout)
    else:
        return asyncio.run_coroutine_threadsafe(coroutine, loop).result()
    
class VespaCollection(Collection):
    def __init__(self, name, vespa_url=config.VESPA_URL):
        self.vespa_url = vespa_url
        self.namespace = config.DEFAULT_NAMESPACE
        super().__init__(name)

    def write(self, dataframe, overwrite=False):
        print(f"\nWriting {dataframe.count()} documents to '{self.name}' collection")

        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=16, pool_maxsize=16)
        session.mount('http://', adapter)

        if overwrite:
            url = f"{self.vespa_url}/document/v1/{self.namespace}/{self.name}/docid?selection=true&cluster={self.namespace}"
            response = requests.delete(url)
            if response.status_code not in [200, 201]:
                print(response)

        client = httpx.AsyncClient(http2=True)

        async def async_write(doc):
            retries = 5 
            while retries >= 0:
                try:
                    doc = {k: str(v) for k, v in doc.items()}
                    doc_id = doc.get("id", doc.get("upc", str(hash(json.dumps(doc, sort_keys=True)))))
                    json_document = {"fields": doc}
                    url = f"{self.vespa_url}/document/v1/{self.namespace}/{self.name}/docid/{doc_id}"                    
                    response = await client.post(url, json=json_document, headers={"Content-Type": "application/json"})
                    response.raise_for_status()
                    return True
                except Exception as ex:
                    print(str(retries) + "  " + str(ex))
                    print(response.content)
                    retries -= 1
                    time.sleep(5 + 20 * random.random())

        async def write_all_async(docs):
            await asyncio.gather(*[async_write(d) for d in docs])
            retries = 5 
            while retries >= 0:
                try:
                    doc = {k: str(v) for k, v in doc.items()}
                    doc_id = doc.get("id", doc.get("upc", str(hash(json.dumps(doc, sort_keys=True)))))
                    json_document = {"fields": doc}
                    url = f"{self.vespa_url}/document/v1/{self.namespace}/{self.name}/docid/{doc_id}"
                    response = session.post(url, json=json_document, headers={"Content-Type": "application/json"})
                    response.raise_for_status()
                    return True
                except Exception as ex:
                    print(str(retries) + "  " + str(ex))
                    print(response.content)
                    retries -= 1
                    time.sleep(5 + 20 * random.random())
                  

        run_coroutine_sync(write_all_async)
        #with ThreadPoolExecutor(max_workers=16) as executor:
        #    for row in dataframe.collect():
        #        last_future = executor.submit(async_write, row.asDict())

        print(f"Successfully written {dataframe.count()} documents")
        self.commit()

    def commit(self):
        time.sleep(2)

    def get_document_count(self):
        try:
            request = {"yql": f"select * from {self.name} where true limit 0", "hits": 0}
            response = requests.post(f"{self.vespa_url}/search/", json=request, 
                                     headers={"Content-Type": "application/json"})
            if response.status_code != 200:
                return 0
            result = response.json()
            return result.get("root", {}).get("fields", {}).get("totalCount", 0)
        except Exception as ex:
            print(f"Error getting document count: {ex}")
            return 0

    def get_engine_name(self):
        return "vespa"
        
    def is_query_by_id(self, search_args):
        return "query_fields" in search_args and \
               len(search_args.get("query_fields", [])) == 1 and \
               search_args["query_fields"][0] in ["id", "documentid"]
    
    def is_bm25_search(self, search_args):
        return "query" in search_args and \
            search_args["query"] not in ["", "*", None] and \
            not is_vector_search(search_args) and \
            not self.is_query_by_id(search_args)
    
    def generate_filter_clause(self, search_args):
        conditions = []
        if "filters" in search_args and len(search_args["filters"]) > 0:
            for filter_item in search_args["filters"]:
                field, value = filter_item[0], filter_item[1]
                
                operator = "=" if not field.startswith("-") else "!="
                field = field.lstrip("-")
                
                if isinstance(value, list):
                    or_conditions = " OR ".join([f'{field} = "{v}"' for v in value])
                    conditions.append(f"({or_conditions})")
                elif value == "*":
                    conditions.append(f'{field} matches ".*"')
                else:
                    conditions.append(f'{field} {operator} "{value}"')
        
        return " AND ".join(conditions) if conditions else None
        
    def generate_sort_clause(self, search_args):
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
        if query.find("{!func}") != -1:
            query = query.replace('{!func}query("', "").replace(')"', "")
        return query

    def generate_bm25_query(self, search_args):
        query = self.parse_query_functions(search_args["query"])
        return query

    def get_collection_field_names(self):
        try:
            request = {"yql": f"select * from {self.name} where true limit 1", "hits": 1}
            response = self.native_search(request)
            if response.status_code == 200:
                children = response.json().get("root", {}).get("children", [])
                if children:
                    return list(children[0].get("fields", {}).keys())
        except Exception as ex:
            print(f"Error getting field names: {ex}")
        return []
    
    def add_documents(self, docs, commit=True):
        spark = get_spark_session()
        dataframe = spark.createDataFrame(Row(**d) for d in docs)
        self.write(dataframe, overwrite=False)

    def transform_request(self, **search_args):
        #https://docs.vespa.ai/en/reference/api/query.html
        select_fields = ", ".join(self.generate_return_fields(search_args))
        limit = search_args.get("limit", DEFAULT_SEARCH_SIZE)        
        where_conditions = []
        
        if is_vector_search(search_args):
            vector = search_args.get("query")
            pass
        
        elif self.is_bm25_search(search_args):
            query = self.generate_bm25_query(search_args)
            query_fields = self.generate_query_fields(search_args)
            
            if query_fields:
                field_conditions = [f'{field} contains "{query}"' for field in query_fields]
                where_conditions.append(f"weakAnd({', '.join(field_conditions)})")

        elif self.is_query_by_id(search_args):
            query_fields = self.generate_query_fields(search_args)
            if query_fields:
                field = query_fields[0]
                value = search_args.get("query", "")
                where_conditions.append(f'{field} = "{value}"')
        
        filter_clause = self.generate_filter_clause(search_args)
        if filter_clause:
            where_conditions.append(filter_clause)
        if not where_conditions:
            where_conditions.append("true")
        
        where_clause = " AND ".join(where_conditions)
        order_by_clause = self.generate_sort_clause(search_args)
        yql = f"select {select_fields} from {self.name} where {where_clause}"
        
        if order_by_clause:
            yql += f" order by {order_by_clause}"
        yql += f" limit {limit}"

        request = {"hits": limit, "yql": yql}
        if search_args.get("explain", False):
            request["tracelevel"] = 1
        
        return request
    
    def transform_response(self, search_response):
        docs = []
        
        if "root" in search_response and "children" in search_response["root"]:
            for child in search_response["root"]["children"]:
                doc = child.get("fields", {}).copy()
                
                # Add relevance score
                doc["score"] = child.get("relevance", 0.0)
                
                # Extract document ID from Vespa format
                # Format: "id:namespace:doctype::actualid"
                vespa_id = child.get("id", "")
                if "::" in vespa_id:
                    doc["id"] = vespa_id.split("::")[-1]
                elif "id" not in doc:
                    doc["id"] = vespa_id
                
                docs.append(doc)
        
        response = {"docs": docs}
        return response
        
    def native_search(self, request=None, data=None):
        response = requests.post(f"{self.vespa_url}/search/", json=request,
                                 headers={"Content-Type": "application/json"})
        response.raise_for_status()
        return response.json()
    
    def spell_check(self, query, log=False):
        return {}
    
    def create_view_from_collection(self, view_name, spark, log=False):
        request = {"return_fields": "*", "limit": 1000}
        all_documents = []
        
        try:
            page = 0
            while True:
                if log:
                    print(f"Fetching page {page}...")                
                docs = self.search(**request)["docs"]
                all_documents.extend(docs)                
                if len(docs) < request["limit"]:
                    break
                page += 1
                request["offset"] = page * request["limit"]                    
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
        pass