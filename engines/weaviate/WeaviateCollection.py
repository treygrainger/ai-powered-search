from xml.dom import NotFoundErr
import requests
from aips.spark import get_spark_session
from engines.Collection import Collection, is_vector_search, DEFAULT_SEARCH_SIZE, DEFAULT_NEIGHBORS
from engines.weaviate.config import WEAVIATE_HOST, WEAVIATE_PORT, WEAVIATE_URL, get_vector_field_name, \
    schema_contains_custom_vector_field, schema_contains_id_field, SCHEMAS, get_boost_field
import time
from pyspark.sql.functions import lit
from pyspark.sql import Row

from graphql_query import Query, Argument, Field, Operation    

def rename_id_field(dataframe): #Hack $WV8_001
    #Must exist because Weaviate reserves the 'id' field (and also the _id field??)
    if "id" in dataframe.columns:
        return dataframe.withColumnRenamed("id", "__id")
    return dataframe

def is_query_by_id(search_args):
    #hacks for sort by not existing in bm25.
    #Will catch all queries that can be done without using BM25 handler
    return len(search_args.get("query_fields", [])) == 1 and \
           len(search_args.get("order_by", [])) == 1 and \
           search_args["order_by"][0][0] == "boost"

def is_bm25_query(search_args):
    return "query" in search_args and \
           search_args["query"] not in ["", "*"] and \
           not is_query_by_id(search_args)

def generate_filter_clause(search_args):    
    def generate_filter_argument(name, operator, value):
        value_type = "valueText"
        if isinstance(value, list):
            value = "[" + ", ".join(['"' + v + '"' for v in value]) + "]"
        else:
            value = value if value[0] == '"' else '"' + value + '"'
        return [Argument(name="path", value=['"' + name + '"']),
                Argument(name="operator", value=operator),
                Argument(name=value_type, value=value)]
    filters = []
    filter_clause = None
    if is_query_by_id(search_args):
        id_filter = generate_filter_argument(search_args["query_fields"][0],
                                             "Equal", search_args["query"])
        filters.append(id_filter)
    if "filters" in search_args and len(search_args["filters"]) > 0:
        for filter in search_args["filters"]:
            operator = "Equal" if filter[0][0] != "-" else "NotEqual"
            filter_arg = generate_filter_argument(filter[0].strip("-"),
                                                  operator, filter[1])
            filters.append(filter_arg)
    if len(filters) > 0:
        filter_clause = Argument(name="where", value=[
            Argument(name="operator", value="And"),
            Argument(name="operands", value=filters)])
    return filter_clause
    
def generate_sort_clause(search_args):
    sorts = []
    sort_clause = None
    if not is_bm25_query(search_args): #sorting not implemented for bm25
        if "order_by" in search_args:
            for item in search_args["order_by"]:
                name = item[0] if item[0] != "score" else "_score"
                sorts.append([Argument(name="path", value='"' + name + '"'),
                            Argument(name="order", value=item[1])])
    if len(sorts) > 0:
        sort_clause = Argument(name="sort", value=sorts)
    return sort_clause

class WeaviateCollection(Collection):
    def __init__(self, name):
        super().__init__(name)
        
    def commit(self):
        time.sleep(2)

    def get_engine_name(self):
        return "weaviate"
    
    def generate_return_fields(self, search_args):
        if "return_fields" in search_args:
            return_fields = search_args["return_fields"].copy()
            additional_fields = []
            vector_field = get_vector_field_name(self.name)
            if vector_field and vector_field in return_fields:
                additional_fields.append("vector")
                return_fields.remove(vector_field)
                additional_fields.append("distance")
            if "score" in return_fields:
                return_fields.remove("score")
                additional_fields.append("score")
            if "id" in return_fields:
                return_fields.remove("id")
                if schema_contains_id_field(self.name): #Must appropriately apply id or __id for $WV8_001
                    return_fields.append("__id")
                additional_fields.append("id")
            if additional_fields:
                return_fields.append(Field(name="_additional", fields=additional_fields))
            return return_fields
        else:
            return [Field(name="_additional", fields=["id", "score"])]
        
    def generate_query_fields(self, search_args):
        if "query_fields" in search_args:
            fields = search_args["query_fields"]
            if "id" in fields and schema_contains_id_field(self.name): #Hacky hacky - $WV8_001
                fields.remove("id")
                fields.append("__id")
            if "query_boosts" in search_args:
                if isinstance(search_args["query_boosts"], tuple):
                    boost_field = search_args["query_boosts"][0]
                else:
                    boost_field = get_boost_field(self.name)
                fields.append(boost_field)
            if "index_time_boost" in search_args:
                fields.append(search_args["index_time_boost"][0])
            fields = list(map(lambda s: '"' + s + '"', fields)) # must pad all fields with double quotes
            return fields
        return None
    
    # Weaviate does not support query time, to achieve this stuff the query with expanded boost terms
    def generate_bm25_query(self, search_args):
        query = search_args["query"]
        if "query_boosts" in search_args:
            boost_string = search_args["query_boosts"][1] if isinstance(search_args["query_boosts"], tuple) else search_args["query_boosts"]
            boosts = [(str(b.split("^")[0][1:-1]), int(b.split("^")[1])) for b in boost_string.split(" ")]
            max_boost = max(b[1] for b in boosts)
            query = (query + " ") * max_boost
            for boost in boosts:
                query = query + " " + ((boost[0] + " ") * boost[1])
        if "index_time_boost" in search_args:
            query += " " + search_args["index_time_boost"][1]
        query = query.replace('"', '\\"')
        query = '"' + query + '"' #never null here, needs quotes for proper graphql rendering
        return query           

    def get_collection_field_names(self):
        response = requests.get(f"{WEAVIATE_URL}/v1/schema/{self.name}")
        if response.status_code == 200:
            fields = list(map(lambda p: p["name"], response.json()["properties"]))
            vector_field = SCHEMAS.get(self.name.lower(), {}).get("vector_field", None)
            if vector_field:
                fields.append(vector_field)
        else:
            raise NotFoundErr(f"Collection {self.name} not found")
        return fields
    
    def update_dataframe_with_missing_columns(self, dataframe):
        if self.name.lower() == "products_with_signals_boosts" and \
           "signals_boosts" not in dataframe.columns:
            dataframe = dataframe.withColumn("signals_boosts", lit(""))
        return dataframe

    def write(self, dataframe, overwrite=False): 
        opts = {#"batchSize": 500,
                "scheme": "http",
                "host": f"{WEAVIATE_HOST}:{WEAVIATE_PORT}",
                #"id": "id",
                "className": self.name}
        vector_field = SCHEMAS.get(self.name.lower(), {}).get("vector_field", None)
        if vector_field:
            opts["vector"] = vector_field
        mode = "overwrite" if overwrite else "append"
        dataframe = rename_id_field(dataframe)
        dataframe = self.update_dataframe_with_missing_columns(dataframe)
        dataframe.write.format("io.weaviate.spark.Weaviate").options(**opts).mode(mode).save(self.name)
        self.commit()
        print(f"Successfully written {dataframe.count()} documents")
    
    def add_documents(self, docs, commit=True):
        dataframe = get_spark_session().createDataFrame(Row(**d) for d in docs)
        self.write(dataframe, overwrite=False)

    #https://weaviate.io/developers/weaviate/search/basics
    #https://weaviate.io/developers/weaviate/search/bm25
    #https://graphql.org/learn/queries/
    #https://github.com/denisart/graphql-query
    #Rendering graphql strings with denisart's grahql library also requires:
    #  wrapping string values in double quotes
    #  encasing the rendered GQL query with an additional set of curly braces
    #  wrapping field values in quotes
    #score is handled as a different return field  
    #id is handled as a different field
    "https://weaviate.io/developers/weaviate/api/graphql/filters#where-filter"
    def transform_request(self, **search_args):
        limit = Argument(name="limit", value=search_args.get("limit", DEFAULT_SEARCH_SIZE))
        collection_query_args = [limit]
        if "after" in search_args: #cursor for reading all documents
            after_arg = Argument(name="after", value=f'"{search_args["after"]}"')
            collection_query_args.append(after_arg)        
        if is_vector_search(search_args):
            query_vector = f"[{','.join(map(str, search_args['query']))}]"
            query_arguments = [Argument(name="vector", value=query_vector)]
            collection_query_args.append(Argument(name="nearVector", value=query_arguments)) 
        elif is_bm25_query(search_args):
            query = self.generate_bm25_query(search_args)
            query_arguments = [Argument(name="query", value=query)]
            if "query_fields" in search_args:
                fields = self.generate_query_fields(search_args)
                query_arguments.append(Argument(name="properties", value=fields))
            collection_query_args.append(Argument(name="bm25", value=query_arguments)) 
        else: #unbound search
            pass

        filter_clause = generate_filter_clause(search_args)
        if filter_clause:
            collection_query_args.append(filter_clause)

        sort_clause = generate_sort_clause(search_args)
        if sort_clause:
            collection_query_args.append(sort_clause)

        return_fields = self.generate_return_fields(search_args)
        collection_query = Query(name=self.name, arguments=collection_query_args,
                                 fields=return_fields)
        root_operation = Operation(type="Get", queries=[collection_query])

        if "explain" in search_args:
            pass

        if "log" in search_args:
            print("Search Request:")
            print(root_operation.render())
        request = {"query": "{" + root_operation.render() + "}"}
        return request
    
    def transform_response(self, search_response):
        def format_doc(doc):
            additional = doc.pop("_additional", {})   
            if schema_contains_id_field(self.name):
                if "__id" in doc: # ID Hack 
                    if "id" in additional: 
                        doc["wv8_id"] = additional["id"]
                    doc["id"] = doc["__id"]
                    doc.pop("__id")
            elif "id" in additional:
                doc["id"] = additional["id"]
            if "score" in additional:
                doc["score"] = additional["score"]
            if schema_contains_custom_vector_field(self.name) and \
                "vector" in additional:
                doc[get_vector_field_name(self.name)] = additional["vector"]
                # Scale the score to equal the cosine similarity 
                if "distance" in additional and additional["distance"] is not None:
                    doc["score"] = 1 - additional["distance"]
            return doc
        
        if not search_response or "errors" in search_response:
            raise ValueError(search_response)
        response = {"docs": [format_doc(d)
                             for d in search_response["data"]["Get"][self.name]]}
        if "highlighting" in search_response:
            response["highlighting"] = search_response["highlighting"]
        return response
        
    def native_search(self, request=None, data=None):
        return requests.post(f"{WEAVIATE_URL}/v1/graphql", json=request).json()

    def search(self, **search_args):
        request = self.transform_request(**search_args)
        search_response = self.native_search(request=request)
        if "log" in search_args:
            print(search_response)
        return self.transform_response(search_response)
    
    #https://weaviate.io/developers/weaviate/modules/spellcheck
    def spell_check(self, query, log=False):
        #Needs the contextionary and text spell checker. Currently stubbed out
        return {'modes': 421, 'model': 159, 'modern': 139, 'modem': 56, 'mode6': 9}
        request = """{
  Get {
    %s(nearText: {
      concepts: ["%s"],
      autocorrect: true
    }) {
      %s
      _additional {
        spellCheck {
          changes {
            corrected
            original
          }
          didYouMean
          location
          originalText
        }
      }
    }
  }
}""" % (self.name, query, "_text_")