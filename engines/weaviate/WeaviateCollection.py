from xml.dom import NotFoundErr
import requests
from aips.data_loaders.reviews import transform_dataframe_for_weaviate
from aips import generate_fuzzy_text
from aips.spark import get_spark_session
from engines.Collection import Collection, is_vector_search, DEFAULT_SEARCH_SIZE, DEFAULT_NEIGHBORS
from engines.weaviate.config import WEAVIATE_HOST, WEAVIATE_PORT, WEAVIATE_URL, get_vector_field_name, \
    schema_contains_custom_vector_field, schema_contains_id_field, SCHEMAS, get_boost_field
import time
from pyspark.sql.functions import col, lit, udf, monotonically_increasing_id
from pyspark.sql import Row

from graphql_query import Query, Argument, Field, Operation    

class WeaviateCollection(Collection):
    def __init__(self, name):
        super().__init__(name)
        
    def commit(self):
        time.sleep(2)

    def get_engine_name(self):
        return "weaviate"
        
    def rename_id_field(self, dataframe):
        #Must exist because Weaviate reserves the 'id' field (and also the _id field??)
        if "id" in dataframe.columns:
            return dataframe.withColumnRenamed("id", "__id")
        return dataframe

    def is_query_by_id(self, search_args):
        #hacks for sort by not existing in bm25.
        #Will catch all queries that can be done without using BM25 handler
        return len(search_args.get("query_fields", [])) == 1 and \
            len(search_args.get("order_by", [])) == 1 and \
            search_args["order_by"][0][0] == "boost"

    def is_bm25_search(self, search_args):
        return "query" in search_args and \
            search_args["query"] not in ["", "*"] and \
            not self.is_query_by_id(search_args)

    def is_hybrid_search(self, search_args):
        #This ensures at least one vector and one lexical search
        return "query" in search_args and isinstance(search_args["query"], list)

    def generate_filter_clause(self, search_args):    
        def generate_filter_argument(name, operator, value):
            value_type = "valueText"
            if isinstance(value, bool):
                #value_type = "valueBoolean"
                value = str(value).lower()
            if isinstance(value, list):
                operator = "ContainsAny"
                value = "[" + ", ".join(['"' + v + '"' for v in value]) + "]"
            elif value == "*":
                value = 0
                operator = "NotEqual"
                value_type = "valueInt"
            elif isinstance(value, str):
                value = value if value[0] == '"' else '"' + value + '"'
            return [Argument(name="path", value=['"' + name + '"']),
                    Argument(name="operator", value=operator),
                    Argument(name=value_type, value=value)]
        filters = []
        filter_clause = None
        if self.is_query_by_id(search_args):
            id_filter = generate_filter_argument(search_args["query_fields"][0],
                                                 "Equal", search_args["query"])
            filters.append(id_filter)
        if "filters" in search_args and len(search_args["filters"]) > 0:
            for filter in search_args["filters"]:
                operator = "Equal" if filter[0][0] != "-" else "NotEqual"
                filter_arg = generate_filter_argument(filter[0].strip("-"),
                                                    operator, filter[1])
                filters.append(filter_arg)
        if "query" in search_args and isinstance(search_args["query"], list):
            for query_clause in search_args["query"]:
                if isinstance(query_clause, dict):
                    filter_clause = query_clause.get("filter", None)
                    if filter_clause:
                        filters.append(filter_clause)
        if len(filters) > 0:
            filter_clause = Argument(name="where", value=[
                Argument(name="operator", value="And"),
                Argument(name="operands", value=filters)])
        return filter_clause
        
    def generate_sort_clause(self, search_args):
        sorts = []
        sort_clause = None
        if not self.is_bm25_search(search_args): #sorting not implemented for bm25
            if "order_by" in search_args:
                #score is not a sort option?
                #skip sorting when first sorting by score
                if search_args["order_by"][0][0] != "score":
                    for item in search_args["order_by"]:
                        sorts.append([Argument(name="path", value='"' + item[0] + '"'),
                                      Argument(name="order", value=item[1])])
        if len(sorts) > 0:
            sort_clause = Argument(name="sort", value=sorts)
        return sort_clause

    def generate_return_fields(self, search_args):
        fields = []
        additional_fields = []
        if search_args.get("explain", False):
            additional_fields.append("explainScore")
        if "return_fields" in search_args:
            fields = search_args["return_fields"].copy()
            vector_field = get_vector_field_name(self.name)
            if vector_field and vector_field in fields:
                additional_fields.append("vector")
                fields.remove(vector_field)
                additional_fields.append("distance")
            if "score" in fields:
                fields.remove("score")
                additional_fields.append("score")
            if "__weaviate_id" in fields:
                fields.remove("__weaviate_id")
                additional_fields.append("id")
            if "id" in fields:
                fields.remove("id")
                if schema_contains_id_field(self.name): #Must appropriately apply id or __id for $WV8_001
                    fields.append("__id")
                else:
                    additional_fields.append("id")
        else:
            fields = self.get_collection_field_names()
            additional_fields = ["id", "score"]
        if additional_fields:
            fields.append(Field(name="_additional", fields=additional_fields))
        return fields
        
    def generate_query_fields(self, search_args):
        if "query_fields" in search_args:
            fields = search_args["query_fields"]
            if isinstance(fields, str):
                fields = [fields]
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
    
    def parse_query_functions(self, query):
        #'{!func}query("one") {!func}query("two") {!func}query("three")
        if query.find("{!func}") != -1:
            query = query.replace('{!func}query("', "").replace(')"', "")
        return query

    def generate_bm25_query(self, search_args):
        query = search_args["query"]
        query = self.parse_query_functions(query)
        if "query_boosts" in search_args:
            # Weaviate does not support query time boosting, to achieve this stuff the query with expanded boost terms
            # Parses a boost string into a data structure, supports ints and floats
            boost_string = search_args["query_boosts"][1] if isinstance(search_args["query_boosts"], tuple) else search_args["query_boosts"]
            boosts = [(str(b.split("^")[0][1:-1]),
                       int(float(b.split("^")[1]))) for b in boost_string.split(" ")]
            max_boost = max(b[1] for b in boosts)
            query = (query + " ") * max_boost
            for boost in boosts:
                quantity = int(boost[1] * 100 if isinstance(boost[1], float) else boost[1])
                query = query + " " + ((boost[0] + " ") * quantity)
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
    
    def apply_weaviate_specific_transformations(self, dataframe):
        #Weaviate needs data for all fields in the collection and
        # does not have support for copy fields
        if self.name.lower().find("products") != -1:
            dataframe = dataframe.withColumn("name_fuzzy", udf(generate_fuzzy_text)(col("name")))
            if "has_promotion" not in dataframe.columns:
                dataframe = dataframe.withColumn("has_promotion", lit("false"))
        if self.name.lower() == "reviews":
            dataframe = transform_dataframe_for_weaviate(dataframe)
        if self.name.lower() == "products_with_signals_boosts" and \
           "signals_boosts" not in dataframe.columns:
            dataframe = dataframe.withColumn("signals_boosts", lit(""))
        elif self.name.lower().find("signals") != -1 and \
            "id" not in dataframe.columns:
            dataframe = dataframe.withColumn("id", monotonically_increasing_id())
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
        dataframe = self.apply_weaviate_specific_transformations(dataframe)
        dataframe = self.rename_id_field(dataframe)
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
        elif self.is_hybrid_search(search_args):
            #"query" #to be used by bm25
                #"targetVectors": "named property vector",
                #alpha: 0.25
                #fusionType: relativeScoreFusion
                #properties: ["property^5"],
                #vector: [5]
            query_vector = list(filter(lambda qc: "vector_search" in qc, search_args["query"]))[0]["vector_search"]
            query_vector = query_vector[list(query_vector.keys())[0]]
            text_query = list(filter(lambda qc: isinstance(qc, str), search_args["query"]))[0]
            query_arg = Argument(name="query", value='"' + text_query.replace('"', "") + '"')
            vector_arg = Argument(name="vector", value=f"[{','.join(map(str, query_vector))}]")
            hybrid_args = [query_arg, vector_arg]
            collection_query_args.append(Argument(name="hybrid", value=hybrid_args))
        elif self.is_bm25_search(search_args):
            query = self.generate_bm25_query(search_args)
            query_arguments = [Argument(name="query", value=query)]
            if "query_fields" in search_args:
                fields = self.generate_query_fields(search_args)
                query_arguments.append(Argument(name="properties", value=fields))
            collection_query_args.append(Argument(name="bm25", value=query_arguments)) 
        else: #unbound search
            pass

        filter_clause = self.generate_filter_clause(search_args)
        if filter_clause:
            collection_query_args.append(filter_clause)

        sort_clause = self.generate_sort_clause(search_args)
        if sort_clause:
            collection_query_args.append(sort_clause)

        return_fields = self.generate_return_fields(search_args)
        collection_query = Query(name=self.name, arguments=collection_query_args,
                                 fields=return_fields)
        root_operation = Operation(type="Get", queries=[collection_query])

        if "log" in search_args:
            print("Search Request:")
            print(root_operation.render())
        request = {"query": "{" + root_operation.render() + "}"}
        return request
    
    def transform_response(self, search_response):
        def format_doc(doc):
            additional = doc.pop("_additional", {})   
            if "__id" in doc:
                doc["id"] = doc["__id"]
                doc.pop("__id")
            if "id" in additional:
                doc["__weaviate_id"] = additional["id"]
            if "score" in additional:
                doc["score"] = additional["score"]
            if "explainScore" in additional:
                doc["[explain]"] = additional["explainScore"]
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