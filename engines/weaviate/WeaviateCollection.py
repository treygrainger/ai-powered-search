from xml.dom import NotFoundErr
import requests
from aips.spark import get_spark_session
from engines.Collection import Collection, is_vector_search, DEFAULT_SEARCH_SIZE, DEFAULT_NEIGHBORS
from engines.weaviate.config import WEAVIATE_HOST, WEAVIATE_PORT, WEAVIATE_URL, get_vector_field_name, schema_contains_custom_vector_field, schema_contains_id_field, SCHEMAS
import time
import json
from pyspark.sql import Row
from graphql_query import Query, Argument, Field, Operation    

def rename_id_field(dataframe): #Hack $WV8_001
    #Must exist because Weaviate reserves the 'id' field (and also the _id field??)
    if "id" in dataframe.columns:
        return dataframe.withColumnRenamed("id", "__id")
    return dataframe

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
            fields = list(map(lambda s: '"' + s + '"', fields)) # must pad all fields with double quotes
            print(fields)
            return fields
        return None
    
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
        dataframe = rename_id_field(dataframe)  #.drop("id")
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
        #page = Argument(name="after", value=search_args.get("page", DEFAULT_SEARCH_SIZE))
        collection_query_args = [limit]
        if "after" in search_args: #cursor for reading all documents
            after_arg = Argument(name="after", value=f'"{search_args["after"]}"')
            collection_query_args.append(after_arg)        
        if is_vector_search(search_args):
            query_vector = f"[{','.join(map(str, search_args['query']))}]"
            query_arguments = [Argument(name="vector", value=query_vector)]
            collection_query_args.append(Argument(name="nearVector", value=query_arguments)) 
        elif "query" in search_args and search_args["query"] not in ["", "*"]:
            query = '"' + search_args["query"] + '"' #never null here, needs quotes for proper graphql rendering
            query_arguments = [Argument(name="query", value=query)]
            if "query_fields" in search_args:
                fields = self.generate_query_fields(search_args)
                query_arguments.append(Argument(name="properties", value=fields))
            collection_query_args.append(Argument(name="bm25", value=query_arguments)) 
        else: #unbound search
            pass      
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
        
        if log: print(json.dumps(request, indent=2))
        response = self.native_search(request=request)
        if log: print(json.dumps(response, indent=2))
        suggestions = {}
        if len(response["suggest"]["spell-check"]):
            suggestions = {term["text"]: term["freq"] for term
                            in response["suggest"]["spell-check"][0]["options"]}
        return suggestions