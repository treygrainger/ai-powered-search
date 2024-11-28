import requests
from engines.Collection import Collection
from engines.weaviate.config import WEAVIATE_HOST, WEAVIATE_PORT, WEAVIATE_URL
import time
import json
from pyspark.sql import SparkSession, Row
from graphql_query import Query, Argument, Field, Operation    
    
def generate_return_fields(search_args):
    if "return_fields" in search_args:
        return_fields = search_args["return_fields"]
        additional_fields = []
        if "score" in return_fields:
            return_fields.remove("score")
            additional_fields.append("_score")
        if "id" in return_fields:
            return_fields.remove("id")
            additional_fields.append("_id")
        if not additional_fields:
            return_fields.extend(additional_fields)
        return return_fields
    else:
        return [Field(name="_additional", fields=["id", "score"])]

class WeaviateCollection(Collection):
    def __init__(self, name):
        super().__init__(name)
        
    def commit(self):
        time.sleep(2)

    def write(self, dataframe, overwrite=True):
        opts = {#"batchSize": 500,
                "scheme": "http",
                "host": f"{WEAVIATE_HOST}:{WEAVIATE_PORT}",
                #"id": "id",
                "className": self.name} #"vector": "vector"
        mode = "overwrite" if overwrite else "append"
        dataframe.write.format("io.weaviate.spark.Weaviate").options(**opts).mode(mode).save(self.name)
        self.commit()
        print(f"Successfully written {dataframe.count()} documents")
    
    def add_documents(self, docs, commit=True):
        spark = SparkSession.builder.appName("AIPS").getOrCreate()
        dataframe = spark.createDataFrame(Row(**d) for d in docs)
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
        collection_query_args = [Argument(name="limit", value=search_args.get("limit", DEFAULT_SEARCH_SIZE))]
        
        if is_vector_search(search_args):
            pass
        elif "query" in search_args and search_args["query"] not in ["", "*"]:
            query = '"' + search_args["query"] + '"' #never null here
            query_arguments = [Argument(name="query", value=query)]
            if "query_fields" in search_args:
                fields = search_args["query_fields"]
                fields = list(map(lambda s: '"' + s + '"', fields)) # must pad all fields with double quotes
                query_arguments.append(Argument(name="properties", value=fields))
            collection_query_args.append(Argument(name="bm25", value=query_arguments)) 
        else: #unbound search    
            pass      
        return_fields = generate_return_fields(search_args)
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