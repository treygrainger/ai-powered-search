import random
import requests
from engines.Collection import Collection
from engines.weaviate.config import WEAVIATE_URL
import time
import json
from pyspark.sql import SparkSession, Row
from graphql_query import Query, Argument, Field, Operation

class WeaviateCollection(Collection):
    def __init__(self, name):
        super().__init__(name)
        
    def commit(self):
        time.sleep(2)

    def write(self, dataframe, overwrite=True):
        opts = {"batchSize": 500,
                "scheme": "http",
                "host": "localhost:8080",
                "id": "uuid",
                "className": self.name,
                "vector": "vector"}
        if self.id_field != "_id":
            opts["opensearch.mapping.id"] = self.id_field
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
    def transform_request(self, **search_args):
        query = "some query"
        args = [Argument(name="limit", value=10),
                Argument(name="bm25", value=[Argument(name="query", value=query)])]
        score = Field(name="_additional", fields=["score"])
        request = Operation(name="Get", queries=[Query(name="products", arguments=args, 
                                                    fields=["id", "title", "description", score])],
                        )
        print(request.render())
        request = {}
        for name, value in search_args.items():
            match name:
                case "query":
                case _:
                    pass

        if "log" in search_args:
            print("Search Request:")
            print(json.dumps(request, indent="  "))
        return request
    
    def transform_response(self, search_response):
        def format_doc(doc):
            return doc
            
        if not search_response:
            raise ValueError(search_response)
        response = {"docs": [format_doc(d)
                             for d in search_response["hits"]["hits"]]}
        if "highlighting" in search_response:
            response["highlighting"] = search_response["highlighting"]
        return response
        
    def native_search(self, request=None, data=None):
        return requests.post(f"{WEAVIATE_URL}/{self.name}/_search", json=request, data=data).json()

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