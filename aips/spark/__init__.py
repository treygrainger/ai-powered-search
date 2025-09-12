from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType 
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import engines.opensearch.config as os_config
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType, ArrayType, StructField

def get_spark_session():
    conf = SparkConf()
    conf.set("spark.driver.memory", "7g")
    conf.set("spark.executor.memory", "7g")
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.ui.port", "4040")
    conf.set("spark.dynamicAllocation.executorMemoryOverhead", "7g")
    spark = SparkSession.builder.appName("AIPS").config(conf=conf).getOrCreate()
    return spark

def create_view_from_collection(collection, view_name, spark=None):
    if not spark:
        spark = get_spark_session()
    match collection.get_engine_name():
        case "solr":
            opts = {"zkhost": collection.zk_url, "collection": collection.name}    
            spark.read.format("solr").options(**opts).load().createOrReplaceTempView(view_name)
        case "opensearch":
            if collection.name == "tmdb_with_embeddings":
                return create_view_from_tmdb_embeddings_collection(collection, view_name, spark)            
            parse_id_udf = udf(lambda s: s["_id"], StringType())
            opts = {"opensearch.nodes": os_config.OPENSEARCH_URL,
                    "opensearch.net.ssl": "false"}
            if os_config.SCHEMAS.get(collection.name.lower(), {}).get("id_field", "") == "_id":
                opts["opensearch.read.metadata"] = "true"
            dataframe = spark.read.format("opensearch").options(**opts).load(collection.name)
            if "_metadata" in dataframe.columns and \
                os_config.SCHEMAS.get(collection.name.lower(), {}).get("id_field", "") == "_id":
                dataframe = dataframe.withColumn("id", parse_id_udf(col("_metadata")))
                dataframe = dataframe.drop("_metadata")
            dataframe.createOrReplaceTempView(view_name)
        case "weaviate":
            #Weaviate's current spark connector read functionality not yet implemented
            #Resort to batch paged reading
            fields = collection.get_collection_field_names()
            fields.append("__weaviate_id")            
            request = {"return_fields": fields,
                       "limit": 1000}
            all_documents = []
            while True:
                docs = collection.search(**request)["docs"]
                all_documents.extend(docs)
                if len(docs) != request["limit"]:
                    break
                last_doc = docs[request["limit"] - 1]
                cursor_id = last_doc["__weaviate_id"]
                request["after"] = cursor_id
                
            dataframe = spark.createDataFrame(data=all_documents)
            dataframe.createOrReplaceTempView(view_name)
        case _:
            raise NotImplementedError(type(collection))
            dataframe.createOrReplaceTempView(view_name)
        case _:
            raise NotImplementedError(type(collection))

#This work around exists solely for avoid the knn_vector embedding return bug which
#the following attempts failed to fix this issue:
#specifying a query for spark to execute which has image_embedding requested
#query = {"query": {"match_all": {}},
#         "_source": ["image_id", "movie_id", "title", "image_embedding"]}
#opts["opensearch.read.field.exclude"] = ""
#opts["opensearch.mapping.exclude"] = ""
#opts["opensearch.output.json"] = "true"
def create_view_from_tmdb_embeddings_collection(collection, view_name, spark):
    search_after = None
    documents = []
    while True:
        request = {"query": "*",
                   "return_fields": ["image_id", "movie_id", "title", "image_embedding"],
                   "limit": 500,
                   "sort": [("_id", "asc")]}
        if search_after:
            request["search_after"] = search_after
        response = collection.search(**request)
        if len(response["docs"]) != 500:
            break
        search_after = response["docs"][-1]["sort"]
        documents.extend(response["docs"])
    for d in documents:
        d["image_embedding"] = [f'{str(float(s))}' for s in d["image_embedding"]]
        
    schema = StructType([StructField("image_id", StringType()),
                         StructField("movie_id", StringType()),
                         StructField("title", StringType()),
                         StructField("image_embedding", ArrayType(StringType()))])
    dataframe = spark.createDataFrame(documents, schema=schema)
    dataframe.createOrReplaceTempView(view_name)
