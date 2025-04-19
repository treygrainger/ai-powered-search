from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType 
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

def get_spark_session():
    conf = SparkConf()
    conf.set("spark.driver.memory", "8g")
    conf.set("spark.executor.memory", "8g")
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.ui.port", "4040")
    conf.set("spark.dynamicAllocation.executorMemoryOverhead", "8g")
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
            parse_id_udf = udf(lambda s: s["_id"], StringType())
            is_ssl_connection = "https" in collection.os_url
            opts = {"opensearch.nodes": collection.os_url,
                    "opensearch.net.ssl": str(is_ssl_connection).lower(),
                    "opensearch.read.metadata": "true"}
            if is_ssl_connection:
                opts |= {"opensearch.net.http.auth.user": collection.__access_key,
                         "opensearch.net.http.auth.pass": collection.__secret_key,
                         "opensearch.net.ssl.cert.allow.self.signed": "true"}
            dataframe = spark.read.format("opensearch").options(**opts).load(collection.name)
            if "_metadata" in dataframe.columns:
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
