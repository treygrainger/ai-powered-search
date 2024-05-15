from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf

def load_dataframe(csv_file):
    print("Loading Products")
    spark = SparkSession.builder.appName("AIPS").getOrCreate()
    dataframe = spark.read.csv(csv_file, header=True, inferSchema=True)
    remove_empty = udf(lambda s: s.replace("\\N", " "))
    dataframe = dataframe.withColumn("long_description", remove_empty(col("longDescription"))) 
    dataframe = dataframe.withColumn("short_description", remove_empty(col("shortDescription")))
    dataframe = dataframe.withColumn("manufacturer", remove_empty(col("manufacturer")))
    dataframe = dataframe.drop("longDescription", "shortDescription")
    print("Schema: ")
    dataframe.printSchema()
    return dataframe