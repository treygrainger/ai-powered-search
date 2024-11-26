from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf

def load_dataframe(csv_file):
    print("Loading Products")
    spark = SparkSession.builder.appName("AIPS").getOrCreate()
    dataframe = spark.read.csv(csv_file, header=True, inferSchema=True)
    dataframe = dataframe.withColumn("upc", udf(str)(col("upc")))
    print("Schema: ")
    dataframe.printSchema()
    return dataframe