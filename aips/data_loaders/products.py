from pyspark.sql import SparkSession

def load_dataframe(csv_file):
    print("Loading Products")
    spark = SparkSession.builder.appName("AIPS").getOrCreate()
    dataframe = spark.read.csv(csv_file, header=True, inferSchema=True)
    print("Schema: ")
    dataframe.printSchema()
    return dataframe