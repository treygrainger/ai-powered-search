from pyspark.sql.functions import col, udf, lit
from aips.spark import get_spark_session

def load_dataframe(csv_file):
    print("Loading Products")
    spark = get_spark_session()
    dataframe = spark.read.csv(csv_file, header=True, inferSchema=True)
    dataframe = dataframe.withColumn("upc", udf(str)(col("upc")))
    print("Schema: ")
    dataframe.printSchema()
    return dataframe