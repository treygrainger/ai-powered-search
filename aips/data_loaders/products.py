from pyspark.sql.functions import col, udf, lit
from aips.spark import get_spark_session

def load_dataframe(csv_file):
    print("Loading Products")
    spark = get_spark_session()
    dataframe = spark.read.csv(csv_file, header=True, inferSchema=True)
    dataframe = dataframe.withColumn("upc", udf(str)(col("upc")))
    dataframe = dataframe.withColumn("_text_", lit("stub"))
    dataframe = dataframe.withColumn("name_ngram", lit("stub"))
    dataframe = dataframe.withColumn("name_fuzzy", lit("stub"))
    dataframe = dataframe.withColumn("short_description_ngram", lit("stub"))
    dataframe = dataframe.withColumn("has_promotion", lit(False))
    print("Schema: ")
    dataframe.printSchema()
    return dataframe