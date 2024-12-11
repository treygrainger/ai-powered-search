from aips.spark import get_spark_session
from pyspark.sql.functions import lit

def from_csv(file, more_opts=False, log=True):    
    if log:
        print(f"Loading {file}")
    spark = get_spark_session()
    reader = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
    if more_opts:
        reader = reader.option("charset", "utf-8").option("quote", "\"").option("escape", "\"").option("multiLine","true").option("delimiter", ",")
    dataframe = reader.load(file)
    if more_opts and "category" in more_opts:
        # We can rely on automatic generation of IDs, or we can create them ourselves. 
        # If we do it, comment out previous line
        # .withColumn("id", concat(col("category"), lit("_") col("id")))
        dataframe = dataframe.withColumn("category", lit(more_opts.get("category"))).drop("id")
    
    if log:
        print("Schema: ")
        dataframe.printSchema()

    return dataframe

def from_sql(query, spark=None):
    if not spark:
        spark = get_spark_session()
    return spark.sql(query)