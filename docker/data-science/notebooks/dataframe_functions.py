from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def from_csv(file, more_opts=False):
    print(f"Loading file")
    spark = SparkSession.builder.appName("AIPS").getOrCreate()
    reader = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
    if more_opts:
        reader = reader.option("charset", "utf-8").option("quote", "\"").option("escape", "\"").option("multiLine","true").option("delimiter", ",")
    dataframe = reader.load(file)
    if more_opts and "category" in more_opts:
        # We can rely on automatic generation of IDs, or we can create them ourselves. 
        # If we do it, comment out previous line
        # .withColumn("id", concat(col("category"), lit("_") col("id")))
        dataframe = dataframe.withColumn("category", lit(more_opts.get("category"))).drop("id")
    print(f"Schema: ")
    dataframe.printSchema()
    return dataframe

def from_sql(query, spark=None):
    if not spark:
        spark = SparkSession.builder.appName("AIPS").getOrCreate()
    return spark.sql(query)