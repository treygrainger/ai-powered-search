from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def from_csv(file, additional_columns=False, drop_id=False, log=True):    
    if log:
        print(f"Loading {file}")
    spark = SparkSession.builder.appName("AIPS").getOrCreate()
    reader = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
    dataframe = reader.load(file)
     #this is hacky still, used by SKG collections to apply static category
    if additional_columns and "category" in additional_columns:
        dataframe = dataframe.withColumn("category", lit(additional_columns.get("category")))
    if drop_id:
        dataframe = dataframe.drop("id")    
    if log:
        print("Schema: ")
        dataframe.printSchema()

    return dataframe

def from_sql(query, spark=None):
    if not spark:
        spark = SparkSession.builder.appName("AIPS").getOrCreate()
    return spark.sql(query)