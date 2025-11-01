from aips.spark import get_spark_session
from pyspark.sql.functions import lit

def from_csv(file, additional_columns=False, drop_id=False, multiline_csv=False, log=True):    
    if log:
        print(f"Loading {file}")
    spark = get_spark_session()
    reader = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
    
    if multiline_csv:
        reader = reader.option("charset", "utf-8").option("quote", "\"") \
                       .option("escape", "\"").option("multiLine", "true").option("delimiter", ",")

    #this is hacky still, used by SKG collections to apply static category
    dataframe = reader.load(file)
    if additional_columns and "category" in additional_columns:
        dataframe = dataframe.withColumn("category", lit(additional_columns.get("category")))
    if drop_id:
        dataframe = dataframe.drop("id")    
    if log:
        print("Schema: ")
        dataframe.printSchema()

    return dataframe

def from_sql(query, spark=None):
    return (spark or get_spark_session()).sql(query)