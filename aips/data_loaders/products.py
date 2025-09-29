from aips.spark import get_spark_session
from pyspark.sql.functions import col, udf

promoted_products = [600603141003, 27242813908, 74108007469,
                     12505525766, 400192926087, 47875842328, 
                     803238004525, 27242799127, 27242815414,
                     97360724240, 97360722345, 826663114164]

def load_dataframe(csv_file, with_promotion=False):
    print("Loading Products")
    spark = get_spark_session()
    dataframe = spark.read.csv(csv_file, header=True, inferSchema=True)
    dataframe = dataframe.withColumn("upc", udf(str)(col("upc")))
    if with_promotion:
        promoted = [{"upc": promoted_upc, "has_promotion": True}
                    for promoted_upc in promoted_products]
        promoted_dataframe = spark.createDataFrame(promoted)
        dataframe = dataframe.join(promoted_dataframe, ["upc"], "left")
    print("Schema: ")
    dataframe.printSchema()
    return dataframe