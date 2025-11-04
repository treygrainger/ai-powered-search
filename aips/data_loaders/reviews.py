from aips.spark import get_spark_session
from pyspark.sql.functions import col

def load_dataframe(csv_file):
    print("\nLoading Reviews...")
    spark = get_spark_session()
    dataframe = spark.read.csv(csv_file, inferSchema=True, header=True, multiLine=True, escape="\"") \
        .select(col("id"), col("name_t").alias("business_name"),
                col("name_s").alias("name"),
                col("city_t").alias("city"),
                col("state_t").alias("state"), col("text_t").alias("content"),
                col("categories_t").alias("categories"), col("stars_i").alias("stars_rating"),
                col("location_pt_s").alias("location_coordinates"))
    dataframe.printSchema()
    dataframe = dataframe.filter(dataframe.business_name != "Charlotte Center City Partners")
    return dataframe