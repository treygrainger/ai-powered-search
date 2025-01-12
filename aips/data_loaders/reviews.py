from aips.spark import get_spark_session
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, DoubleType
    
    
def generate_popularity_vector(stars):
    return [stars]

def geo_coordinate(latlon):
    if not latlon:
        latlon = "0,0"
    return {"latitude": float(latlon.split(",")[0]),
            "longitude": float(latlon.split(",")[1])}

vector_udf = udf(generate_popularity_vector, ArrayType(IntegerType()))
geo_udf = udf(geo_coordinate, 
              StructType([StructField("latitude", DoubleType()),
                          StructField("longitude", DoubleType())]))

def transform_dataframe_for_weaviate(dataframe):
    return dataframe.withColumn("popularity_vector", vector_udf(col("stars_rating"))) \
                    .withColumn("location_coordinates", geo_udf(col("location_coordinates")))
    
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