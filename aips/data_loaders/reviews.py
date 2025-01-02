from aips.spark import get_spark_session
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import ArrayType, StringType, IntegerType, MapType, StructType, StructField, DoubleType
    
def generate_popularity_vector(stars):
    return [stars]

vector_udf = udf(generate_popularity_vector, ArrayType(IntegerType()))

def geo_coordinate(latlon):
    #return '{"latitude": -33.8688, "longitude": 151.2093}"'
    #return latlon
    return {"latitude": float(latlon.split(",")[0]),
            "longitude": float(latlon.split(",")[1])}

def geo_coordinateee(latlon):
    m = {"latitude": latlon.split(",")[0],
            "longitude": latlon.split(",")[1]}
    return json.dumps(m)

def geo_coordinatee(latlon):
    return {"latitude": latlon.split(",")[0],
            "longitude": latlon.split(",")[1]}

geo_udf = udf(geo_coordinate, MapType(StringType(), DoubleType()))
              #StructType([StructField("latitude", DoubleType()), StructField("longitude", DoubleType())]))
geo_udff = udf(geo_coordinate, StringType())

def load_dataframe(csv_file):
    print("\nLoading Reviews...")
    spark = get_spark_session()
    dataframe = spark.read.csv(csv_file, inferSchema=True, header=True, multiLine=True, escape="\"") \
        .select(col("id"), col("name_t").alias("business_name"),
                col("name_s").alias("name"),
                col("city_t").alias("city"),
                col("state_t").alias("state"), col("text_t").alias("content"),
                col("categories_t").alias("categories"), col("stars_i").alias("stars_rating"),
                col("location_pt_s"))
    dataframe = dataframe.withColumn("popularity_vector", vector_udf(col("stars_rating")))
    dataframe = dataframe.withColumn("location_coordinates", geo_udf(col("location_pt_s"))).drop("location_pt_s")
    dataframe.printSchema()
    dataframe = dataframe.filter(dataframe.business_name != "Charlotte Center City Partners")
    return dataframe