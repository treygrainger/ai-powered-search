from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import concat_ws, lit
from aips.spark import get_spark_session

def load_dataframe(csv_file):
    print("Loading Geonames...")
    schema = StructType() \
          .add("id",StringType(),True) \
          .add("name",StringType(),True) \
          .add("ascii_name_s",StringType(),True) \
          .add("alternative_names_s",StringType(),True) \
          .add("latitude_s",StringType(),True) \
          .add("longitude_s",StringType(),True) \
          .add("feature_class_s",StringType(),True) \
          .add("feature_code_s",StringType(),True) \
          .add("country",StringType(),True) \
          .add("cc2_s",StringType(),True) \
          .add("admin_area",StringType(),True) \
          .add("admin_code_2_s",StringType(),True) \
          .add("admin_code_3_s",StringType(),True) \
          .add("admin_code_4_s",StringType(),True) \
          .add("popularity",IntegerType(),True) \
          .add("elevation_s",StringType(),True) \
          .add("dem_s",StringType(),True) \
          .add("timezone_s",StringType(),True) \
          .add("modification_date_s",StringType(),True)

    spark = get_spark_session()
    dataframe = spark.read.csv(csv_file, schema=schema, multiLine=True, escape="\\", sep="\t") \
        .withColumn("type", lit("city")) \
        .withColumn("location_coordinates", concat_ws(",", "latitude_s", "longitude_s"))

    return dataframe