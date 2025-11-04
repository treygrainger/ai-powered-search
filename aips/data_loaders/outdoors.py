from aips.spark import get_spark_session
from pyspark.sql.functions import coalesce, col, lit, udf
from pyspark.sql.types import ArrayType, StringType 
import html
import re

from bs4 import BeautifulSoup

def load_dataframe(csv_file):
    def split_tags(ascii_html):
        tags = re.compile("[\<\>]").split(html.unescape(ascii_html or ""))
        return [t.replace("-", " ") for t in tags if len(t)]
    
    def strip_HTML(ascii_html):
        text = html.unescape(ascii_html or "")
        text = BeautifulSoup(text, "lxml").get_text(separator=" ")
        return re.sub(r"\s+", " ", text.strip())
    
    split_tags_udf = udf(split_tags, ArrayType(StringType()))
    strip_html_udf = udf(strip_HTML)
    generate_url_udf = udf(lambda id: f"https://outdoors.stackexchange.com/questions/{id}", StringType())
    post_type_udf = udf(lambda type_id: "question" if type_id == 1 else "answer", StringType())

    spark = get_spark_session()
    dataframe = spark.read.csv(csv_file, header=True, inferSchema=True)
    dataframe = dataframe.filter((dataframe.post_type_id == 1) | (dataframe.post_type_id == 2))
    dataframe = dataframe.withColumn("post_type", post_type_udf(col("post_type_id")))
    dataframe = dataframe.withColumn("view_count", coalesce(col("view_count"), lit(0))) 
    dataframe = dataframe.withColumn("body", strip_html_udf(col("body"))) 
    dataframe = dataframe.withColumn("owner_user_id", coalesce(col("owner_user_id"), col("owner_display_name"))) 
    dataframe = dataframe.withColumn("title", strip_html_udf(col("title"))) 
    dataframe = dataframe.withColumn("tags", split_tags_udf(col("tags"))) 
    dataframe = dataframe.withColumn("url", generate_url_udf(col("id"))) 
    dataframe = dataframe.drop("post_type_id", "deletion_date", "owner_display_name", "last_editor_user_id",
                               "last_editor_display_name", "last_edit_date", "last_activity_date", "comment_count",
                               "favorite_count", "closed_date", "community_owned_date")
    return dataframe