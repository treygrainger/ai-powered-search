import pandas
import re
import html
import json
import pandas
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession

def strip_HTML(h):
    h = html.unescape(h)
    soup = BeautifulSoup(h, 'lxml')
    text = soup.get_text(separator=' ')
    text = text.strip()
    text = re.sub(r'\s+',' ',text)
    return text

def split_tags(tags):
    if len(tags):
        return [t.replace('-', ' ') for t in re.compile("[\<\>]").split(html.unescape(tags)) if len(t)]
    else:
        return None

def load_outdoors_data(csv_filename):
    df = pandas.read_csv(csv_filename)
    
    df["owner_display_name"] = df["owner_display_name"].fillna("")
    df["title"] = df["title"].fillna("")
    df["creation_date"] = df["creation_date"].fillna("")
    df["closed_date"] = df["closed_date"].fillna("")
    df["community_owned_date"] = df["community_owned_date"].fillna("")
    df["last_edit_date"] = df["last_edit_date"].fillna("")
    df["last_activity_date"] = df["last_activity_date"].fillna("")
    df["last_editor_display_name"] = df["last_editor_display_name"].fillna("")
    df = df.fillna(0)
    df["id"] = pandas.to_numeric(df["id"], errors='coerce').astype(int).astype(str)
    df["url"] = df["id"].apply(lambda id: f"https://outdoors.stackexchange.com/questions/{id}")
    df["post_type_id"] = pandas.to_numeric(df["post_type_id"], errors='coerce').astype(int)
    df["parent_id"] = pandas.to_numeric(df["parent_id"], errors='coerce').astype(int)
    df["accepted_answer_id"] = pandas.to_numeric(df["accepted_answer_id"], errors='coerce').astype(int)
    df["score"] = pandas.to_numeric(df["score"], errors='coerce').astype(int)
    df["view_count"] = pandas.to_numeric(df["view_count"], errors='coerce').astype(int)
    df["answer_count"] = pandas.to_numeric(df["answer_count"], errors='coerce').astype(int)
    df["title"] = df["title"].astype(str).apply(strip_HTML)
    df["body"] = df["body"].astype(str).apply(strip_HTML)
    df["tags"] = df["tags"].astype(str).apply(split_tags)
    df["owner_user_id"] = pandas.to_numeric(df["owner_user_id"], errors='coerce').astype(int)   
    
    spark = SparkSession.builder.appName("AIPS").getOrCreate()
    return spark.createDataFrame(df) 