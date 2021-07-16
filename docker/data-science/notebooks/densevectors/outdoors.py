import pandas
import re
import html
import json
import pandas
from bs4 import BeautifulSoup

def stripHTML(h):
    h = html.unescape(h)
    soup = BeautifulSoup(h, 'lxml')
    text = soup.get_text(separator=' ')
    text = text.strip()
    text = re.sub(r'\s+',' ',text)
    return text

def splitTags(tags):
    if len(tags):
        return [t.replace('-',' ') for t in re.compile("[\<\>]").split(html.unescape(tags)) if len(t)]
    else:
        return None

def cleanDataset(filename):
    nullval = None
    df = pandas.read_csv(filename)
    df["title"] = df["title"].fillna('')
    df = df.fillna(0)
    df["id"] = pandas.to_numeric(df["id"], errors='coerce').astype(int).astype(str)
    df["post_type_id"] = pandas.to_numeric(df["post_type_id"], errors='coerce').astype(int)
    df["accepted_answer_id"] = pandas.to_numeric(df["accepted_answer_id"], errors='coerce').astype(int)
    df["parent_id"] = pandas.to_numeric(df["parent_id"], errors='coerce').astype(int)
    df["score"] = pandas.to_numeric(df["score"], errors='coerce').astype(int)
    df["view_count"] = pandas.to_numeric(df["view_count"], errors='coerce').astype(int)
    df["answer_count"] = pandas.to_numeric(df["answer_count"], errors='coerce').astype(int)
    df["title"] = df["title"].astype(str).apply(stripHTML)
    df["body"] = df["body"].astype(str).apply(stripHTML)
    df["tags"] = df["tags"].astype(str).apply(splitTags)
    df["owner_user_id"] = pandas.to_numeric(df["owner_user_id"], errors='coerce').astype(int)
    return df
    
def transformDataFrame(df):
    data = []
    nullval = None
    for idx,row in df.iterrows():
        data.append({
            "id": row["id"],
            "url": "https://outdoors.stackexchange.com/questions/" + str(row["id"]),
            "post_type_id" : (row["post_type_id"] if row["post_type_id"] else nullval),
            "accepted_answer_id" : (row["accepted_answer_id"] if row["accepted_answer_id"] else nullval),
            "parent_id" : (row["parent_id"] if row["parent_id"] else nullval),
            "score" : (row["score"] if row["score"] else nullval),
            "view_count" : (row["view_count"] if row["view_count"] else nullval),
            "body" : row["body"],
            "title" : row["title"],
            "tags" : row["tags"],
            "answer_count" : (row["answer_count"] if row["answer_count"] else nullval)
        })
    return data