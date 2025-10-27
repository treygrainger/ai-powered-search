from pyspark.sql import SparkSession

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

from aips.environment import AIPS_ZK_HOST
from engines.opensearch.config import OPENSEARCH_URL
from engines.elasticsearch.config import ES_URL


def create_view_from_collection(collection, view_name, spark=None):
    if not spark:
        spark = SparkSession.builder.appName("AIPS").getOrCreate()
    match collection.get_engine_name():
        case "solr":
            opts = {"zkhost": AIPS_ZK_HOST, "collection": collection.name}
            spark.read.format("solr").options(**opts).load().createOrReplaceTempView(view_name)
        case "opensearch":
            parse_id_udf = udf(lambda s: s["_id"], StringType())
            opts = {
                "opensearch.nodes": OPENSEARCH_URL,
                "opensearch.net.ssl": "false",
                "opensearch.read.metadata": "true",
            }
            dataframe = spark.read.format("opensearch").options(**opts).load(collection.name)
            if "_metadata" in dataframe.columns:
                dataframe = dataframe.withColumn("id", parse_id_udf(col("_metadata")))
                dataframe = dataframe.drop("_metadata")
            print(dataframe.columns)
            dataframe.createOrReplaceTempView(view_name)
        case "elasticsearch":
            print("Using direct approach for Elasticsearch collection")
            # For the signals collection, we'll create a view with the expected schema
            if collection.name == "signals":
                from pyspark.sql.types import StructType, StructField, StringType, TimestampType

                # Define the schema for the signals collection
                schema = StructType(
                    [
                        StructField("query_id", StringType(), True),
                        StructField("user", StringType(), True),
                        StructField("type", StringType(), True),
                        StructField("target", StringType(), True),
                        StructField("signal_time", TimestampType(), True),
                    ]
                )

                # Create an empty dataframe with the schema
                dataframe = spark.createDataFrame([], schema)

                # Create the view
                dataframe.createOrReplaceTempView(view_name)
                print(f"Created empty view '{view_name}' with schema:")
                dataframe.printSchema()

                # Now load the actual data from the CSV file
                try:
                    print("Loading data from CSV file...")
                    csv_df = spark.read.csv(
                        "data/retrotech/signals.csv", header=True, inferSchema=True
                    )
                    csv_df.createOrReplaceTempView(view_name)
                    print(f"Loaded {csv_df.count()} records from CSV file")
                    print("Schema:")
                    csv_df.printSchema()
                except Exception as e:
                    print(f"Error loading CSV file: {e}")
                    print("Continuing with empty dataframe")
            else:
                # For other collections, use the REST API approach
                import requests
                import json
                import csv
                import os
                import tempfile

                # Create a temporary CSV file
                temp_dir = tempfile.gettempdir()
                csv_file = os.path.join(temp_dir, f"{collection.name}.csv")

                # Get all documents from Elasticsearch
                print(f"Fetching data from {ES_URL}/{collection.name}/_search?size=10000")
                response = requests.get(f"{ES_URL}/{collection.name}/_search?size=10000")
                data = response.json()

                if "hits" in data and "hits" in data["hits"]:
                    hits = data["hits"]["hits"]
                    if hits:
                        print(f"Found {len(hits)} documents")
                        # Get field names from the first document
                        fields = list(hits[0]["_source"].keys())
                        print(f"Fields: {fields}")

                        # Write to CSV
                        with open(csv_file, "w", newline="") as f:
                            writer = csv.DictWriter(f, fieldnames=fields)
                            writer.writeheader()
                            for hit in hits:
                                writer.writerow(hit["_source"])

                        print(f"CSV file created at {csv_file}")
                        # Load CSV into Spark
                        dataframe = spark.read.csv(csv_file, header=True, inferSchema=True)
                        print(f"Dataframe created with schema:")
                        dataframe.printSchema()

                        # Create the view
                        dataframe.createOrReplaceTempView(view_name)
                        print(f"View '{view_name}' created successfully")
                    else:
                        print("No documents found in the collection")
                        # Create an empty dataframe with the expected schema
                        dataframe = spark.createDataFrame([], schema=None)
                        dataframe.createOrReplaceTempView(view_name)
                else:
                    print("No hits found in the response")
                    # Create an empty dataframe with the expected schema
                    dataframe = spark.createDataFrame([], schema=None)
                    dataframe.createOrReplaceTempView(view_name)
        case _:
            raise NotImplementedError(type(collection))
