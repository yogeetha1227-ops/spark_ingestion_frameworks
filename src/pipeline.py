import json
from pyspark.sql import SparkSession
from src.api_reader import fetch_api_data
from src.transformer import json_to_dataframe
from src.delta_writer import write_to_delta


def run_pipeline():

    # Create Spark session
    spark = SparkSession.builder.appName("API_Ingestion").getOrCreate()

    # Load config
    with open("config/config.json") as f:
        config = json.load(f)

    api_url = config["api_url"]
    target_path = config["target_path"]

    # Fetch API data
    api_data = fetch_api_data(api_url)

    # Convert JSON to DataFrame
    df = json_to_dataframe(spark, api_data)

    # Write to Delta Bronze table
    write_to_delta(df, target_path)

    print("Pipeline completed successfully")