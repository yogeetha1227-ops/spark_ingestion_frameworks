import json
from utils.spark_session import get_spark_session
from src.api_reader import fetch_api_data
from src.transformer import json_to_dataframe
from src.delta_writer import write_to_delta


def run_pipeline():

    spark = get_spark_session()

    with open("config/api_config.json") as f:
        config = json.load(f)

    api_url = config["api_url"]
    target_path = config["target_path"]

    api_data = fetch_api_data(api_url)

    df = json_to_dataframe(spark, api_data)

    write_to_delta(df, target_path)

    print("Pipeline completed successfully")