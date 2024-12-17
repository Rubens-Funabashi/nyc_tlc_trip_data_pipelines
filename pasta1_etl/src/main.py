"""
    - Read parquet files from an input S3 bucket
    - For each parquet file, create a Delta Table on the output S3 bucket in a "bronze" layer/folder
    - For each Bronze Delta Table, clean the data and save it in a "silver" layer/folder
    - For each Silver Delta Table, aggregate the data and save it in a "gold" layer/folder
"""

# Standard library imports
import logging
import os

# Related third-party imports
from dotenv import load_dotenv # for local runs
from pyspark.sql import SparkSession

# Local application/library specific imports
from utils import list_parquet_files_in_bucket


load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')
SOURCE_S3_BUCKET = os.getenv("SOURCE_S3_BUCKET")

# DESTINATION_S3_BUCKET = os.getenv("DESTINATION_S3_BUCKET")


def create_bronze_layer():
    """ 
        Read the parquet files from the input bucket to create Delta Tables as the Bronze layer
    """

    parquet_objects = list_parquet_files_in_bucket(bucket=SOURCE_S3_BUCKET, prefix='yellow_tripdata')
    logging.info(f"Found {len(parquet_objects)} parquet files in the bucket {SOURCE_S3_BUCKET}: {parquet_objects}")

    for parquet_obj in parquet_objects:
        df = spark.read.parquet(f"s3a://{SOURCE_S3_BUCKET}/{parquet_obj}")



def main():
    """
        Main function
    """

    create_bronze_layer()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Set level for handlers
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create and set formatters
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    
    # Create a Spark session with Delta Lake support
    spark = (
        SparkSession.builder.appName("nyc_tlc_trip_etl")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.13:2.4.0",
        )
        .getOrCreate()
    )
    
    main()