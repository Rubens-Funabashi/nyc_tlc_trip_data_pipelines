"""
    - Read parquet files from an input S3 bucket
    - For each parquet file, create a Delta Table on the output S3 bucket in a "bronze" layer/folder
    - From the Bronze Delta Table, drop unnecessary columns, add partitioning columns and save it in a "gold" layer/folder

    ## Assumptions
    - It is being assumed that the parquet files are being consumed from an S3 bucket. 
        The team that manages that bucket is the same team responsible for creating the pipelines in the repo. 
        This is relevant because, if it was an external bucket or another team's bucket, 
        I would highly recommend making a copy of the parquet files to the output/destination bucket in this repository for auditing purposes.
    - It is being assumed that the parquet files have the naming convention "yellow_tripdata_YYYY-MM", 
        which is the same name as when you download the files from their website
    - The data in the parquet files is not limited to their corresponding month. 
        - For example, in `yellow_tripdata_2023-02.parquet`, you will find data where both 
            `tpep_pickup_datetime` and `tpep_dropoff_datetime` are before `2023-02-01`. 
            Some of them are from `2023-01-31` close to midnight **and are not in the January file**, 
            but some are very far in the past, dating to 2008. 
            On `.env` there is a parameter `PICKUP_DATE_TOLERANCE_IN_HOURS` that specifies the tolerance to be considered for the 
            `tpep_pickup_datetime` column. For example, if `PICKUP_DATE_TOLERANCE_IN_HOURS = 1`, 
            then for the parquet file `yellow_tripdata_2023-02.parquet` we will also include data from 
            `2022-01-31 23:00:00 onwards (2023-02-01 00:00:00 minus 1 hour)`. 
            **Any data before that will be filtered out and will be not included in the table.**
        - If you read the parquets from January to May, you will also find data from June, July, August and September. 
            For each parquet, data that exceeds the corresponding month will also be considered outliers and will be filtered out. 
            For example, if in `yellow_tripdata_2023-02.parquet` there is data from March, those will be removed.
    - Analyzing the timestamp columns, it seems that it is for the GMT timezone. I tried using NYC timezone, GMT-5, 
        but the timestamps did not seem correct. I contacted the data owners, but haven't heard back. I am assuming it's GMT.
    - I'm assuming the Hive instance is already created somewhere else and all that is needed is to provide the 
        .sql file with the query to create the tables.
"""

# Standard library imports
from datetime import datetime
import os

# Related third-party imports
from dotenv import load_dotenv  # for local runs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, day, hour
from pyspark.sql.types import (
    ByteType,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    LongType,
)
from delta import *

# Local application/library specific imports
from utils import (
    list_parquet_files_in_bucket,
    save_file_content_in_s3,
    get_date_minus_tolerance,
    get_first_day_of_next_month,
)

load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
SOURCE_S3_BUCKET = os.getenv("SOURCE_S3_BUCKET")
DESTINATION_S3_BUCKET = os.getenv("DESTINATION_S3_BUCKET")
START_DATE = os.getenv("START_DATE")
END_DATE = os.getenv("END_DATE")
FILE_PREFIX = os.getenv("FILE_PREFIX")
PICKUP_DATE_TOLERANCE_IN_HOURS = int(os.getenv("PICKUP_DATE_TOLERANCE_IN_HOURS"))

YELLOW_TRIP_SCHEMA = {
    f"{FILE_PREFIX}_bronze": {
        "columns": {
            "VendorID": ByteType(),
            "tpep_pickup_datetime": TimestampType(),
            "tpep_dropoff_datetime": TimestampType(),
            "Passenger_count": IntegerType(),
            "Trip_distance": DoubleType(),
            "PULocationID": LongType(),
            "DOLocationID": LongType(),
            "RateCodeID": DoubleType(),
            "Store_and_fwd_flag": StringType(),
            "Payment_type": ByteType(),
            "Fare_amount": DoubleType(),
            "Extra": DoubleType(),
            "MTA_tax": DoubleType(),
            "Improvement_surcharge": DoubleType(),
            "Tip_amount": DoubleType(),
            "Tolls_amount": DoubleType(),
            "Total_amount": DoubleType(),
            "congestion_Surcharge": DoubleType(),
            "Airport_fee": DoubleType(),
        },
        "partitioning": [],
    },
    f"{FILE_PREFIX}_gold": {
        "columns": {
            "VendorID": ByteType(),
            "tpep_pickup_datetime": TimestampType(),
            "tpep_dropoff_datetime": TimestampType(),
            "Passenger_count": IntegerType(),
            "Total_amount": DoubleType(),
            "pickup_year": IntegerType(),
            "pickup_month": IntegerType(),
        },
        "partitioning": ["pickup_year", "pickup_month"],
    },
}


def create_empty_delta_table(delta_path: list[str]) -> None:
    """
    Create an empty Delta Table for each path in the input list

    Parameters:
        delta_path (list[str]): List of destinations to create the Delta Table. Expected format: ["s3a://bucket_name/table_name"]

    Returns:
        None
    """

    for path in delta_path:
        table_name = path.split("/")[-1]

        dt1 = DeltaTable.create(spark).tableName(table_name)

        for column_name, column_type in YELLOW_TRIP_SCHEMA[table_name]["columns"].items():
            dt1.addColumn(column_name, column_type, True)

        if YELLOW_TRIP_SCHEMA[table_name]["partitioning"]:
            dt1.partitionedBy(*YELLOW_TRIP_SCHEMA[table_name]["partitioning"])

        dt1.location(path).execute()
        print(f">> Created {table_name} Delta Table at {path}")


def create_bronze_layer() -> tuple[list[str], list[str]]:
    """
    Read the parquet files from the input bucket to create Delta Tables as the Bronze layer

    Parameters:
        None

    Returns:
        tuple[list[str], list[str]]: A tuple with two lists. The first list contains the processed parquets and the second the failed ones
    """

    processed_parquets = []
    failed_parquets = []

    parquet_objects = list_parquet_files_in_bucket(
        bucket=SOURCE_S3_BUCKET, prefix=FILE_PREFIX
    )
    print(f">> Found {len(parquet_objects)} parquet files in the bucket {SOURCE_S3_BUCKET}: {parquet_objects}")

    for counter in range(0, len(parquet_objects)):
        try:
            parquet_year_month = (parquet_objects[counter].replace(f"{FILE_PREFIX}_","").replace(".parquet",""))  # Results in 'YYYY-MM'
            
            if parquet_year_month < START_DATE:
                print(f">> Skipping {parquet_objects[counter]} because {parquet_year_month} is before the start date {START_DATE}")
                continue
            elif parquet_year_month > END_DATE:
                print(f">> Skipping {parquet_objects[counter]} because {parquet_year_month} is after the end date {END_DATE}")
                continue

            print(f">> Processing {counter + 1} out of {len(parquet_objects)}: {parquet_objects[counter]}")
            df = spark.read.parquet(f"s3a://{SOURCE_S3_BUCKET}/{parquet_objects[counter]}")
            print(f"\t>> Parquet {parquet_objects[counter]} has {df.count()} rows")

            # Cast all columns to the specified types in the schema
            for column_name, column_type in YELLOW_TRIP_SCHEMA[f"{FILE_PREFIX}_bronze"]["columns"].items():
                df = df.withColumn(column_name, col(column_name).cast(column_type))
            print("\t>> Casted columns to the specified types")

            # Filter out rows with dates prior to {parquet_year_month} minus the tolerance
            # We are filtering out rows here to make sure we can correctly identify outliers
            # For example, if in the parquet file for 2023-02, we have a row with a date of 2023-01-31 22:00:00,
            #   we are able to identify that this row is an outlier
            # However, if we were to filter them out after appending all the parquet files,
            #   we wouldn't be able to identify if the row came from the 2023-01 parquet file or 2023-02
            date_minus_tolerance = get_date_minus_tolerance(parquet_year_month, PICKUP_DATE_TOLERANCE_IN_HOURS)
            print(f"\t>> Filtering out rows with dates prior to {date_minus_tolerance}")
            filtered_df = df.filter(col("tpep_pickup_datetime") >= date_minus_tolerance)

            next_month = get_first_day_of_next_month(parquet_year_month)
            print(f"\t>> Filtering out rows with dates after {next_month}")
            filtered_df = filtered_df.filter(col("tpep_pickup_datetime") < next_month)

            print(f"\t>> Rows after filtering: {filtered_df.count()}")

            print(f"\t>> Saving to {f's3a://{DESTINATION_S3_BUCKET}/{FILE_PREFIX}_bronze'}")
            filtered_df.write\
                        .format("delta")\
                        .mode("append")\
                        .save(f"s3a://{DESTINATION_S3_BUCKET}/{FILE_PREFIX}_bronze")

            processed_parquets.append(parquet_objects[counter])
        except Exception as e:
            failed_parquets.append(parquet_objects[counter])
            print(f"\t>> Error processing {parquet_objects[counter]}: {e}")

    return processed_parquets, failed_parquets


def create_gold_layer(bronze_delta_path: str) -> None:
    """
    Drop unnecessary columns from Delta Table adding partitioning columns

    Parameters:
        bronze_delta_path (str): The path to the Bronze Delta Table

    Returns:
        None
    """

    bronze_df = spark.read.format("delta").load(bronze_delta_path)
    print(f">> Bronze Delta table has {bronze_df.count()} rows")

    bronze_drop_columns_df = bronze_df.drop(
        *(
            YELLOW_TRIP_SCHEMA[f"{FILE_PREFIX}_bronze"]["columns"].keys()
            - YELLOW_TRIP_SCHEMA[f"{FILE_PREFIX}_gold"]["columns"].keys()
        )
    )

    # Add partitioning columns
    # Partitioning by year and month to improve query performance
    gold_df = bronze_drop_columns_df.withColumn("pickup_year", year("tpep_pickup_datetime"))\
                                    .withColumn("pickup_month", month("tpep_pickup_datetime"))
    

    print(f">> Gold Delta table has {gold_df.count()} rows")
    print(f">> Gold Delta Schema: {gold_df.schema}")
    print(f">> Saving to {f's3a://{DESTINATION_S3_BUCKET}/{FILE_PREFIX}_gold'}")

    gold_df.write\
            .format("delta")\
            .partitionBy(*YELLOW_TRIP_SCHEMA[f"{FILE_PREFIX}_gold"]["partitioning"])\
            .mode("append")\
            .save(f"s3a://{DESTINATION_S3_BUCKET}/{FILE_PREFIX}_gold")


def main():
    current_date = datetime.now().strftime("%Y_%m_%d")

    create_empty_delta_table(
        [
            f"s3a://{DESTINATION_S3_BUCKET}/{FILE_PREFIX}_bronze",
            f"s3a://{DESTINATION_S3_BUCKET}/{FILE_PREFIX}_gold",
        ]
    )

    processed_parquets, failed_parquets = create_bronze_layer()

    # Save a log file in s3 with the processed and failed parquets
    if processed_parquets:
        save_file_content_in_s3(
            bucket=DESTINATION_S3_BUCKET,
            path=f"logs/create_delta_table/processed_parquets/{current_date}/{FILE_PREFIX}_processed_parquets_{current_date}.txt",
            file_content="\n".join(processed_parquets),
        )

    if failed_parquets:
        save_file_content_in_s3(
            bucket=DESTINATION_S3_BUCKET,
            path=f"logs/create_delta_table/failed_parquets/{current_date}/{FILE_PREFIX}_failed_parquets_{current_date}.txt",
            file_content="\n".join(failed_parquets),
        )

    create_gold_layer(f"s3a://{DESTINATION_S3_BUCKET}/{FILE_PREFIX}_bronze")


if __name__ == "__main__":
    # Create a Spark session with Delta Lake support
    builder = (
        SparkSession.builder.appName("create_delta_table")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.2.2,io.delta:delta-core_2.12:1.0.0",
        )
    )
    
    my_packages = ["org.apache.hadoop:hadoop-aws:3.2.2,io.delta:delta-core_2.12:1.0.0"]
    spark = configure_spark_with_delta_pip(builder, my_packages).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        raise e
    finally:
        spark.stop()