# Standard library imports
import os

# Related third-party imports
from dotenv import load_dotenv  # for local runs
from pyspark.sql import SparkSession
from delta import *

load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
DESTINATION_S3_BUCKET = os.getenv("DESTINATION_S3_BUCKET")
FILE_PREFIX = os.getenv("FILE_PREFIX")

def main():
    # For each month, calculate the average total amount of the trips
    yearly_monthly_avg_total_amount = spark.sql(
        f"""
            SELECT pickup_year, pickup_month, AVG(Total_amount) AS avg_total_amount
            FROM delta.`s3a://{DESTINATION_S3_BUCKET}/{FILE_PREFIX}_gold`
            GROUP BY pickup_year, pickup_month
            ORDER BY pickup_year, pickup_month;
        """
    )
    yearly_monthly_avg_total_amount.write.format("csv").option("header", "true").mode("overwrite").save("s3a://{DESTINATION_S3_BUCKET}/analytics/avg_total_amount.csv")
    
    
    # For each day and hour, calculate the average passenger count
    # The column avg_passenger_count_per_day is the average passenger count for each day (which is why it is the same value for all the hours of the day)
    # The column avg_passenger_count_per_hour is the average passenger count for each hour of that day
    daily_hourly_avg_passenger_count_df = spark.sql(
        """
            WITH yellow_tripdata_window AS (
                SELECT
                    pickup_year,
                    pickup_month,
                    DAY(tpep_pickup_datetime) AS pickup_day,
                    HOUR(tpep_pickup_datetime) AS pickup_hour,
                    AVG(Passenger_count) OVER (PARTITION BY pickup_year, pickup_month, DAY(tpep_pickup_datetime)) AS avg_passenger_count_per_day,
                    AVG(Passenger_count) OVER (PARTITION BY pickup_year, pickup_month, DAY(tpep_pickup_datetime), HOUR(tpep_pickup_datetime)) AS avg_passenger_count_per_hour
                FROM delta.`s3a://{DESTINATION_S3_BUCKET}/{FILE_PREFIX}_gold`
            )
            SELECT *
            FROM yellow_tripdata_window
            GROUP BY pickup_year, pickup_month, pickup_day, pickup_hour, avg_passenger_count_per_day, avg_passenger_count_per_hour
            ORDER BY pickup_year, pickup_month, pickup_day, pickup_hour;
        """
    )
    daily_hourly_avg_passenger_count_df.write.format("csv").option("header", "true").mode("overwrite").save("s3a://{DESTINATION_S3_BUCKET}/analytics/avg_passenger_count.csv")


if __name__ == "__main__":
    # Create a Spark session with Delta Lake support
    builder = (
        SparkSession.builder.appName("delta_table_analytics")
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

    main()