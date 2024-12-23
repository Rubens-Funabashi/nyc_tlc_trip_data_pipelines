from datetime import datetime, timedelta

import boto3

def get_s3_client() -> boto3.client:
    """
        Create and return a boto3 client for S3

        Returns:
            boto3.client: A boto3 client for S3
    """
    s3_client = boto3.client('s3')
    return s3_client


def list_s3_objects(bucket: str, prefix:str = '') -> list:
    """
        List all objects in a given S3 bucket with an optional prefix

        Parameters:
            bucket (str): The name of the S3 bucket to list the objects
            prefix (str): An optional prefix to filter the objects

        Returns:
            list: A list of objects in the given S3 bucket with the optional prefix
    """

    obj_key_list = []

    s3_client = get_s3_client()

    # The method list_objects_v2 has a limitation of 1000 objects per request
    # To overcome this limitation, we need to use the paginator method
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    # You could use a list comprehension, but, for better readability, we will use for loops
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                obj_key_list.append(obj['Key'])

    return obj_key_list


def list_parquet_files_in_bucket(bucket: str, prefix: str = '') -> list:
    """
        List all parquet files in a given S3 bucket with an optional prefix

        Parameters:
            bucket (str): The name of the S3 bucket to list the parquet files
            prefix (str): An optional prefix to filter the parquet files

        Returns:
            list: A sorted list of parquet files in the given S3 bucket with the optional prefix
    """

    parquet_files = []

    obj_list = list_s3_objects(bucket, prefix)

    for obj in obj_list:
        if obj.endswith('.parquet'):
            parquet_files.append(obj)

    return sorted(parquet_files)


def save_file_content_in_s3(bucket: str, path: str, file_content: str) -> None:
    """
        Save a string as a file in a given S3 bucket

        Parameters:
            bucket (str): The name of the S3 bucket to save the file
            path (str): The path to save the file in the S3 bucket
            file_content (str): The content of the file to be saved

        Returns:
            None
    """

    s3_client = get_s3_client()

    s3_client.put_object(Bucket=bucket, Key=path, Body=file_content)
    

def get_first_day_of_next_month(parquet_year_month: str) -> str:
    """
    Get the first day of the next month for the parquet file

    Parameters:
        parquet_year_month (str): The year and month of the parquet file in the format "YYYY-MM"

    Returns:
        str: The first day of the next month
    """

    parquet_date = datetime.strptime(f"{parquet_year_month}-01", "%Y-%m-%d")

    return datetime(parquet_date.year + 1, 1, 1) if parquet_date.month == 12 else datetime(parquet_date.year, parquet_date.month + 1, 1)


def get_date_minus_tolerance(parquet_year_month: str, pickup_date_tolerance_in_hours: int) -> str:
    """
    Get the date minus the tolerance in hours for the parquet file

    Parameters:
        parquet_year_month (str): The year and month of the parquet file in the format "YYYY-MM"
        pickup_date_tolerance_in_hours (int): The tolerance in hours to subtract from the date

    Returns:
        str: The date minus the tolerance in hours
    """

    parquet_start_date = datetime.strptime(f"{parquet_year_month}-01", "%Y-%m-%d")

    return parquet_start_date - timedelta(hours=pickup_date_tolerance_in_hours)