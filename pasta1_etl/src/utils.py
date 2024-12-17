import boto3

def get_s3_client() -> boto3.client:
    """
        Create and return a boto3 client for S3

        Returns:
            boto3.client: A boto3 client for S3
    """
    try:
        s3_client = boto3.client('s3')
        return s3_client
    except Exception as e:
        err_msg = f"Error creating S3 client: {e}"
        raise err_msg



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