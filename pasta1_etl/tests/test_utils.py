import unittest
from unittest.mock import patch
from datetime import datetime
from moto import mock_aws
import boto3

from src.utils import (
    get_s3_client,
    list_s3_objects,
    list_parquet_files_in_bucket,
    save_file_content_in_s3,
    get_first_day_of_next_month,
    get_date_minus_tolerance,
)


@mock_aws
class TestUtils(unittest.TestCase):
    def test_get_s3_client(self):
        """
        Test client is successfully created
        """
        client = get_s3_client()
        self.assertIsInstance(client, boto3.client("s3").__class__.__base__)

    def test_get_s3_client_exception(self):
        """
        Test client is NOT successfully created
        """
        with patch("boto3.client") as mock_client:
            mock_client.side_effect = Exception("Error creating client")
            with self.assertRaises(Exception) as context:
                get_s3_client()
            self.assertEqual(str(context.exception), "Error creating client")

    def test_list_s3_objects_no_prefix(self):
        """
        Test listing objects in a bucket without a prefix
        """
        # Mock S3 bucket and objects
        bucket_name = "test-bucket"
        s3_client = boto3.client(
            "s3", endpoint_url="https://s3.amazonaws.com", region_name="us-east-1"
        )
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="file1.txt", Body="content")
        s3_client.put_object(Bucket=bucket_name, Key="folder/file2.txt", Body="content")

        # Test listing objects
        objects = list_s3_objects(bucket_name)
        self.assertEqual(objects, ["file1.txt", "folder/file2.txt"])

    def test_list_s3_objects_with_prefix(self):
        """
        Test listing objects in a bucket with a prefix
        """
        # Mock S3 bucket and objects
        bucket_name = "test-bucket"
        s3_client = boto3.client(
            "s3", endpoint_url="https://s3.amazonaws.com", region_name="us-east-1"
        )
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="file1-01.txt", Body="content")
        s3_client.put_object(
            Bucket=bucket_name, Key="folder/file1-03.txt", Body="content"
        )
        s3_client.put_object(Bucket=bucket_name, Key="folder/file2.txt", Body="content")

        # Test listing objects
        objects = list_s3_objects(bucket_name, prefix="file1")
        self.assertEqual(objects, ["file1-01.txt"])

    def test_list_parquet_files_in_bucket(self):
        """
        Test listing parquet files in a bucket filtering out non-parquet files
        """
        # Set up mock S3 bucket and objects
        bucket_name = "test-bucket"
        s3_client = boto3.client(
            "s3", endpoint_url="https://s3.amazonaws.com", region_name="us-east-1"
        )
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="file1.parquet", Body="content")
        s3_client.put_object(Bucket=bucket_name, Key="file2.txt", Body="content")

        # Test listing parquet files
        parquet_files = list_parquet_files_in_bucket(bucket_name)
        self.assertEqual(parquet_files, ["file1.parquet"])

    def test_save_file_content_in_s3(self):
        """
        Test saving content in an S3 bucket
        """
        # Set up mock S3 bucket
        bucket_name = "test-bucket"
        s3_client = boto3.client(
            "s3", endpoint_url="https://s3.amazonaws.com", region_name="us-east-1"
        )
        s3_client.create_bucket(Bucket=bucket_name)

        # Test saving content
        save_file_content_in_s3(bucket_name, "test-file.txt", "test-content")

        # Verify content
        response = s3_client.get_object(Bucket=bucket_name, Key="test-file.txt")
        self.assertEqual(response["Body"].read().decode(), "test-content")

    def test_get_first_day_of_next_month(self):
        """
        Test getting the first day of the next month
        """
        # Test case for December
        result = get_first_day_of_next_month("2023-12")
        self.assertEqual(result, datetime(2024, 1, 1))

        # Test case for a regular month
        result = get_first_day_of_next_month("2023-11")
        self.assertEqual(result, datetime(2023, 12, 1))

    def test_get_date_minus_tolerance(self):
        """
        Test getting the date minus the tolerance in hours
        """
        # Test tolerance calculation
        result = get_date_minus_tolerance("2023-11", 1)
        expected = datetime(2023, 10, 31, 23)
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
