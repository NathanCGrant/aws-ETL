import csv
import io
import json
from typing import Dict, List, Any

class S3Service:
    """Service for interacting with S3 storage."""

    def __init__(self, s3_client, clean_bucket: str, logger):
        """
        Initializes the S3Service.

        Args:
            s3_client: Boto3 S3 client instance.
            clean_bucket: The name of the S3 bucket to work with.
            logger: Logger instance for logging messages.
        """
        self.s3 = s3_client
        self.bucket = clean_bucket
        self.logger = logger

    def store_data(self, data: str, file_key: str) -> None:
        """
        Uploads string data to an S3 bucket under the given key.

        Args:
            data: The content to upload (CSV or JSON as a string).
            file_key: The destination key (path/filename) in the S3 bucket.

        Raises:
            Exception: If the upload fails.
        """
        try:
            self.s3.put_object(Bucket=self.bucket, Key=file_key, Body=data)
            self.logger.info(f"Uploaded {file_key} to s3://{self.bucket}")
        except Exception as e:
            self.logger.error(f"Error storing {file_key} in S3: {e}", exc_info=True)
            raise

    def read_csv(self, file_key: str) -> List[Dict[str, str]]:
        """
        Downloads and parses a CSV file from S3 into a list of dictionaries.

        Args:
            file_key: The key (path/filename) of the CSV file in the S3 bucket.

        Returns:
            A list of rows as dictionaries (column headers are the keys).

        Raises:
            s3.exceptions.NoSuchKey: If the file doesn't exist in S3.
        """
        response = self.s3.get_object(Bucket=self.bucket, Key=file_key)
        content = response["Body"].read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
        return list(reader)

    def read_json(self, file_key: str) -> Dict[str, Any]:
        """
        Downloads and parses a JSON file from S3.

        Args:
            file_key: The key (path/filename) of the JSON file in the S3 bucket.

        Returns:
            A dictionary containing the parsed JSON content.

        Raises:
            s3.exceptions.NoSuchKey: If the file doesn't exist in S3.
        """
        response = self.s3.get_object(Bucket=self.bucket, Key=file_key)
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)

    def file_exists(self, file_key: str) -> bool:
        """
        Checks whether a file exists in the S3 bucket.

        Args:
            file_key: The key (path/filename) to check.

        Returns:
            True if the file exists, False otherwise.
        """
        try:
            self.s3.head_object(Bucket=self.bucket, Key=file_key)
            return True
        except self.s3.exceptions.ClientError:
            return False
