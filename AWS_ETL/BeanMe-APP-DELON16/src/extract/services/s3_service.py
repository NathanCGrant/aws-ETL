import csv
import io
import hashlib
import json
import os
from typing import List, Dict

class S3Service:
    def __init__(self, s3_client, logger):
        """
        Initialize the S3Service with an S3 client and a logger.

        Args:
            s3_client: An instance of boto3 S3 client for interacting with S3.
            logger: A logger instance for logging messages.
        """
        self.s3 = s3_client
        self.logger = logger
        self.last_duplicate_count = 0

        # Define expected CSV headers in correct order
        self.headers = [
            "transaction_timestamp", "location_name", "customer_name", "products",
            "transaction_total", "payment_method", "card_number"
        ]
        
    def _generate_record_hash(self, row_dict):
        """
        Generate a hash value for a record based on business keys.
        
        Args:
            row_dict (Dict): Dictionary representing a record
            
        Returns:
            str: MD5 hash of the record
        """
        
        # Adjust the fields used for hashing
        hash_input = (
            f"{row_dict['transaction_timestamp']}|"
            f"{row_dict['location_name']}|"
            f"{row_dict['customer_name']}|"
            f"{row_dict['products']}|"
            f"{row_dict['transaction_total']}"
        )
        
        return hashlib.md5(hash_input.encode('utf-8')).hexdigest()

    def _process_csv_content(self, content: str) -> List[Dict[str, str]]:
        """
        Converts raw CSV content into a list of dictionaries with pre-defined headers
        and adds a unique hash identifier for each record.
        """
        data = []

        # Create a CSV reader from the string content
        reader = csv.reader(io.StringIO(content))
        self.logger.info("🔄 Processing CSV content...")

        # Iterate over each row, ensuring it matches the expected header length
        for row_number, row in enumerate(reader, start=1):
            if len(row) != len(self.headers):
                
                # Warning and skip logic
                self.logger.warning(
                    f"⚠️ Skipping row {row_number}: expected {len(self.headers)} columns, "
                    f"got {len(row)}. Row data: {row}"
                )
                continue

            # Combine headers with row values into a dictionary
            row_dict = dict(zip(self.headers, row))
            
            # Generate and add the hash to the record
            row_dict['record_hash'] = self._generate_record_hash(row_dict)
            data.append(row_dict)

        return data
        
    def check_record_exists(self, bucket: str, record_hash: str) -> bool:
        """
        Check if a record with the given hash already exists in the hash registry.
        
        Args:
            bucket (str): The S3 bucket name
            record_hash (str): The hash value to check
            
        Returns:
            bool: True if the record exists, False otherwise
        """
        hash_registry_key = "hash_registry/record_hashes.json"
        
        try:
            # Try to get the existing hash registry
            response = self.s3.get_object(Bucket=bucket, Key=hash_registry_key)
            hash_registry = json.loads(response["Body"].read().decode("utf-8"))
            
            # Check if hash exists in registry
            return record_hash in hash_registry
            
        except self.s3.exceptions.NoSuchKey:
            # If the registry doesn't exist yet, create an empty one
            self.logger.info(f"🔄 Hash registry doesn't exist yet, creating new one")
            self.s3.put_object(
                Bucket=bucket,
                Key=hash_registry_key,
                Body=json.dumps([]).encode("utf-8"),
                ContentType="application/json"
            )
            return False
        
        except Exception as e:
            self.logger.error(f"❌ Error checking hash registry: {e}", exc_info=True)
            return False
            
    def update_hash_registry(self, bucket: str, new_hashes: List[str]) -> None:
        """
        Update the hash registry with new record hashes.
        
        Args:
            bucket (str): The S3 bucket name
            new_hashes (List[str]): List of new hashes to add
        """
        if not new_hashes:
            return
            
        hash_registry_key = "hash_registry/record_hashes.json"
        
        try:
            try:
                # Try to get the existing hash registry
                response = self.s3.get_object(Bucket=bucket, Key=hash_registry_key)
                hash_registry = json.loads(response["Body"].read().decode("utf-8"))
            except self.s3.exceptions.NoSuchKey:
                # Create a new registry if it doesn't exist
                hash_registry = []
            
            # Add new hashes to the registry
            hash_registry.extend(new_hashes)
            
            # Update the registry in S3
            self.s3.put_object(
                Bucket=bucket,
                Key=hash_registry_key,
                Body=json.dumps(hash_registry).encode("utf-8"),
                ContentType="application/json"
            )
            
            self.logger.info(f"✅ Added {len(new_hashes)} new hashes to registry")
            
        except Exception as e:
            self.logger.error(f"❌ Error updating hash registry: {e}", exc_info=True)
            raise

    def extract_csv(
            self,
            bucket: str, key: str,
            perform_deduplication: bool = True
        ) -> List[Dict[str, str]]:
        """
        Extract CSV data from an S3 object, process it, and return it as a list
        of dictionaries with duplicate records removed if needed.

        Args:
            bucket (str): The name of the S3 bucket.
            key (str): The S3 key (file path).
            perform_deduplication (bool): Whether to perform deduplication.

        Returns:
            List[Dict[str, str]]: List of unique rows as dictionaries.
        """
        data = []
        unique_data = []
        self.last_duplicate_count = 0

        try:
            self.logger.info(f"🟢 Attempting to fetch file from S3: s3://{bucket}/{key}")

            # Fetch the object from S3
            response = self.s3.get_object(Bucket=bucket, Key=key)
            body = response["Body"].read().decode("utf-8")
            self.logger.info(
                f"✅ Successfully fetched file from S3: {key}, File Size: {len(body)} bytes"
            )

            # Process CSV content into a list of dictionaries with hash values
            data = self._process_csv_content(body)
            
            # Perform deduplication if requested
            if perform_deduplication:
                new_hashes = []
                
                for record in data:
                    record_hash = record['record_hash']
                    
                    # Check if this record already exists
                    if not self.check_record_exists(bucket, record_hash):
                        
                        # If it's new, add to the list of new hashes and to unique data
                        new_hashes.append(record_hash)
                        unique_data.append(record)
                    else:
                        self.last_duplicate_count += 1
                        self.logger.info(f"🔄 Skipping duplicate record: {record_hash}")
                
                # Update the hash registry with all new hashes at once
                self.update_hash_registry(bucket, new_hashes)
                
                # Store the processed data (with hashes) back to S3
                # This creates a version with the hash included for tracking
                processed_key = f"processed/{os.path.basename(key)}"
                processed_content = self._convert_to_csv(unique_data)
                
                self.s3.put_object(
                    Bucket=bucket,
                    Key=processed_key,
                    Body=processed_content.encode("utf-8"),
                    ContentType="text/csv"
                )
                
                self.logger.info(
                    f"✔️ Stored processed data with hashes at s3://{bucket}/{processed_key}"
                )
                self.logger.info(
                    f"✔️ Completed processing file. Total rows: {len(data)}, Unique rows: "
                    f"{len(unique_data)}, Duplicates: {self.last_duplicate_count}"
                )
                
                return unique_data
            else:
                self.logger.info(
                    f"✔️ Completed processing file without deduplication. Total rows: {len(data)}"
                )
                return data

        except Exception as e:
            self.logger.error(f"❌ Error processing s3://{bucket}/{key}: {e}", exc_info=True)
            raise RuntimeError(f"Failed to extract CSV from S3: {e}")
            
    def _convert_to_csv(self, data: List[Dict[str, str]]) -> str:
        """
        Convert a list of dictionaries back to CSV format.
        
        Args:
            data (List[Dict[str, str]]): List of record dictionaries
            
        Returns:
            str: CSV formatted data
        """
        if not data:
            return ""
            
        # Get all headers including record_hash
        all_headers = self.headers + ["record_hash"]
        
        # Create a string buffer for the CSV output
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=all_headers)
        
        # Write header and rows
        writer.writeheader()
        writer.writerows(data)
        
        return output.getvalue()