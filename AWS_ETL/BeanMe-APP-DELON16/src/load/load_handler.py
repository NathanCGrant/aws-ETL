import json
import logging
from typing import Dict, Any

# Import service modules
from services import get_redshift_config, get_config, load_csv_to_redshift
from utils import determine_table_name

# Set up basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Load Handler")

# Get configuration from environment variables
config = get_config()
CLEAN_DATA_BUCKET = config["CLEAN_DATA_BUCKET"]
SSM_PARAMETER_NAME = config["SSM_PARAMETER_NAME"]

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda function to process S3 event notifications and load corresponding
    CSV files into Redshift.

    Args:
        event (dict): The event payload that includes S3 object details.
        context (object): AWS Lambda context object (unused here, but passed by AWS runtime).

    Returns:
        dict: An HTTP-style response with status code and result message.
    """
    try:
        logger.info("Starting Load Lambda")

        # Fetch Redshift connection details from AWS SSM Parameter Store
        logger.info("🔐 Fetching Redshift configuration from SSM Parameter Store")
        redshift_config = get_redshift_config(SSM_PARAMETER_NAME)

        files_processed = 0  # Counter to track how many files were successfully processed

        # Loop over each S3 event record
        for record in event["Records"]:
            s3_info = record["s3"]
            bucket_name = s3_info["bucket"]["name"]
            s3_key = s3_info["object"]["key"]

            logger.info(f"📦 Processing file: s3://{bucket_name}/{s3_key}")

            # Skip files that are not CSV or JSON (JSON might be needed for id_counters)
            if not s3_key.endswith(".csv") and not s3_key.endswith(".json"):
                logger.info(f"⏭️ Skipping non-data file: {s3_key}")
                continue

            # Use path-based logic to determine what Redshift table this file maps to
            table_name = determine_table_name(s3_key)
            logger.info(f"Determined table name: {table_name}")

            # If table mapping is not found, skip the file
            if not table_name:
                logger.info(f"⏭️ Skipping file that doesn't map to a table: {s3_key}")
                continue

            # Load the CSV file from S3 into the determined Redshift table
            logger.info(
                f"📦 Loading data from s3://{bucket_name}/{s3_key} into table {table_name}"
            )
            try:
                load_csv_to_redshift(s3_key, table_name, redshift_config, CLEAN_DATA_BUCKET)
                logger.info(
                    f"✅ Successfully loaded data from {s3_key} into table {table_name}"
                )
                files_processed += 1
            
            except Exception as load_error:
                logger.error(
                    f"❌ Error loading file {s3_key} into Redshift: {load_error}", exc_info=True
                )

        # Log completion of the Lambda function
        logger.info(f"Completed Load Lambda - {files_processed} files processed")

        # Return success response
        return {
            "statusCode": 200,
            "body": json.dumps(f"{files_processed} files successfully loaded into Redshift.")
        }

    except Exception as e:
        # Handle unexpected errors during execution
        logger.error(f"❌ Load Lambda failed with exception: {str(e)}", exc_info=True)

        # Return failure response
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }