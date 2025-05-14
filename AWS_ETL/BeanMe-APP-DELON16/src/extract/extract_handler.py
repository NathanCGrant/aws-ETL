import os
import logging

# Import custom service modules
from services import get_aws_clients, get_config, S3Service, SQSService
from utils import EventUtils

# Set up basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Extract Handler")

s3_client, sqs_client = get_aws_clients()
config = get_config()

# Get deduplication flag from config or environment variables
PERFORM_DEDUPLICATION = config.get(
    "PERFORM_DEDUPLICATION", 
    os.environ.get("PERFORM_DEDUPLICATION", "true").lower() == "true"
)

# Get processed directory path from config or set default
PROCESSED_DIR = config.get(
    "PROCESSED_DIR", 
    os.environ.get("PROCESSED_DIR", "processed/")
)

# Initialize services with required dependencies
s3_service = S3Service(s3_client, logger)
sqs_service = SQSService(sqs_client, config["TRANSFORM_QUEUE_URL"], logger)
event_utils = EventUtils(
    s3_service,
    sqs_service,
    config,
    logger,
    perform_deduplication=PERFORM_DEDUPLICATION
)


def should_process_file(key):
    """
    Check if a file should be processed by verifying it's not in the processed directory.
    
    Args:
        key (str): The S3 object key
        
    Returns:
        bool: True if the file should be processed, False otherwise
    """
    return PROCESSED_DIR not in key


def lambda_handler(event, context):
    """
    AWS Lambda function handler that processes incoming S3 events.
    It reads CSV files, sanitizes data, deduplicates records, and sends unique records
    to an SQS queue for downstream processing. Skips files in the processed directory.

    Args:
        event (dict): The event payload from S3 (via Lambda trigger).

    Returns:
        dict: HTTP-style response with status code and a message body.
    """
    try:
        # Log the beginning of Lambda execution
        logger.info("🟢 Lambda function started.")

        # Filter out events for files in the processed directory
        if 'Records' in event:
            original_records_count = len(event['Records'])
            filtered_records = []
            skipped_files = 0
            
            for record in event.get('Records', []):
                if record.get('eventName', '').startswith('ObjectCreated:'):
                    bucket = record.get('s3', {}).get('bucket', {}).get('name', '')
                    key = record.get('s3', {}).get('object', {}).get('key', '')
                    
                    if should_process_file(key):
                        filtered_records.append(record)
                    else:
                        skipped_files += 1
                        logger.info(f"Skipping file in processed directory: {key}")
                else:
                    # Keep non-ObjectCreated events
                    filtered_records.append(record)
            
            # Create a new event with filtered records
            filtered_event = event.copy()
            filtered_event['Records'] = filtered_records
            
            logger.info(
                f"Filtered {skipped_files} files in processed directory. Processing "
                f"{len(filtered_records)} out of {original_records_count} records."
            )
            
            # Skip processing if all records were filtered out
            if not filtered_records:
                return {
                    "statusCode": 200,
                    "body": f"No files to process"
                }
            
            # Delegate event processing to the EventProcessor class with filtered event
            processed_count, duplicate_count = event_utils.process_event(filtered_event)
        else:
            
            # If there are no Records in the event, process as normal
            processed_count, duplicate_count = event_utils.process_event(event)

        # Return success message with processing stats
        return {
            "statusCode": 200,
            "body": (
                f"CSV extraction complete. {processed_count} records sent to SQS, "
                f"{duplicate_count} duplicates skipped."
            )
        }

    except Exception as e:
        # Log any exceptions with traceback
        logger.error(f"❌ Lambda function failed: {e}", exc_info=True)

        # Return failure message
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }