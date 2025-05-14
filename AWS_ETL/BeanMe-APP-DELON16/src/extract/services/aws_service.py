import os
import logging
import boto3
import botocore.exceptions

# Set up basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AWS Service")

def get_aws_clients():
    """Initialize and return AWS service clients with error handling."""
    try:
        s3 = boto3.client("s3")
        sqs = boto3.client("sqs")
        logger.info("Successfully initialized AWS S3 and SQS clients.")
        return s3, sqs

    except botocore.exceptions.NoCredentialsError:
        logger.error("AWS credentials not found.", exc_info=True)
        raise RuntimeError("AWS credentials not found.")
    
    except botocore.exceptions.PartialCredentialsError:
        logger.error("Incomplete AWS credentials found.", exc_info=True)
        raise RuntimeError("Incomplete AWS credentials found.")
    
    except botocore.exceptions.NoRegionError:
        logger.error("AWS region not specified.", exc_info=True)
        raise RuntimeError("AWS region not specified. Please set AWS_REGION.")
    
    except Exception as e:
        logger.error("Failed to initialize AWS clients.", exc_info=True)
        raise RuntimeError(f"Failed to initialize AWS clients: {e}")


def get_config():
    """Return configuration from environment variables with error checking."""
    try:
        config = {
            "RAW_DATA_BUCKET": os.environ["RAW_BUCKET"],
            "TRANSFORM_QUEUE_URL": os.environ["TRANSFORM_QUEUE_URL"]
        }
        logger.info("Successfully loaded configuration from environment.")
        return config

    except KeyError as e:
        logger.error(f"❌ Missing required environment variable: {e}", exc_info=True)
        raise RuntimeError(f"Missing required environment variable: {e}")
    
    except Exception as e:
        logger.error(f"❌ Error loading configuration: {e}", exc_info=True)
        raise RuntimeError(f"Error loading configuration: {e}")