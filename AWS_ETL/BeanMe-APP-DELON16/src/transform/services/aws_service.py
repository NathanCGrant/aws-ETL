import boto3
import os
import logging
import botocore.exceptions

# Set up basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AWS Service")

def get_aws_clients():
    """
    Initialize and return AWS service clients with error handling.
    
    Returns:
        dict: Dictionary containing initialized AWS clients
    """
    try:
        logger.info("🔄 Initializing AWS service clients...")
        s3 = boto3.client("s3")
        logger.info("✅ Successfully initialized AWS S3 client")
        return {"s3": s3}
    
    except botocore.exceptions.NoCredentialsError:
        logger.error("❌ AWS credentials not found", exc_info=True)
        raise RuntimeError("AWS credentials not found")
    
    except botocore.exceptions.PartialCredentialsError:
        logger.error("❌ Incomplete AWS credentials found", exc_info=True)
        raise RuntimeError("Incomplete AWS credentials found")
    
    except botocore.exceptions.NoRegionError:
        logger.error("❌ AWS region not specified", exc_info=True)
        raise RuntimeError("AWS region not specified")
    
    except Exception as e:
        logger.error(f"❌ Failed to initialize AWS clients: {e}", exc_info=True)
        raise RuntimeError(f"Failed to initialize AWS clients: {e}")


def get_config():
    """
    Return configuration from environment variables with error handling.
    
    Returns:
        dict: Configuration values from environment variables
    """
    logger.info("🔄 Loading configuration from environment variables...")
    
    try:
        config = {
            "CLEAN_DATA_BUCKET": os.environ["CLEAN_BUCKET"],
            "COUNTER_FILE_KEY": "metadata/id_counters.json",
            "LOCATIONS_FILE_KEY": "locations/locations.csv",
            "PRODUCTS_FILE_KEY": "products/products.csv"
        }
        
        logger.info(f"✅ Configuration loaded successfully.")
        return config
        
    except KeyError as e:
        logger.error(f"❌ Missing required environment variable: {e}", exc_info=True)
        raise RuntimeError(f"Missing required environment variable: {e}")
    
    except Exception as e:
        logger.error(f"❌ Error loading configuration: {e}", exc_info=True)
        raise RuntimeError(f"Error loading configuration: {e}")
