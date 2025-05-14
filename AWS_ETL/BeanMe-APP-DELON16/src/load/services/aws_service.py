import os
import json
import boto3
import logging
import botocore.exceptions
from typing import Dict, Any

# Set up logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AWS Service")

def get_aws_clients():
    """
    Initialize and return AWS service clients with error handling.
    
    Returns:
        tuple: (s3_client, ssm_client) - Initialized AWS clients
    """
    try:
        s3_client = boto3.client("s3")
        ssm_client = boto3.client("ssm")
        logger.info("✅ Successfully initialized AWS clients.")
        return s3_client, ssm_client
    
    except botocore.exceptions.NoCredentialsError:
        logger.error("❌ AWS credentials not found.", exc_info=True)
        raise RuntimeError("AWS credentials not found.")
    
    except botocore.exceptions.PartialCredentialsError:
        logger.error("❌ Incomplete AWS credentials found.", exc_info=True)
        raise RuntimeError("Incomplete AWS credentials found.")
    
    except botocore.exceptions.NoRegionError:
        logger.error("❌ AWS region not specified.", exc_info=True)
        raise RuntimeError("AWS region not specified.")
    
    except Exception as e:
        logger.error(f"❌ Failed to initialize AWS clients: {e}", exc_info=True)
        raise RuntimeError(f"Failed to initialize AWS clients: {e}")


def get_config():
    """
    Return configuration from environment variables with error checking.
    
    Returns:
        dict: Configuration values from environment variables
    """
    try:
        return {
            "SSM_PARAMETER_NAME": os.environ.get("SSM_PARAMETER_NAME"),
            "CLEAN_DATA_BUCKET": os.environ["CLEAN_BUCKET"]
        }
    
    except KeyError as e:
        logger.error(f"❌ Missing required environment variable: {e}", exc_info=True)
        raise RuntimeError(f"Missing required environment variable: {e}")
    
    except Exception as e:
        logger.error(f"❌ Error loading configuration: {e}", exc_info=True)
        raise RuntimeError(f"Error loading configuration: {e}")


def get_redshift_config(param_name: str) -> Dict[str, Any]:
    """
    Retrieves the Redshift configuration from SSM Parameter Store.
    
    Args:
        param_name (str): The name of the SSM parameter containing the Redshift
                          configuration.
    
    Returns:
        dict: A dictionary containing the Redshift connection details.
    """
    # Log the action of fetching the Redshift configuration from SSM
    logger.info(f"🔐 Fetching Redshift config from SSM: {param_name}")
    
    try:
        # Initialise SSM client
        _, ssm_client = get_aws_clients()
        
        # Get the parameter from SSM with decryption
        response = ssm_client.get_parameter(Name=param_name, WithDecryption=True)
        
        # Parse the JSON value of the parameter into a Python dictionary
        config = json.loads(response["Parameter"]["Value"])
        
        # Log success message
        logger.info("✅ Successfully retrieved Redshift configuration.")
        
        # Return the configuration
        return config
    
    except Exception as e:
        # Log error if fetching the parameter fails
        logger.error(f"❌ Failed to retrieve Redshift config from SSM: {e}")
        
        # Raise the error so it can be handled upstream
        raise


def get_s3_object(bucket: str, key: str) -> Dict[str, Any]:
    """
    Retrieves an object from S3.
    
    Args:
        bucket (str): The S3 bucket name.
        key (str): The S3 object key (path).
    
    Returns:
        dict: The S3 object response.
    """
    # Log the action of retrieving the object from S3
    logger.info(f"📥 Retrieving object from S3: s3://{bucket}/{key}")
    
    try:
        # Initialize S3 client
        s3_client, _ = get_aws_clients()
        
        # Fetch the object from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        # Return the S3 response (contains the object data)
        return response
    
    except Exception as e:
        # Log error if fetching the object fails
        logger.error(f"❌ Failed to retrieve object from S3: {e}")
        
        # Raise the error so it can be handled upstream
        raise