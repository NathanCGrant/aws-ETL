import os
import json
import logging
import boto3
import botocore.exceptions

# Set up basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AWS Service")

def get_aws_clients():
    """Initialize and return AWS service clients with error handling."""
    try:
        ssm = boto3.client("ssm")
        logger.info("Successfully initialized AWS SSM client.")
        return ssm
    
    except botocore.exceptions.NoCredentialsError:
        logger.error("AWS credentials not found.", exc_info=True)
        raise RuntimeError("AWS credentials not found.")
    
    except botocore.exceptions.PartialCredentialsError:
        logger.error("Incomplete AWS credentials found.", exc_info=True)
        raise RuntimeError("Incomplete AWS credentials found.")
    
    except botocore.exceptions.NoRegionError:
        logger.error("AWS region not specified.", exc_info=True)
        raise RuntimeError("AWS region not specified.")
    
    except Exception as e:
        logger.error("Failed to initialize AWS clients.", exc_info=True)
        raise RuntimeError(f"Failed to initialize AWS clients: {e}")


def get_config():
    """Return configuration from environment variables with error checking."""
    try:
        return {
            "SSM_PARAMETER_NAME": os.environ.get("SSM_PARAMETER_NAME")
        }
    
    except KeyError as e:
        logger.error(f"❌ Missing required environment variable: {e}", exc_info=True)
        raise RuntimeError(f"Missing required environment variable: {e}")
    
    except Exception as e:
        logger.error(f"❌ Error loading configuration: {e}", exc_info=True)
        raise RuntimeError(f"Error loading configuration: {e}")


def get_parameter_from_ssm(param_name, with_decryption=True):
    """
    Retrieves a parameter from AWS SSM Parameter Store.

    Args:
        param_name (str): Name of the parameter to retrieve.
        with_decryption (bool): Whether to decrypt the parameter value.

    Returns:
        dict: Parsed JSON configuration from the parameter value.
    """
    # Initialize SSM client
    ssm = get_aws_clients()
    
    try:
        
        # Fetch the parameter value from SSM Parameter Store
        response = ssm.get_parameter(Name=param_name, WithDecryption=with_decryption)
        
        # Parse the JSON string stored in the parameter into a Python dictionary
        config = json.loads(response["Parameter"]["Value"])
        return config
    
    except Exception as e:
        
        # Raise any exceptions encountered during parameter retrieval
        raise e
