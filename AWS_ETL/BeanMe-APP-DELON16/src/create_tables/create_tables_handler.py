import json
import logging

# Import custom utility and service modules
from services import get_config, get_parameter_from_ssm
from utils import send_cfn_response, setup_redshift_schema

# Set up basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Create Tables Handler")

def lambda_handler(event, context):
    """
    Lambda function handler that:
      - Fetches Redshift config from AWS SSM Parameter Store
      - Sets up the Redshift schema (creates tables)
      - Sends a success/failure response back to CloudFormation
    """
    logger.info(f"📦 Lambda invoked with event: {json.dumps(event)}")
    
    try:
        # Check what kind of CloudFormation request this is
        request_type = event.get("RequestType")

        # If it"s a DELETE request, skip schema setup and just acknowledge success
        if request_type == "Delete":
            logger.info("🗑️ DELETE event received — skipping cleanup for now.")
            send_cfn_response(
                event, context, "SUCCESS", "Delete event handled successfully.", logger
            )
            return

        # Retrieve config from SSM parameter
        config = get_config()
        
        # Fetch the actual Redshift config (credentials and host info) from SSM
        redshift_config = get_parameter_from_ssm(config["SSM_PARAMETER_NAME"])
        logger.info("✅ Successfully fetched Redshift credentials.")

        # Create the required tables in Redshift
        setup_redshift_schema(redshift_config, logger)

        # Notify CloudFormation that the operation was successful
        send_cfn_response(event, context, "SUCCESS", "Tables created successfully.", logger)

    except Exception as e:
        # Log the exception and notify CloudFormation that the operation failed
        logger.error(f"❌ Error during Lambda execution: {e}")
        send_cfn_response(event, context, "FAILED", f"Error during Lambda execution: {e}", logger)
