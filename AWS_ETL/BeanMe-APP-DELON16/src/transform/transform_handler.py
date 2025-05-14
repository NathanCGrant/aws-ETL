import json
import logging
from typing import Dict, Any

# Import custom service modules
from services import (
    get_aws_clients,
    get_config,
    S3Service,
    RegistryService,
    TransformService,
    MessageHandler,
    GroupProcessor
)

# Set up basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Transform Handler")

# Initialise global resources and services
aws_clients = get_aws_clients()
config = get_config()
s3_client = aws_clients["s3"]

# Initialize services required for processing
s3_service = S3Service(s3_client, config["CLEAN_DATA_BUCKET"], logger)
registry_service = RegistryService(s3_service, config, logger)
transform_service = TransformService(registry_service, logger)
message_handler = MessageHandler(logger)
group_processor = GroupProcessor(registry_service, s3_service, transform_service, logger)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda function handler that processes incoming SQS messages, transforms
    the data, and stores it in S3.

    Args:
        event: Lambda event data containing SQS messages
        context: Lambda context

    Returns:
        API Gateway compatible response
    """
    try:
        logger.info("🟢 Transform Lambda triggered")

        # Extract and flatten SQS messages from the incoming event
        raw_records = event.get("Records", [])  # Fetch the "Records" from the event
        messages = message_handler.parse_sqs_messages(raw_records)  # Parse SQS messages

        logger.info(f"📨 Received {len(messages)} message(s) from SQS.")

        if not messages:
            logger.warning("⚠️ No messages to process.")
            return {"statusCode": 204, "body": "No content to process."}

        # Group messages by location and date for batch processing
        message_groups = message_handler.group_messages(messages)

        # Read central registries (locations and products) from S3
        location_registry = registry_service.read_all_locations()
        product_registry = registry_service.read_all_products()

        # Flags to track if updates are made to locations or products
        any_location_updated = False
        any_products_updated = False

        # Process each group of messages (grouped by date and location)
        for (date, location_folder), group_messages in message_groups.items():
            location_updated, products_updated = group_processor.process_message_group(
                date, location_folder, group_messages, location_registry, product_registry
            )
            # Track if any location or product data has been updated
            any_location_updated = any_location_updated or location_updated
            any_products_updated = any_products_updated or products_updated

        # If locations were updated, store the new location data to S3
        if any_location_updated:
            registry_service.update_locations(location_registry)

        # If products were updated, store the new product data to S3
        if any_products_updated:
            registry_service.update_products(product_registry["products"])

        logger.info("✅ All data processed and saved.")

        # Return a successful response with message count and group count
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"{len(messages)} message(s) processed and stored.",
                "groups_processed": len(message_groups)
            })
        }

    except Exception as e:
        
        # Log any exception that occurs during processing
        logger.error(f"❌ Exception in lambda_handler: {str(e)}", exc_info=True)
        
        # Return a response with error status if an exception occurs
        return {"statusCode": 500, "body": json.dumps({"message": "Internal server error"})}
