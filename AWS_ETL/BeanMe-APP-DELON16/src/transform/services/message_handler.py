import json
from typing import Dict, List, Tuple, Any

# Import custom model
from models.data_models import TransactionModel

class MessageHandler:
    """Handler for processing and grouping SQS messages received by the Lambda."""

    def __init__(self, logger):
        """
        Initializes the MessageHandler with a logger.

        Args:
            logger: A logger instance used for logging events and errors.
        """
        self.logger = logger

    def parse_sqs_messages(self, raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract and parse JSON messages from SQS event records.

        Args:
            raw_records: Raw SQS records from the Lambda event.

        Returns:
            A flat list of parsed message dictionaries.
        """
        messages = []

        for record in raw_records:
            body = record.get("body")  # Extract message body
            if not body:
                continue  # Skip records with no body

            try:
                parsed_body = json.loads(body)  # Parse JSON string

                # Handle two common formats: a list of messages, or a single message
                if isinstance(parsed_body, list):
                    messages.extend(parsed_body)  # Flatten the list
                elif isinstance(parsed_body, dict):
                    messages.append(parsed_body)  # Single message dictionary
                else:
                    self.logger.warning(f"⚠️ Unexpected message format: {type(parsed_body)}")

            except json.JSONDecodeError as e:
                # Log any JSON decoding issues
                self.logger.error(f"❌ JSON decode error: {e} — Body: {body}")

        return messages

    def group_messages(
            self,
            messages: List[Dict[str, Any]]
        ) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
        """
        Groups messages by transaction date and location for batch processing.

        Args:
            messages: List of parsed message dictionaries.

        Returns:
            A dictionary where each key is a (date, location_folder) tuple, and
            the value is a list of messages belonging to that group.
        """
        message_groups = {}

        for msg in messages:
            # Extract the date from the timestamp
            date, _ = TransactionModel.transform_timestamp(msg["transaction_timestamp"])

            # Normalize the location name into a safe folder format
            location_name = msg["location_name"]
            location_folder = location_name.replace(" ", "_")
            key = (date, location_folder)

            # Add message to the appropriate group
            if key not in message_groups:
                message_groups[key] = []
            message_groups[key].append(msg)

        self.logger.info(
            f"📦 Grouped messages into {len(message_groups)} location-date groups"
        )
        return message_groups
