import json
from typing import List, Dict

class SQSService:
    def __init__(self, sqs_client, queue_url, logger):
        """
        Initializes the SQSService with the required SQS client, queue URL, and logger.

        Args:
            sqs_client: An instance of boto3 SQS client used to interact with SQS.
            queue_url (str): The URL of the target SQS queue.
            logger: A logger instance for logging messages.
        """
        self.sqs = sqs_client
        self.queue_url = queue_url
        self.logger = logger

    def send_message(self, data: List[Dict[str, str]]):
        """
        Sends the processed data as a single message to the SQS queue.

        Args:
            data (List[Dict[str, str]]): A list of dictionaries representing the data to send.
        """
        try:
            # Skip sending if the data is empty
            if not data:
                self.logger.info("ℹ️ No data to send to SQS, skipping message.")
                return None
                
            self.logger.info(
                f"🟢 Preparing to send data to SQS as a single message. Total rows: {len(data)}"
            )

            # Convert the list of dictionaries into a JSON-formatted string
            message_body = json.dumps(data)

            # Send the message to the SQS queue
            response = self.sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody=message_body
            )

            # Log success and message ID returned by SQS
            self.logger.info(f"📩 Sent message to SQS: MessageId={response['MessageId']}")
            return response

        except Exception as e:
            self.logger.error(f"❌ Failed to send message to SQS: {e}", exc_info=True)
            raise RuntimeError(f"Failed to send data to SQS: {e}")