import unittest
from unittest.mock import MagicMock
from src.extract.services import SQSService

class TestSQSService(unittest.TestCase):
    def setUp(self):
        self.mock_sqs_client = MagicMock()
        self.mock_logger = MagicMock()
        self.queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"
        self.sqs_service = SQSService(self.mock_sqs_client, self.queue_url, self.mock_logger)

    def test_send_message_with_data(self):
        test_data = [{"id": "1", "name": "Alice"}]
        self.mock_sqs_client.send_message.return_value = {"MessageId": "abc123"}

        response = self.sqs_service.send_message(test_data)

        self.mock_logger.info.assert_any_call(
            f"🟢 Preparing to send data to SQS as a single message. Total rows: {len(test_data)}"
        )
        self.mock_sqs_client.send_message.assert_called_once()
        self.assertEqual(response["MessageId"], "abc123")

    def test_send_message_with_empty_data(self):
        response = self.sqs_service.send_message([])

        self.mock_logger.info.assert_called_with("ℹ️ No data to send to SQS, skipping message.")
        self.mock_sqs_client.send_message.assert_not_called()
        self.assertIsNone(response)

    def test_send_message_raises_exception(self):
        test_data = [{"id": "1", "name": "Error"}]
        self.mock_sqs_client.send_message.side_effect = Exception("SQS failure")

        with self.assertRaises(RuntimeError) as context:
            self.sqs_service.send_message(test_data)

        self.assertIn("Failed to send data to SQS", str(context.exception))
        self.mock_logger.error.assert_called()
        self.mock_sqs_client.send_message.assert_called_once()


if __name__ == "__main__":
    unittest.main()
