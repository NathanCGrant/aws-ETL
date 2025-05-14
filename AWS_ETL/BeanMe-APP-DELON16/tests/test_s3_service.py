import unittest
from unittest.mock import MagicMock, patch
from src.extract.services import S3Service

class TestS3Service(unittest.TestCase):

    def setUp(self):
        self.mock_s3 = MagicMock()
        self.mock_logger = MagicMock()
        self.service = S3Service(self.mock_s3, self.mock_logger)

    def test_generate_record_hash_consistency(self):
        row = {
            "transaction_timestamp": "2024-01-01T12:00:00",
            "location_name": "Store A",
            "customer_name": "Alice",
            "products": "Item1,Item2",
            "transaction_total": "25.50",
            "payment_method": "card",
            "card_number": "1234567890123456"
        }
        hash1 = self.service._generate_record_hash(row)
        hash2 = self.service._generate_record_hash(row)
        self.assertEqual(hash1, hash2)

    def test_process_csv_content_valid(self):
        csv_content = (
            "2024-01-01T12:00:00,Store A,Alice,Item1,25.50,card,1234\n"
            "2024-01-01T12:05:00,Store B,Bob,Item2,30.00,cash,5678"
        )
        data = self.service._process_csv_content(csv_content)
        self.assertEqual(len(data), 2)
        self.assertIn("record_hash", data[0])

    def test_process_csv_content_with_invalid_row(self):
        csv_content = "2024-01-01T12:00:00,Store A,Alice,Item1,25.50,card\n"  # Missing card_number
        data = self.service._process_csv_content(csv_content)
        self.assertEqual(len(data), 0)
        self.mock_logger.warning.assert_called_once()

    @patch("json.loads", return_value=["abc123"])
    def test_check_record_exists_true(self, mock_json_loads):
        self.mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: b'["abc123"]')
        }
        exists = self.service.check_record_exists("my-bucket", "abc123")
        self.assertTrue(exists)

    @patch("json.loads", return_value=[])
    def test_check_record_exists_false_and_create_registry(self, mock_json_loads):
        class NoSuchKey(Exception): pass  # Create mock exception type
        self.mock_s3.exceptions.NoSuchKey = NoSuchKey
        self.mock_s3.get_object.side_effect = NoSuchKey("Key not found")

        exists = self.service.check_record_exists("my-bucket", "new_hash")
        self.assertFalse(exists)
        self.mock_s3.put_object.assert_called_once()

    @patch("json.loads", return_value=["existing_hash"])
    def test_update_hash_registry_adds_new_hashes(self, mock_json_loads):
        self.mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: b'["existing_hash"]')
        }
        self.service.update_hash_registry("bucket", ["new_hash"])
        self.mock_s3.put_object.assert_called_once()
        args, kwargs = self.mock_s3.put_object.call_args
        body = kwargs["Body"].decode("utf-8")
        self.assertIn("new_hash", body)

    def test_convert_to_csv_returns_valid_csv(self):
        input_data = [{
            "transaction_timestamp": "2024-01-01T12:00:00",
            "location_name": "Store A",
            "customer_name": "Alice",
            "products": "Item1",
            "transaction_total": "25.50",
            "payment_method": "card",
            "card_number": "1234",
            "record_hash": "abc123"
        }]
        csv_string = self.service._convert_to_csv(input_data)
        self.assertIn("transaction_timestamp", csv_string)
        self.assertIn("abc123", csv_string)

    def test_extract_csv_with_deduplication(self):
        csv_content = "2024-01-01T12:00:00,Store A,Alice,Item1,25.50,card,1234\n"

        class NoSuchKey(Exception): pass
        self.mock_s3.exceptions.NoSuchKey = NoSuchKey

        # Simulate responses based on Key
        def get_object_side_effect(Bucket, Key):
            if Key == "file.csv":
                return {"Body": MagicMock(read=lambda: csv_content.encode("utf-8"))}
            elif Key == "hash_registry/record_hashes.json":
                raise NoSuchKey("Missing hash registry")
            else:
                raise ValueError("Unexpected S3 key")

        self.mock_s3.get_object.side_effect = get_object_side_effect

        result = self.service.extract_csv("bucket", "file.csv")
        self.assertEqual(len(result), 1)
        self.assertEqual(self.service.last_duplicate_count, 0)
        self.mock_s3.put_object.assert_called()

    def test_extract_csv_without_deduplication(self):
        csv_content = "2024-01-01T12:00:00,Store A,Alice,Item1,25.50,card,1234\n"
        self.mock_s3.get_object.return_value = {"Body": MagicMock(read=lambda: csv_content.encode("utf-8"))}

        result = self.service.extract_csv("bucket", "file.csv", perform_deduplication=False)
        self.assertEqual(len(result), 1)
        self.mock_logger.info.assert_any_call("✔️ Completed processing file without deduplication. Total rows: 1")

    def test_extract_csv_handles_exception(self):
        self.mock_s3.get_object.side_effect = Exception("S3 error")
        with self.assertRaises(RuntimeError):
            self.service.extract_csv("bucket", "file.csv")

if __name__ == "__main__":
    unittest.main()
