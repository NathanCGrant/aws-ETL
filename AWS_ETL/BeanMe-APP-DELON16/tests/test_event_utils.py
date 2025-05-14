import unittest
from unittest.mock import MagicMock, patch, call
import pytest

# Import the class to test - assuming it's in a module called event_utils
from src.extract.utils import EventUtils

class TestEventUtils(unittest.TestCase):
    """Test suite for the EventUtils class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.s3_service = MagicMock()
        self.sqs_service = MagicMock()
        self.logger = MagicMock()
        
        # Reset duplicate count attribute on S3 service mock
        self.s3_service.last_duplicate_count = 0
        
        # Configuration dictionary
        self.config = {
            "RAW_DATA_BUCKET": "test-raw-bucket"
        }
        
        # Create an instance of EventUtils for testing
        self.event_utils = EventUtils(
            self.s3_service,
            self.sqs_service,
            self.config,
            self.logger
        )
    
    def test_init(self):
        """Test the initialization of EventUtils."""
        # Test default initialization
        self.assertEqual(self.event_utils.s3_service, self.s3_service)
        self.assertEqual(self.event_utils.sqs_service, self.sqs_service)
        self.assertEqual(self.event_utils.raw_bucket, "test-raw-bucket")
        self.assertEqual(self.event_utils.logger, self.logger)
        self.assertTrue(self.event_utils.perform_deduplication)
        self.assertEqual(self.event_utils.processed_count, 0)
        self.assertEqual(self.event_utils.duplicate_count, 0)
        
        # Test initialization with deduplication disabled
        event_utils_no_dedup = EventUtils(
            self.s3_service,
            self.sqs_service,
            self.config,
            self.logger,
            perform_deduplication=False
        )
        self.assertFalse(event_utils_no_dedup.perform_deduplication)
    
    def test_process_event_no_records(self):
        """Test process_event with an event containing no records."""
        empty_event = {"Records": []}
        
        with self.assertRaises(ValueError) as context:
            self.event_utils.process_event(empty_event)
        
        self.assertEqual(str(context.exception), "No records found in event.")
    
    def test_process_event_empty_event(self):
        """Test process_event with a completely empty event."""
        empty_event = {}
        
        with self.assertRaises(ValueError) as context:
            self.event_utils.process_event(empty_event)
        
        self.assertEqual(str(context.exception), "No records found in event.")
    
    def test_process_record_wrong_bucket(self):
        """Test _process_record with a record from the wrong bucket."""
        record = {
            "s3": {
                "bucket": {"name": "wrong-bucket"},
                "object": {"key": "file.csv"}
            }
        }
        
        self.event_utils._process_record(record)
        
        # Verify that extraction was not called
        self.s3_service.extract_csv.assert_not_called()
        self.sqs_service.send_message.assert_not_called()
    
    def test_process_record_processed_file(self):
        """Test _process_record with an already processed file."""
        record = {
            "s3": {
                "bucket": {"name": "test-raw-bucket"},
                "object": {"key": "processed/file.csv"}
            }
        }
        
        self.event_utils._process_record(record)
        
        # Verify that extraction was not called
        self.s3_service.extract_csv.assert_not_called()
        self.sqs_service.send_message.assert_not_called()
    
    def test_process_record_non_csv_file(self):
        """Test _process_record with a non-CSV file."""
        record = {
            "s3": {
                "bucket": {"name": "test-raw-bucket"},
                "object": {"key": "file.txt"}
            }
        }
        
        self.event_utils._process_record(record)
        
        # Verify that extraction was not called
        self.s3_service.extract_csv.assert_not_called()
        self.sqs_service.send_message.assert_not_called()
    
    def test_process_record_valid_file_with_data(self):
        """Test _process_record with a valid CSV file containing data."""
        record = {
            "s3": {
                "bucket": {"name": "test-raw-bucket"},
                "object": {"key": "file.csv"}
            }
        }
        
        # Mock the extract_csv to return data
        sanitized_data = [{"column1": "value1"}, {"column1": "value2"}]
        self.s3_service.extract_csv.return_value = sanitized_data
        self.s3_service.last_duplicate_count = 3
        
        self.event_utils._process_record(record)
        
        # Verify that extraction was called with the right parameters
        self.s3_service.extract_csv.assert_called_once_with(
            "test-raw-bucket", "file.csv", True
        )
        
        # Verify that SQS message was sent with the sanitized data
        self.sqs_service.send_message.assert_called_once_with(sanitized_data)
        
        # Verify that the processed and duplicate counts were updated
        self.assertEqual(self.event_utils.processed_count, 2)
        self.assertEqual(self.event_utils.duplicate_count, 3)
    
    def test_process_record_valid_file_no_data(self):
        """Test _process_record with a valid CSV file containing no unique data."""
        record = {
            "s3": {
                "bucket": {"name": "test-raw-bucket"},
                "object": {"key": "file.csv"}
            }
        }
        
        # Mock the extract_csv to return no data
        self.s3_service.extract_csv.return_value = []
        self.s3_service.last_duplicate_count = 5
        
        self.event_utils._process_record(record)
        
        # Verify that extraction was called
        self.s3_service.extract_csv.assert_called_once()
        
        # Verify that SQS message was not sent
        self.sqs_service.send_message.assert_not_called()
        
        # Verify that the duplicate count was updated
        self.assertEqual(self.event_utils.processed_count, 0)
        self.assertEqual(self.event_utils.duplicate_count, 5)
    
    def test_process_event_multiple_records(self):
        """Test process_event with multiple records."""
        # Create test event with multiple records
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "test-raw-bucket"},
                        "object": {"key": "file1.csv"}
                    }
                },
                {
                    "s3": {
                        "bucket": {"name": "test-raw-bucket"},
                        "object": {"key": "file2.csv"}
                    }
                },
                {
                    "s3": {
                        "bucket": {"name": "wrong-bucket"},
                        "object": {"key": "file3.csv"}
                    }
                }
            ]
        }
        
        # Mock the extract_csv to return different data for each call
        data1 = [{"column1": "value1"}, {"column1": "value2"}]
        data2 = [{"column1": "value3"}]
        
        def mock_extract_csv(bucket, key, dedup):
            if key == "file1.csv":
                self.s3_service.last_duplicate_count = 2
                return data1
            elif key == "file2.csv":
                self.s3_service.last_duplicate_count = 1
                return data2
            return []
        
        self.s3_service.extract_csv.side_effect = mock_extract_csv
        
        # Process the event
        processed, duplicates = self.event_utils.process_event(event)
        
        # Verify the extract_csv calls
        expected_calls = [
            call("test-raw-bucket", "file1.csv", True),
            call("test-raw-bucket", "file2.csv", True)
        ]
        self.s3_service.extract_csv.assert_has_calls(expected_calls, any_order=False)
        
        # Verify the SQS send_message calls
        expected_sqs_calls = [
            call(data1),
            call(data2)
        ]
        self.sqs_service.send_message.assert_has_calls(expected_sqs_calls, any_order=False)
        
        # Verify the return values
        self.assertEqual(processed, 3)  # 2 from file1.csv + 1 from file2.csv
        self.assertEqual(duplicates, 3)  # 2 from file1.csv + 1 from file2.csv
    
    def test_process_event_no_deduplication(self):
        """Test process_event with deduplication disabled."""
        # Create EventUtils instance with deduplication disabled
        event_utils_no_dedup = EventUtils(
            self.s3_service,
            self.sqs_service,
            self.config,
            self.logger,
            perform_deduplication=False
        )
        
        # Create test event
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "test-raw-bucket"},
                        "object": {"key": "file.csv"}
                    }
                }
            ]
        }
        
        # Mock the extract_csv to return data
        sanitized_data = [{"column1": "value1"}, {"column1": "value2"}]
        self.s3_service.extract_csv.return_value = sanitized_data
        
        # Process the event
        processed, duplicates = event_utils_no_dedup.process_event(event)
        
        # Verify that extract_csv was called with deduplication=False
        self.s3_service.extract_csv.assert_called_once_with(
            "test-raw-bucket", "file.csv", False
        )
        
        # Verify the return values
        self.assertEqual(processed, 2)
        self.assertEqual(duplicates, 0)

if __name__ == "__main__":
    unittest.main()