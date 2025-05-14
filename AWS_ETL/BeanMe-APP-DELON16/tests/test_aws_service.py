import os
import unittest
from unittest.mock import patch, MagicMock
import botocore.exceptions

# Import the module to be tested
from src.extract.services import get_aws_clients, get_config

class TestAWSServices(unittest.TestCase):
    """Test cases for AWS service functions."""

    @patch('boto3.client')
    def test_get_aws_clients_success(self, mock_boto_client):
        """Test successful initialization of AWS clients."""
        # Create mock S3 and SQS clients
        mock_s3 = MagicMock()
        mock_sqs = MagicMock()
        
        # Configure boto3.client to return our mocks
        mock_boto_client.side_effect = lambda service: {
            's3': mock_s3,
            'sqs': mock_sqs
        }[service]
        
        # Call the function and verify results
        s3, sqs = get_aws_clients()
        
        # Assert that boto3.client was called twice with correct services
        self.assertEqual(mock_boto_client.call_count, 2)
        mock_boto_client.assert_any_call('s3')
        mock_boto_client.assert_any_call('sqs')
        
        # Assert that returned clients are our mocks
        self.assertEqual(s3, mock_s3)
        self.assertEqual(sqs, mock_sqs)

    @patch('boto3.client')
    def test_get_aws_clients_no_credentials(self, mock_boto_client):
        """Test handling of missing AWS credentials."""
        # Configure boto3.client to raise NoCredentialsError
        mock_boto_client.side_effect = botocore.exceptions.NoCredentialsError()
        
        # Verify that the function raises the expected exception
        with self.assertRaises(RuntimeError) as context:
            get_aws_clients()
        
        self.assertEqual(str(context.exception), "AWS credentials not found.")

    @patch('boto3.client')
    def test_get_aws_clients_partial_credentials(self, mock_boto_client):
        """Test handling of incomplete AWS credentials."""
        # Configure boto3.client to raise PartialCredentialsError
        mock_boto_client.side_effect = botocore.exceptions.PartialCredentialsError(
            provider='test-provider',
            cred_var='test-var'
        )
        
        # Verify that the function raises the expected exception
        with self.assertRaises(RuntimeError) as context:
            get_aws_clients()
        
        self.assertEqual(str(context.exception), "Incomplete AWS credentials found.")

    @patch('boto3.client')
    def test_get_aws_clients_no_region(self, mock_boto_client):
        """Test handling of missing AWS region."""
        # Configure boto3.client to raise NoRegionError
        mock_boto_client.side_effect = botocore.exceptions.NoRegionError()
        
        # Verify that the function raises the expected exception
        with self.assertRaises(RuntimeError) as context:
            get_aws_clients()
        
        self.assertEqual(str(context.exception), "AWS region not specified. Please set AWS_REGION.")

    @patch('boto3.client')
    def test_get_aws_clients_general_exception(self, mock_boto_client):
        """Test handling of general exceptions during client initialization."""
        # Configure boto3.client to raise a general exception
        mock_boto_client.side_effect = Exception("Some AWS error")
        
        # Verify that the function raises the expected exception
        with self.assertRaises(RuntimeError) as context:
            get_aws_clients()
        
        self.assertEqual(str(context.exception), "Failed to initialize AWS clients: Some AWS error")

    @patch.dict(os.environ, {
        "RAW_BUCKET": "test-raw-bucket",
        "TRANSFORM_QUEUE_URL": "https://sqs.region.amazonaws.com/account/queue"
    })
    def test_get_config_success(self):
        """Test successful loading of configuration from environment variables."""
        config = get_config()
        
        # Assert that config contains expected values
        self.assertEqual(config["RAW_DATA_BUCKET"], "test-raw-bucket")
        self.assertEqual(config["TRANSFORM_QUEUE_URL"], "https://sqs.region.amazonaws.com/account/queue")

    @patch.dict(os.environ, {}, clear=True)
    def test_get_config_missing_env_var(self):
        """Test handling of missing environment variables."""
        with self.assertRaises(RuntimeError) as context:
            get_config()
        
        # The exact key in the error message can vary based on which variable is tried first
        self.assertTrue("Missing required environment variable:" in str(context.exception))

    @patch.dict(os.environ, {"RAW_BUCKET": "test-bucket"})
    def test_get_config_partial_env_vars(self):
        """Test handling of some but not all required environment variables."""
        with self.assertRaises(RuntimeError) as context:
            get_config()
        
        self.assertTrue("Missing required environment variable: 'TRANSFORM_QUEUE_URL'" in str(context.exception))

    @patch('os.environ')
    def test_get_config_general_exception(self, mock_environ):
        """Test handling of general exceptions during configuration loading."""
        # Configure os.environ to raise an exception when accessed
        mock_environ.__getitem__.side_effect = Exception("Some config error")
        
        with self.assertRaises(RuntimeError) as context:
            get_config()
        
        self.assertEqual(str(context.exception), "Error loading configuration: Some config error")

if __name__ == '__main__':
    unittest.main()