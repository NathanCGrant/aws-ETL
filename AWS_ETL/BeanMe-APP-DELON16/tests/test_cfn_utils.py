import json
import unittest
from unittest.mock import patch, MagicMock, ANY
import logging

# Import the function to test
from src.create_tables.utils import send_cfn_response

class TestSendCfnResponse(unittest.TestCase):
    """Tests for send_cfn_response function"""

    def setUp(self):
        """Set up test fixtures"""
        # Create a sample CloudFormation event
        self.event = {
            "RequestType": "Create",
            "ResponseURL": "https://pre-signed-s3-url-for-response",
            "StackId": "arn:aws:cloudformation:us-east-1:123456789012:stack/MyStack/guid",
            "RequestId": "unique-request-id",
            "ResourceType": "Custom::MyResource",
            "LogicalResourceId": "MyCustomResource",
            "ResourceProperties": {
                "CustomProperty": "CustomValue"
            }
        }
        
        # Mock Lambda context
        self.context = MagicMock()
        self.context.log_stream_name = "2023/01/01/[$LATEST]abcdef123456"
        
        # Set up logger
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

    @patch('urllib3.PoolManager')
    def test_send_cfn_response_success(self, mock_pool_manager):
        """Test sending a success response"""
        # Set up the mock
        mock_http = MagicMock()
        mock_pool_manager.return_value = mock_http
        
        # Call the function with success status
        send_cfn_response(
            event=self.event,
            context=self.context,
            status="SUCCESS",
            reason="Operation completed successfully",
            logger=self.logger
        )
        
        # Assert PoolManager was instantiated
        mock_pool_manager.assert_called_once()
        
        # Assert request was made with correct params
        mock_http.request.assert_called_once_with(
            "PUT", 
            self.event["ResponseURL"], 
            body=ANY
        )
        
        # Verify the body content
        call_args = mock_http.request.call_args
        body_arg = call_args[1]['body']
        body_json = json.loads(body_arg)
        
        self.assertEqual(body_json["Status"], "SUCCESS")
        self.assertEqual(body_json["Reason"], "Operation completed successfully")
        self.assertEqual(body_json["PhysicalResourceId"], self.context.log_stream_name)
        self.assertEqual(body_json["StackId"], self.event["StackId"])
        self.assertEqual(body_json["RequestId"], self.event["RequestId"])
        self.assertEqual(body_json["LogicalResourceId"], self.event["LogicalResourceId"])

    @patch('urllib3.PoolManager')
    def test_send_cfn_response_failed(self, mock_pool_manager):
        """Test sending a failure response"""
        # Set up the mock
        mock_http = MagicMock()
        mock_pool_manager.return_value = mock_http
        
        # Call the function with failed status
        send_cfn_response(
            event=self.event,
            context=self.context,
            status="FAILED",
            reason="Resource creation failed: Invalid configuration",
            logger=self.logger
        )
        
        # Assert request was made with correct params
        mock_http.request.assert_called_once()
        
        # Verify the body content
        call_args = mock_http.request.call_args
        body_arg = call_args[1]['body']
        body_json = json.loads(body_arg)
        
        self.assertEqual(body_json["Status"], "FAILED")
        self.assertEqual(body_json["Reason"], "Resource creation failed: Invalid configuration")

    @patch('urllib3.PoolManager')
    @patch('logging.Logger.info')
    def test_logger_called(self, mock_logger_info, mock_pool_manager):
        """Test that logger is called with the response"""
        # Set up the mock
        mock_http = MagicMock()
        mock_pool_manager.return_value = mock_http
        
        # Call the function
        send_cfn_response(
            event=self.event,
            context=self.context,
            status="SUCCESS",
            reason="Test logging",
            logger=self.logger
        )
        
        # Assert logger was called
        mock_logger_info.assert_called_once()
        # Verify log message contains the response body
        log_call = mock_logger_info.call_args[0][0]
        self.assertTrue("📬 Sending CloudFormation response:" in log_call)
        self.assertTrue("SUCCESS" in log_call)
        self.assertTrue("Test logging" in log_call)

    @patch('urllib3.PoolManager')
    def test_http_exception_handling(self, mock_pool_manager):
        """Test handling of HTTP exceptions"""
        # Set up the mock to raise an exception
        mock_http = MagicMock()
        mock_pool_manager.return_value = mock_http
        mock_http.request.side_effect = Exception("Connection error")
        
        # Call the function and expect exception to be raised
        # Note: The current implementation doesn't handle exceptions, so we expect it to propagate
        with self.assertRaises(Exception):
            send_cfn_response(
                event=self.event,
                context=self.context,
                status="SUCCESS",
                reason="This will fail",
                logger=self.logger
            )


if __name__ == '__main__':
    unittest.main()