import unittest
from unittest.mock import patch, MagicMock, ANY
import json
import sys
from pathlib import Path

# Add the project root to Python path to fix import issues
sys.path.insert(0, str(Path(__file__).parent.parent))

# Mock the modules that can't be imported directly
sys.modules['services'] = MagicMock()
sys.modules['utils'] = MagicMock()

# Import after mocking dependencies
from src.create_tables.create_tables_handler import lambda_handler

class TestExtractHandler(unittest.TestCase):
    """Test cases for the create_tables Lambda handler"""

    def setUp(self):
        """Set up test fixtures"""
        # Mock CloudFormation event for testing
        self.cloudformation_event = {
            "RequestType": "Create",
            "ResponseURL": "https://cloudformation-custom-resource-response-useast1.s3.amazonaws.com/response-url",
            "StackId": "arn:aws:cloudformation:us-east-1:123456789012:stack/MyStack/guid",
            "RequestId": "unique-request-id",
            "ResourceType": "Custom::CreateTables",
            "LogicalResourceId": "CreateTablesResource",
            "ResourceProperties": {
                "ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:MyFunction"
            }
        }
        
        # Mock delete event
        self.delete_event = {
            "RequestType": "Delete",
            "ResponseURL": "https://cloudformation-custom-resource-response-useast1.s3.amazonaws.com/response-url",
            "StackId": "arn:aws:cloudformation:us-east-1:123456789012:stack/MyStack/guid",
            "RequestId": "unique-delete-id",
            "ResourceType": "Custom::CreateTables",
            "LogicalResourceId": "CreateTablesResource",
            "ResourceProperties": {
                "ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:MyFunction"
            }
        }
        
        # Mock context for Lambda
        self.context = MagicMock()
        self.context.function_name = "create-tables-function"
        self.context.function_version = "$LATEST"
        self.context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:create-tables-function"
        
        # Mock objects for dependencies
        self.mock_config = {
            "SSM_PARAMETER_NAME": "/beanme/redshift/credentials"
        }
        self.mock_redshift_config = {
            "host": "redshift-cluster.amazonaws.com",
            "port": 5439,
            "dbname": "beanme",
            "user": "admin",
            "password": "password123"
        }

    @patch('src.create_tables.create_tables_handler.send_cfn_response')
    @patch('src.create_tables.create_tables_handler.setup_redshift_schema')
    @patch('src.create_tables.create_tables_handler.get_parameter_from_ssm')
    @patch('src.create_tables.create_tables_handler.get_config')
    def test_lambda_handler_success(self, mock_get_config, mock_get_parameter, mock_setup_schema, mock_send_response):
        """Test successful execution of lambda handler"""
        # Configure mocks
        mock_get_config.return_value = self.mock_config
        mock_get_parameter.return_value = self.mock_redshift_config
        
        # Call the lambda handler
        lambda_handler(self.cloudformation_event, self.context)
        
        # Assert the expected behavior
        mock_get_config.assert_called_once()
        mock_get_parameter.assert_called_once_with(self.mock_config["SSM_PARAMETER_NAME"])
        mock_setup_schema.assert_called_once_with(self.mock_redshift_config, ANY)
        mock_send_response.assert_called_once_with(
            self.cloudformation_event, self.context, "SUCCESS", "Tables created successfully.", ANY
        )

    @patch('src.create_tables.create_tables_handler.send_cfn_response')
    @patch('src.create_tables.create_tables_handler.setup_redshift_schema')
    @patch('src.create_tables.create_tables_handler.get_parameter_from_ssm')
    @patch('src.create_tables.create_tables_handler.get_config')
    def test_lambda_handler_client_error(self, mock_get_config, mock_get_parameter, mock_setup_schema, mock_send_response):
        """Test handling of AWS client initialization error"""
        # Configure mocks
        mock_get_config.side_effect = RuntimeError("AWS credentials not found")
        
        # Call the lambda handler
        lambda_handler(self.cloudformation_event, self.context)
        
        # Assert the expected behavior
        mock_get_config.assert_called_once()
        mock_get_parameter.assert_not_called()
        mock_setup_schema.assert_not_called()
        mock_send_response.assert_called_once_with(
            self.cloudformation_event, self.context, "FAILED", ANY, ANY
        )

    @patch('src.create_tables.create_tables_handler.send_cfn_response')
    @patch('src.create_tables.create_tables_handler.setup_redshift_schema')
    @patch('src.create_tables.create_tables_handler.get_parameter_from_ssm')
    @patch('src.create_tables.create_tables_handler.get_config')
    def test_lambda_handler_config_error(self, mock_get_config, mock_get_parameter, mock_setup_schema, mock_send_response):
        """Test handling of configuration error"""
        # Configure mocks
        mock_get_config.return_value = self.mock_config
        mock_get_parameter.side_effect = RuntimeError("Missing required parameter")
        
        # Call the lambda handler
        lambda_handler(self.cloudformation_event, self.context)
        
        # Assert the expected behavior
        mock_get_config.assert_called_once()
        mock_get_parameter.assert_called_once()
        mock_setup_schema.assert_not_called()
        mock_send_response.assert_called_once_with(
            self.cloudformation_event, self.context, "FAILED", ANY, ANY
        )

    @patch('src.create_tables.create_tables_handler.send_cfn_response')
    @patch('src.create_tables.create_tables_handler.setup_redshift_schema')
    @patch('src.create_tables.create_tables_handler.get_parameter_from_ssm')
    @patch('src.create_tables.create_tables_handler.get_config')
    def test_lambda_handler_processing_error(self, mock_get_config, mock_get_parameter, mock_setup_schema, mock_send_response):
        """Test handling of schema setup error"""
        # Configure mocks
        mock_get_config.return_value = self.mock_config
        mock_get_parameter.return_value = self.mock_redshift_config
        mock_setup_schema.side_effect = Exception("Error creating tables")
        
        # Call the lambda handler
        lambda_handler(self.cloudformation_event, self.context)
        
        # Assert the expected behavior
        mock_get_config.assert_called_once()
        mock_get_parameter.assert_called_once()
        mock_setup_schema.assert_called_once()
        mock_send_response.assert_called_once_with(
            self.cloudformation_event, self.context, "FAILED", ANY, ANY
        )

    @patch('src.create_tables.create_tables_handler.send_cfn_response')
    @patch('src.create_tables.create_tables_handler.setup_redshift_schema')
    @patch('src.create_tables.create_tables_handler.get_parameter_from_ssm')
    @patch('src.create_tables.create_tables_handler.get_config')
    def test_lambda_handler_delete_request(self, mock_get_config, mock_get_parameter, mock_setup_schema, mock_send_response):
        """Test handling of Delete request type"""
        # Call the lambda handler with a delete event
        lambda_handler(self.delete_event, self.context)
        
        # Assert the expected behavior
        mock_get_config.assert_not_called()
        mock_get_parameter.assert_not_called()
        mock_setup_schema.assert_not_called()
        mock_send_response.assert_called_once_with(
            self.delete_event, self.context, "SUCCESS", "Delete event handled successfully.", ANY
        )

if __name__ == '__main__':
    unittest.main()