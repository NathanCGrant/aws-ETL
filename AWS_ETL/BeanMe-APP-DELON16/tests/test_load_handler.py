import unittest
from unittest.mock import patch
import json

# Import the lambda_handler
from src.load.load_handler import lambda_handler

class TestLambdaHandler(unittest.TestCase):

    @patch("src.load.load_handler.load_csv_to_redshift")
    @patch("src.load.load_handler.determine_table_name")
    @patch("src.load.load_handler.get_redshift_config")
    @patch("src.load.load_handler.get_config")
    def test_successful_csv_load(self, mock_get_config, mock_get_redshift_config, mock_determine_table_name, mock_load_csv):
        # Setup mocks
        mock_get_config.return_value = {
            "CLEAN_DATA_BUCKET": "clean-bucket",
            "SSM_PARAMETER_NAME": "redshift-config"
        }
        mock_get_redshift_config.return_value = {"host": "example-redshift"}
        mock_determine_table_name.return_value = "transactions"
        mock_load_csv.return_value = None

        # Simulate a valid S3 event
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "some-bucket"},
                        "object": {"key": "transactions/2024/file.csv"}
                    }
                }
            ]
        }

        # Run the lambda handler
        response = lambda_handler(event, context={})

        # Assertions
        self.assertEqual(response["statusCode"], 200)
        self.assertIn("1 files successfully loaded", json.loads(response["body"]))
        mock_load_csv.assert_called_once()

    @patch("src.load.load_handler.load_csv_to_redshift")
    @patch("src.load.load_handler.determine_table_name")
    @patch("src.load.load_handler.get_redshift_config")
    @patch("src.load.load_handler.get_config")
    def test_skips_unmapped_table(self, mock_get_config, mock_get_redshift_config, mock_determine_table_name, mock_load_csv):
        mock_get_config.return_value = {
            "CLEAN_DATA_BUCKET": "clean-bucket",
            "SSM_PARAMETER_NAME": "redshift-config"
        }
        mock_get_redshift_config.return_value = {}
        mock_determine_table_name.return_value = None

        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "some-bucket"},
                        "object": {"key": "misc/unmapped.csv"}
                    }
                }
            ]
        }

        response = lambda_handler(event, context={})
        self.assertEqual(response["statusCode"], 200)
        self.assertIn("0 files successfully loaded", json.loads(response["body"]))
        mock_load_csv.assert_not_called()

    @patch("src.load.load_handler.load_csv_to_redshift", side_effect=Exception("DB error"))
    @patch("src.load.load_handler.determine_table_name", return_value="products")
    @patch("src.load.load_handler.get_redshift_config", return_value={})
    @patch("src.load.load_handler.get_config", return_value={
        "CLEAN_DATA_BUCKET": "clean-bucket",
        "SSM_PARAMETER_NAME": "redshift-config"
    })
    def test_load_csv_error_handled(self, mock_get_config, mock_get_redshift_config, mock_determine_table_name, mock_load_csv):
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "some-bucket"},
                        "object": {"key": "products/products.csv"}
                    }
                }
            ]
        }

        response = lambda_handler(event, context={})
        self.assertEqual(response["statusCode"], 200)
        self.assertIn("0 files successfully loaded", json.loads(response["body"]))

if __name__ == "__main__":
    unittest.main()
