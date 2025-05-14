import pytest
from unittest import mock

@pytest.fixture
def sample_s3_event():
    return {
        "Records": [
            {
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": "my-bucket"},
                    "object": {"key": "incoming/file1.csv"}
                }
            },
            {
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": "my-bucket"},
                    "object": {"key": "processed/file2.csv"}
                }
            }
        ]
    }


def import_handler_with_mocks():
    with mock.patch("services.get_aws_clients", return_value=(mock.Mock(), mock.Mock())), \
         mock.patch("services.get_config", return_value={
             "PERFORM_DEDUPLICATION": True,
             "PROCESSED_DIR": "processed/",
             "TRANSFORM_QUEUE_URL": "https://mock-sqs-url"
         }), \
         mock.patch("services.S3Service"), \
         mock.patch("services.SQSService"), \
         mock.patch("utils.EventUtils") as mock_event_utils:
        
        import importlib
        from src.extract import extract_handler
        importlib.reload(extract_handler)  # Ensure reload with mocks
        return extract_handler, mock_event_utils


def test_should_process_file_true():
    handler, _ = import_handler_with_mocks()
    assert handler.should_process_file("incoming/data.csv") is True


def test_should_process_file_false():
    handler, _ = import_handler_with_mocks()
    assert handler.should_process_file("processed/data.csv") is False


def test_lambda_handler_filters_processed_files(sample_s3_event):
    handler, mock_event_utils = import_handler_with_mocks()
    mock_event_utils.return_value.process_event.return_value = (5, 2)

    response = handler.lambda_handler(sample_s3_event, None)

    assert response["statusCode"] == 200
    assert "CSV extraction complete" in response["body"]
    assert mock_event_utils.return_value.process_event.called
    processed_keys = [rec["s3"]["object"]["key"] for rec in mock_event_utils.return_value.process_event.call_args[0][0]["Records"]]
    assert processed_keys == ["incoming/file1.csv"]


def test_lambda_handler_all_files_skipped(sample_s3_event):
    handler, mock_event_utils = import_handler_with_mocks()
    for record in sample_s3_event["Records"]:
        record["s3"]["object"]["key"] = "processed/file.csv"

    response = handler.lambda_handler(sample_s3_event, None)

    assert response["statusCode"] == 200
    assert response["body"] == "No files to process"
    mock_event_utils.return_value.process_event.assert_not_called()


def test_lambda_handler_handles_exception(sample_s3_event):
    handler, mock_event_utils = import_handler_with_mocks()
    mock_event_utils.return_value.process_event.side_effect = Exception("Something went wrong")

    response = handler.lambda_handler(sample_s3_event, None)

    assert response["statusCode"] == 500
    assert "Error: Something went wrong" in response["body"]
