import json
import urllib3

def send_cfn_response(event, context, status, reason, logger):
    """
    Sends a response back to the AWS CloudFormation service for a custom resource.

    Args:
        event (dict): The event data from the CloudFormation custom resource.
        context (LambdaContext): Runtime information provided by AWS Lambda.
        status (str): The status of the operation ("SUCCESS" or "FAILED").
        reason (str): A human-readable reason for the status.
        logger (Logger): Logger for outputting log messages.
    """
    
    # Create an HTTP connection pool manager
    http = urllib3.PoolManager()
    
    response_body = {
        "Status": status,
        "Reason": reason,
        "PhysicalResourceId": context.log_stream_name,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
    }

    # Log the response being sent
    logger.info(f"📬 Sending CloudFormation response: {json.dumps(response_body)}")
    
    # Send the response back to CloudFormation using the pre-signed S3 URL
    http.request("PUT", event["ResponseURL"], body=json.dumps(response_body))
