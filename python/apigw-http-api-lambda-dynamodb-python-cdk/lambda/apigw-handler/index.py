# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch all supported libraries
patch_all()

import boto3
import os
import json
import logging
import uuid
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def log_event(level, event_type, message, context, event, **kwargs):
    """Helper function for structured logging"""
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": level,
        "eventType": event_type,
        "message": message,
        "requestId": context.request_id,
        "functionName": context.function_name,
        "sourceIp": event.get("requestContext", {}).get("identity", {}).get("sourceIp"),
        "userAgent": event.get("requestContext", {}).get("identity", {}).get("userAgent"),
        **kwargs
    }
    logger.info(json.dumps(log_entry))


def handler(event, context):
    table = os.environ.get("TABLE_NAME")
    
    log_event("INFO", "REQUEST_RECEIVED", "Processing API request", context, event, 
              tableName=table, hasBody=bool(event.get("body")))
    
    if event["body"]:
        item = json.loads(event["body"])
        log_event("INFO", "PAYLOAD_PARSED", "Received payload", context, event, 
                  payload=item)
        year = str(item["year"])
        title = str(item["title"])
        id = str(item["id"])
        dynamodb_client.put_item(
            TableName=table,
            Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
        )
        log_event("INFO", "DATA_INSERTED", "Successfully inserted data", context, event, 
                  itemId=id)
        message = "Successfully inserted data!"
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": message}),
        }
    else:
        log_event("INFO", "DEFAULT_PAYLOAD", "Request without payload, using default", context, event)
        default_id = str(uuid.uuid4())
        dynamodb_client.put_item(
            TableName=table,
            Item={
                "year": {"N": "2012"},
                "title": {"S": "The Amazing Spider-Man 2"},
                "id": {"S": default_id},
            },
        )
        log_event("INFO", "DATA_INSERTED", "Successfully inserted default data", context, event, 
                  itemId=default_id)
        message = "Successfully inserted data!"
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": message}),
        }
