import logging
import boto3
import pandas as pd
import os
import hashlib

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def handler(event, context):
    LOGGER.info(f"Event structure: {event}")

    # Use boto3 to download the event s3 object key to the /tmp directory.
    client = boto3.client("s3")
    s3 = boto3.resource("s3")
    
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    filename = os.path.basename(key)
    s3.meta.client.download_file(bucket, key, f"/tmp/{filename}")
    # File downloaded to /tmp directory