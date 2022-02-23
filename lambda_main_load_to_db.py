import boto3
import json
import pandas as pd
from lambda_database_func import *

def handler(event, context):
    
    s3_client = boto3.client('s3')
    s3 = boto3.resource('s3')
    
    # sqs_client = boto3.client('sqs')
    # sqs_message = sqs_client.recieve_message(QueueUrl = "https://sqs.eu-west-1.amazonaws.com/696036660875/team-4-sqs-queue-production")
    # print("SQS MESSAGE")
    # print(sqs_message)
    
    print("sssss")
    
    print("EVENT")
    print(event)
    
    # print("CONTEXT")
    # print(context)
    
    sqs_msg = event['Records'][0]["body"]
    sqs_msg = json.loads(sqs_msg)
    
    s3_bucket_name = sqs_msg["Records"][0]["s3"]["bucket"]["name"]
    s3_file_key = sqs_msg["Records"][0]["s3"]["object"]["key"]
    
    filename = s3_file_key
    filename_without_time = s3_file_key.split(".")[0]
    file_type = filename.split(".")[-1]
    
    print(f"s3_bucket_name: {s3_bucket_name}")
    print(f"filename: {filename}")
    
    # if file_type == "csv":
    #     #insert into Red Shift
    #     if filename_without_time == "product_df":
    #         insert_value('products', filename, s3_bucket_name)
        
    #     elif filename_without_time == "location_df":
    #         insert_value("cafe", filename, s3_bucket_name)
        
    #     elif filename_without_time == "orders_df":
    #         insert_value("orders", filename, s3_bucket_name)
        
    #     elif filename_without_time == "orders_products_df":
    #         insert_value("orders_products", filename, s3_bucket_name)
    
    else:
        print(f"file type is {file_type} instead of csv, nothing loaded to redshift.")
        print(f"s3_bucket_name: {s3_bucket_name}")
        print(f"s3_file_key: {s3_file_key}")
    
    print("Done")

    
    
