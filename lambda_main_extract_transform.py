import os, boto3
import logging
import pandas as pd

from lambda_normalisation import *

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
  filename_location_date = filename.split(".")[0]
  file_type = filename.split(".")[-1]
  
  if file_type == "csv":
    s3.meta.client.download_file(bucket, key, f"/tmp/{filename}")
    print("csv loaded to /tmp")
    
    # 1.Create different dataframe
    df_original = load_csv_to_df(f"/tmp/{filename}")
    df_transformed = basic_transform(df_original)

    product_df = create_product_df(df_transformed)
    location_df = create_location_df(df_transformed)
    orders_df = create_orders_df(df_transformed, location_df)
    orders_products_df = create_orders_products_df(df_transformed, product_df)
  
    #2. Covert df into csv in lambda /tmp/ directory 
    product_csv_path = write_csv_to_tmp(product_df, 'product_df.csv', filename_location_date)
    location_csv_path = write_csv_to_tmp(location_df, 'location_df.csv', filename_location_date)
    orders_csv_path = write_csv_to_tmp(orders_df, 'orders_df.csv', filename_location_date)
    orders_products_csv_path = write_csv_to_tmp(orders_products_df, 'orders_products_df.csv', filename_location_date)
    
    #3. send vs(s) to s3 bucket (team-4-extract-transform-production)
    bucket = 'team-4-extract-transform-production'
    upload_file(product_csv_path, bucket)
    upload_file(location_csv_path, bucket)
    upload_file(orders_csv_path, bucket)
    upload_file(orders_products_csv_path, bucket)
  else:
    print(f"file type is {file_type} instead of csv, nothing loaded to redshift.")
    
  print("Extract-transform completed")