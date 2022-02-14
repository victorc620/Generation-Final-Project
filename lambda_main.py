import os, boto3
import logging

from lambda_database_func import *
from lambda_data_normalisation_func import *

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
  file_type = filename.split(".")[-1]
  
  if file_type == "csv":
    s3.meta.client.download_file(bucket, key, f"/tmp/{filename}")
    print("csv loaded to /tmp")
  
    # 1. Create different dataframe
    df_original = load_csv_to_df(f"/tmp/{filename}")
    df_transformed = basic_transform(df_original)

    product_df = create_product_df(df_transformed)
    location_df = create_location_df(df_transformed)
    orders_df = create_orders_df(df_transformed)
    orders_products_df = create_orders_products_df(df_transformed)
  
    # 2. Insert df into Redshift 
    insert_value(product_df, "products", table_temp = "products_temp")
    insert_value(location_df, "cafe", table_temp = "cafe_temp")
    insert_value(orders_df, "orders", table_temp = "orders_temp")
    insert_value(orders_products_df, "orders_products", table_temp = "orders_products_temp")
    print("Done")

  else:
    print(f"file type is {file_type} instead of csv, nothing loaded to redshift.")