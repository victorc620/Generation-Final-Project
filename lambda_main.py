import os
import logging
import boto3

from lambda_database_func import *
from lambda_data_normalisation import *

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
  print("csv loaded to /tmp")
  
  # 1. Create different dataframe
  df_original = load_csv_to_df(f"/tmp/{filename}")
  df_transformed = basic_transform(df_original)

  product_df = create_product_df(df_transformed)
  location_df = create_location_df(df_transformed)
  orders_df = create_orders_df(df_transformed)
  orders_products_df = create_orders_products_df(df_transformed)
        
  insert_value(product_df, "products", table_temp = "products_temp")
  insert_value(location_df, "cafe", table_temp = "cafe_temp")
  insert_value(orders_df, "orders", table_temp = "orders_temp")
  insert_value(orders_products_df, "orders_products", table_temp = "orders_products_temp")
  print("Done")
  
  # 2. Insert dataframe into database
  
  # load_dotenv()
  # host = os.environ.get("host")
  # user = os.environ.get("user")
  # password = os.environ.get("password")
  # db = os.environ.get("database")
  # url = URL.create(
  # drivername='redshift+redshift_connector', # indicate redshift_connector driver and dialect will be used
  # host=host, # Amazon Redshift host
  # port=5439, # Amazon Redshift port
  # database=db, # Amazon Redshift database
  # username=user, # Amazon Redshift username
  # password=password # Amazon Redshift password
  # )

