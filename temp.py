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
    
    #----------------------------------------------------------
    
    import pandas as pd
import hashlib
from datetime import datetime

def basic_transform(df_original: pd.DataFrame):
    """
    Perform basic transform from original csv dataframe to transformed dataframe
    transformed dataframe will then be used to generate different dataframe (i.e. cafe, orders, orders_products and products)
    which will be ready for upload to database
    """
    df_transformed = copy_of_original_data(df_original)
    df_transformed = create_hash_id(df_transformed, "order_id")
    df_transformed = products_price_explode(df_transformed, "productsprice", ",")
    df_transformed = add_product_price_colume(df_transformed)
    df_transformed = drop_column(df_transformed, "card_number")
    df_transformed = drop_column(df_transformed, "fullname")
    df_transformed = set_index(df_transformed, "order_id")
    df_transformed = clean_spaces(df_transformed)
    return df_transformed

def create_hash_id(df_arg: pd.DataFrame, column:str):
    """
    Generate a hash id based on the original data (before removing any data)
    hashing method still waiting to be updated
    """
    df_arg[column]=df_arg.astype(str).sum(1).apply(lambda x:hashlib.md5(x.encode()).hexdigest())
    return df_arg

def drop_column(df_arg: pd.DataFrame, column:str):
    "Drop any column in the DataFrame"
    df_arg.drop(column, inplace=True, axis=1)
    return df_arg
    
def set_index(df_arg:pd.DataFrame, column:str):
    "Set specific column as index"
    df_arg.set_index(column, inplace = True)
    return df_arg

def products_price_explode(df_arg: pd.DataFrame, column:str, split_criteria:str):
    "Perform 1NF explode to productsprice where each role contain one product only"
    df_func = df_arg.copy()
    df_func[column] = df_func[column].map(str)
    df_func[column] = df_func[column].str.split(split_criteria)
    df_func = df_func.explode(column)
    return df_func

def add_product_price_colume(df_arg: pd.DataFrame):
    """
    Add 'product_price' colume for product price
    Rename column "productsprice" to "products"
    """
    df_arg["product_price"] = df_arg["productsprice"].str.split(" - ").str[-1]
    df_arg = remove_price_from_products(df_arg)
    df_arg.rename(columns={'productsprice':'products'}, inplace=True)
    return df_arg

def remove_price_from_products(df_arg: pd.DataFrame):
    "data cleansing: remove price from product"
    df_arg["productsprice"] = df_arg["productsprice"].map(lambda x:x.rstrip(' -0123456789.'))
    return df_arg

def load_csv_to_df(path:str):
    """
    loading a cafe csv file into dataframe
    columns = "datetime","location","fullname", "productsprice", "total_price","payment_type","card_number"
    """
    custom_date_parser = lambda x: datetime.strptime(x, "%d/%m/%Y %H:%M")
    "load csv file to python in pandas DataFrame format"
    df = pd.read_csv(path, names = ["datetime","location","fullname", "productsprice", "total_price","payment_type","card_number"], parse_dates=['datetime'],
                date_parser=custom_date_parser)
    return df

def copy_of_original_data(df_arg: pd.DataFrame):
    """make a copy of a dataframe"""
    df_copy = df_arg.copy()
    return df_copy

def clean_spaces(df_args: pd.DataFrame):
    """Data cleansing: Remove left white_space of products"""
    df_args['products'] = df_args['products'].map(lambda x:x.lstrip())
    return df_args

def create_product_df(df_transformed: pd.DataFrame):
    """Generate a product_df that ready to be uploaded to products table in database"""
    product_df = df_transformed[["products","product_price"]]
    product_df.reset_index(inplace=True)
    product_df = product_df.drop(columns = "order_id")
    product_df = product_df.drop_duplicates(subset=['products'])
    product_df = create_hash_id(product_df , "product_id")
    # product_df.set_index("product_id", inplace=True)
    return product_df

def create_location_df(df_transformed: pd.DataFrame):
    """Generate a location_df that ready to be uploaded to location table in database"""
    loction_array = df_transformed["location"].unique()
    location_df = pd.DataFrame(loction_array, columns= ["location"])
    location_df = create_hash_id(location_df, "cafe_id")
    # location_df.set_index("cafe_id",inplace=True)
    return location_df

def create_orders_df(df_transformed: pd.DataFrame):
    """Generate a orders_df that ready to be uploaded to orders table in database"""
    orders_df = df_transformed[["location","datetime","payment_type","total_price"]]
    orders_df = orders_df.drop_duplicates()
    orders_df.reset_index(inplace=True)
    orders_df.rename(columns = {"datetime":"date"}, inplace=True)
    return orders_df

def create_orders_products_df(df_transformed: pd.DataFrame):
    """Generate a orders_df that ready to be uploaded to orders_products table in database"""
    orders_products_df = df_transformed[["products"]]
    orders_products_df = orders_products_df.groupby(["order_id","products"]).size()
    orders_products_df = orders_products_df.reset_index(name="quantity_purchased")
    return orders_products_df

#-------------------------------------------------------------
import os, boto3
import psycopg2
import pandas as pd
import backoff

def backoff_hdlr(details):
    print ("Backoff triggered")

def connect():
    
    aws_client = boto3.client('ssm')
    response = aws_client.get_parameter(
    Name='team4_creds',
    WithDecryption=True)

    cred_string = response['Parameter']['Value']
    cred_string = cred_string.strip("}{").split(",")
    cred_string = [x.strip("\n") for x in cred_string]

    redshift_host = cred_string[0].replace('"',"").replace(" ","").split(":")[1]
    redshift_port = cred_string[1].replace('"',"").replace(" ","").split(":")[1]
    redshift_password = cred_string[2].replace('"',"").replace(" ","").split(":")[1]
    redshift_user = cred_string[3].replace('"',"").replace(" ","").split(":")[1]
    
    conn = psycopg2.connect(
    host=redshift_host,
    port=redshift_port,
    database="team4_cafe",
    user=redshift_user,
    password=redshift_password
    )
    return conn

@backoff.on_exception(backoff.expo, Exception, on_backoff=backoff_hdlr)
def insert_value(df, table_name, table_temp):
    """
    execture PostgreSQL command to insert data to redshift database
    # TODO: Find out how to pass sql f-string as argument 
    """
    connection = connect()
    connection.autocommit = True
    cursor = connection.cursor()
    
    try: 
        np_data = df.to_numpy()
        args_str = b','.join(cursor.mogrify(f'({",".join(["%s"] * len(np_data[0]))})', x) for x in tuple(map(tuple,np_data)))
        cols = [x for x in df.columns]
        insert_tmp = f"insert into {table_temp} ({', '.join(cols)}) values {args_str.decode('utf-8')}"
        
        if table_name == "cafe":
            try:
                sql = """
                CREATE TEMP TABLE cafe_temp(
                cafe_id VARCHAR(256),
                location VARCHAR(256)
                );"""
                cursor.execute(sql)
                
                # sql = "LOCK cafe_temp;"
                # cursor.execute(sql)
                
                sql = insert_tmp
                cursor.execute(sql)
                
                sql = "LOCK cafe;"
                cursor.execute(sql)
                
                sql = """INSERT INTO cafe SELECT t.* FROM cafe_temp t
                        LEFT JOIN cafe c ON t.cafe_id = c.cafe_id
                        WHERE c.cafe_id IS NULL;"""
                cursor.execute(sql)
                
                print("Cafe Successfully Inserted~~!")
                
            except Exception as e:
                print(f"Insert Failed, error: {e}")
            
        elif table_name == "products":
            try:
                sql = """
                    CREATE TEMP TABLE products_temp(
                    product_id VARCHAR(256) NOT NULL,
                    products VARCHAR(256) NOT NULL,
                    product_price DOUBLE PRECISION NOT NULL
                    );"""
                cursor.execute(sql)
                
                # sql = "LOCK products_temp;"
                # cursor.execute(sql)
                
                sql = insert_tmp
                cursor.execute(sql)
                
                sql = "LOCK products;"
                cursor.execute(sql)
                
                sql = """INSERT INTO products SELECT t.* FROM products_temp t
                    LEFT JOIN products p ON t.product_id = p.product_id
                    WHERE p.product_id IS NULL;"""
                cursor.execute(sql)
                    
                print("Products Successfully Inserted~~!")
                
            except Exception as e:
                print(f"Insert Failed, error: {e}")
    
        elif table_name == "orders":
            try:
                sql = """
                CREATE TEMP TABLE orders_temp(
                order_id VARCHAR(256) NOT NULL,
                location VARCHAR(256) NOT NULL,
                date TIMESTAMP without time zone,
                payment_type VARCHAR(256) NOT NULL,
                total_price double precision NOT NULL
                );"""
                cursor.execute(sql)
            
                # sql ="LOCK orders_temp;"
                # cursor.execute(sql)
            
                sql = insert_tmp
                cursor.execute(sql)
                
                sql = """
                CREATE TEMP TABLE orders_temp_2 AS (
                SELECT order_id, cafe.cafe_id, date, payment_type, total_price
                FROM orders_temp
                INNER JOIN cafe
                ON cafe.location = orders_temp.location
                );"""
                cursor.execute(sql)
                
                # sql="LOCK orders_temp_2, orders;"
                # cursor.execute(sql)
            
                sql = """INSERT INTO orders SELECT t.* FROM orders_temp_2 t
                LEFT JOIN orders o ON t.order_id = o.order_id
                WHERE o.order_id IS NULL;"""
                cursor.execute(sql)
                
                print("Orders Successfully Inserted~~!")
                
            except Exception as e:
                print(f"Insert Failed, error: {e}")
            
            
        elif table_name == "orders_products":
            try:
                sql ="""
                    CREATE TEMP TABLE orders_products_temp (
                    order_id VARCHAR(256) NOT NULL,
                    products VARCHAR(256) NOT NULL,
                    quantity_purchased INT
                    );"""
                cursor.execute(sql)
                    
                # sql = "LOCK orders_products_temp;"
                # cursor.execute(sql)
                
                sql = insert_tmp
                cursor.execute(sql)
                    
                sql = """CREATE TEMP TABLE orders_products_temp_2 AS (
                        SELECT order_id, products.product_id, quantity_purchased
                        FROM orders_products_temp
                        INNER JOIN products
                        ON products.products = orders_products_temp.products
                        );"""
                cursor.execute(sql)
                    
                # sql = "LOCK orders_products_temp_2, orders_products;"
                # cursor.execute(sql)
            
                sql = """INSERT INTO orders_products SELECT t.* FROM orders_products_temp_2 t
                    LEFT JOIN orders_products op ON t.order_id = op.order_id
                    WHERE op.order_id IS NULL;"""
                cursor.execute(sql)
        
                print("Orders_Products Successfully Inserted~~!")
                
            except Exception as e:
                print(f"Insert Failed, error: {e}")
    
    except Exception as e:
        print(f"Error: {e}")
        
    finally:
        cursor.close()
        connection.close()
        print("Database connection closed")
    