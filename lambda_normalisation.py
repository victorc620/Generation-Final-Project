import pandas as pd
import hashlib
from datetime import datetime
import os, boto3
# TRANSFORM

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
    product_df = product_df.reindex(columns=["product_id","products","product_price"])
    product_df["product_price"] = pd.to_numeric(product_df["product_price"], downcast="float")
    return product_df

def create_location_df(df_transformed: pd.DataFrame):
    """Generate a location_df that ready to be uploaded to location table in database"""
    loction_array = df_transformed["location"].unique()
    location_df = pd.DataFrame(loction_array, columns= ["location"])
    location_df = create_hash_id(location_df, "cafe_id")
    location_df = location_df.reindex(columns=["cafe_id","location"])
    return location_df

def create_orders_df(df_transformed: pd.DataFrame, location_df):
    """Generate a orders_df that ready to be uploaded to orders table in database"""
    orders_df = df_transformed[["location","datetime","payment_type","total_price"]]
    orders_df = orders_df.drop_duplicates()
    orders_df.reset_index(inplace=True)
    orders_df.rename(columns = {"datetime":"date"}, inplace=True)
    orders_df = orders_df.merge(location_df, on="location", how="left")
    orders_df.drop(columns="location", inplace=True)
    orders_df = orders_df.reindex(columns=["order_id","cafe_id","date","payment_type","total_price"])
    return orders_df

def create_orders_products_df(df_transformed: pd.DataFrame, product_df):
    """Generate a orders_df that ready to be uploaded to orders_products table in database"""
    orders_products_df = df_transformed[["products"]]
    orders_products_df = orders_products_df.groupby(["order_id","products"]).size()
    orders_products_df = orders_products_df.reset_index(name="quantity_purchased")
    orders_products_df = orders_products_df.merge(product_df, on="products", how="left")
    orders_products_df.drop(columns = ["products", "product_price"], inplace=True)
    orders_products_df = orders_products_df.reindex(columns=["order_id","product_id","quantity_purchased"])
    return orders_products_df

# SEND TO S3 BUCKET 
def write_csv_to_tmp(df, file, filename_location_date):
    """This function will convert Dataframe into csv to load into s3 bucket to be load later into RS"""
    file_timestamp_name = create_time_stamp_filename(file, filename_location_date)
    destination_path_name = f"/tmp/{file_timestamp_name}"
    df.to_csv(destination_path_name, index=False)
    return destination_path_name
    
def upload_file(file_name, bucket):
    s3_client = boto3.client('s3')
    key = file_name.split("/")[-1]
    print(key)
    s3_client.upload_file(file_name, bucket, key)

def create_time_stamp_filename(file, filename_location_date):
    date = datetime.now().strftime("%Y-%m-%d_%I-%M-%S-%p")
    file_type = file.split(".")[-1]
    filename = file.split(".")[0]
    # file_timestamp_name = f"{filename}.{date}.{filename_location_date}.{file_type}"
    file_timestamp_name = f"{filename}.{filename_location_date}.{file_type}"
    return file_timestamp_name
