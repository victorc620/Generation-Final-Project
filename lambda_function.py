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
    
    # Use python-pandas to read and normalise the data from csv.
    def create_hash_id(df_arg: pd.DataFrame):
        """
        Generate a hash id based on the original data (before removing any data)
        hashing method still waiting to be updated
        """
        df_arg["id"]=df_arg.astype(str).sum(1).apply(lambda x:hashlib.md5(x.encode()).hexdigest())
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
        "Perform 1NF explode to products+price where each role contain one product only"
        df_func = df_arg.copy()
        df_func[column] = df_func[column].map(str)
        df_func[column] = df_func[column].str.split(split_criteria)
        df_func = df_func.explode(column)
        print(df_func)
        return df_func

    def add_product_price_colume(df_arg: pd.DataFrame):
        """
        Add 'product_price' colume for product price
        Rename column "products+price" to "products"
        """
        df_arg["product_price"] = df_arg["products+price"].str.split(" - ").str[-1]
        df_arg = remove_price_from_products(df_arg)
        df_arg.rename(columns={'products+price':'products'}, inplace=True)
        return df_arg

    def remove_price_from_products(df_arg):
        "data cleansing: remove price from product"
        df_arg["products+price"] = df_arg["products+price"].map(lambda x:x.rstrip(' -0123456789.'))
        return df_arg

    def load_csv_to_df(path:str):
        "load csv file to python in pandas DataFrame format"
        df = pd.read_csv(path, names = ["datetime","Location","fullname", "products+price", "total_price","payment_type","card_number"])
        return df

    def copy_of_original_data(df_arg):
        df_copy = df_arg.copy()
        return df_copy

    #------------------------------------------------------------------------
    # Load csv into python as pandas DataFrame
    df_original = load_csv_to_df(f"/tmp/{filename}")

    # Perform data_normalization
    df_transformed = copy_of_original_data(df_original)
    df_transformed = create_hash_id(df_transformed)
    df_transformed = products_price_explode(df_transformed, "products+price", ",")
    df_transformed = add_product_price_colume(df_transformed)
    df_transformed = drop_column(df_transformed, "card_number")
    df_transformed = drop_column(df_transformed, "fullname")
    df_transformed = set_index(df_transformed, "id")
    print(df_transformed)
    
    print("Ex4 func complete")