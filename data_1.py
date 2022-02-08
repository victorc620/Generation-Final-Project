from numpy import product
import pandas as pd
import hashlib
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine

# def create_hash_id(df_arg: pd.DataFrame):
#     """
#     Generate a hash id based on the original data (before removing any data)
#     hashing method still waiting to be updated
#     """
#     df_arg["order_id"]=df_arg.astype(str).sum(1).apply(lambda x:hashlib.md5(x.encode()).hexdigest())
#     return df_arg

def create_hash_id(df_arg: pd.DataFrame, column):
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

def remove_price_from_products(df_arg):
    "data cleansing: remove price from product"
    df_arg["productsprice"] = df_arg["productsprice"].map(lambda x:x.rstrip(' -0123456789.'))
    return df_arg

def load_csv_to_df(path:str):
    custom_date_parser = lambda x: datetime.strptime(x, "%d/%m/%Y %H:%M")
    "load csv file to python in pandas DataFrame format"
    df = pd.read_csv(path, names = ["datetime","location","fullname", "productsprice", "total_price","payment_type","card_number"], parse_dates=['datetime'],
                date_parser=custom_date_parser)
    return df

def copy_of_original_data(df_arg):
    df_copy = df_arg.copy()
    return df_copy

def clean_spaces(df_args):
    df_args['products'] = df_args['products'].map(lambda x:x.lstrip())
    return df_args

def create_product_df(df_transformed):
    product_df = df_transformed[["products","product_price"]]
    product_df.reset_index(inplace=True)
    product_df = product_df.drop(columns = "order_id")
    product_df = product_df.drop_duplicates(subset=['products'])
    product_df = create_hash_id(product_df , "product_id")
    product_df.set_index("product_id", inplace=True)
    return product_df

def create_location_df(df_transformed):
    loction_array = df_transformed["location"].unique()
    location_df = pd.DataFrame(loction_array, columns= ["location"])
    location_df = create_hash_id(location_df, "location_id")
    location_df.set_index("location_id",inplace=True)
    return location_df

def create_orders_df(df_transformed):
    # order_id, cafe_id, date, payment_type, total_price
    orders_df = df_transformed[["location","datetime","payment_type","total_price"]]
    orders_df = orders_df.drop_duplicates()
    return orders_df

def create_orders_products_df(df_transformed):
    orders_products_df = df_transformed[["products"]]
    orders_products_df = orders_products_df.groupby(["order_id","products"]).size()
    orders_products_df = orders_products_df.reset_index(name="quantity_purchased")
    return orders_products_df

def load_data():
    #------------------------------------------------------------------------
    # Load csv into python as pandas DataFrame
    df_original = load_csv_to_df('src/chesterfield_25-08-2021_09-00-00.csv')

    # Perform data_normalization
    df_transformed = copy_of_original_data(df_original)
    df_transformed = create_hash_id(df_transformed, "order_id")
    df_transformed = products_price_explode(df_transformed, "productsprice", ",")
    df_transformed = add_product_price_colume(df_transformed)
    df_transformed = drop_column(df_transformed, "card_number")
    df_transformed = drop_column(df_transformed, "fullname")
    df_transformed = set_index(df_transformed, "order_id")
    df_transformed = clean_spaces(df_transformed)
    return df_transformed
    #print(df_transformed)

def products():
    df_transformed = load_data()
    product_df = create_product_df(df_transformed)
    return product_df.to_dict('series')
    
def location():
    df_transformed = load_data()
    location_df = create_location_df(df_transformed)
    return location_df.to_dict()

def orders():
    df_transformed = load_data()
    orders_df = create_orders_df(df_transformed)
    return orders_df

def orders_products():
    df_transformed = load_data()
    orders_products_df = create_orders_products_df(df_transformed)
    return orders_products_df

'''
# Upload location_df to SQL
engine = create_engine("postgresql://team4gp:team4pw@localhost:5432")
try:
    location_df.to_sql("cafe", engine, if_exists="append", index=False)
except:
    pass

# Download location_df to SQL
location_df_from_sql = pd.read_sql("cafe", engine,index_col="cafe_id")
print(location_df_from_sql)

location_dict = location_df_from_sql.to_dict()["location"]
location_dict = {y:x for x,y in location_dict.items()}
print(location_dict)


orders_df = copy_of_original_data(df_transformed)
orders_df["location"].replace(location_dict, inplace=True)
print(orders_df)
'''
