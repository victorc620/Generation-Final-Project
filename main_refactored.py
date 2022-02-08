import imp
import psycopg2
import pandas as pd
from datetime import datetime
import hashlib
import sqlalchemy
from sqlalchemy import create_engine


##########################################################################################################################################
# DATABASE I/O FUNCTION

def fetch_sql_db(sql, val=None):
    connection = psycopg2.connect(host= "localhost", user= "team4gp", password = "team4pw", database = "team4gp", port = "5432")
    cursor = connection.cursor()
    cursor.execute(sql, val)
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    print(rows)
    
def execute_sql_db(sql, val=None):
    connection = psycopg2.connect(host= "localhost", user= "team4gp", password = "team4pw", database = "team4gp", port = "5432")
    cursor = connection.cursor()
    cursor.execute(sql, val)
    connection.commit()
    cursor.close()
    connection.close()
    
def insert_into_cafe(location_df, engine):
    location_df.to_sql(name = "cafe_temp", con=engine, index=True, if_exists='replace')

    sql = "INSERT INTO cafe SELECT * FROM cafe_temp WHERE cafe_id NOT IN (SELECT cafe_id FROM cafe)"
    execute_sql_db(sql)
    execute_sql_db("DROP TABLE cafe_temp")

def insert_into_products(product_df, engine):
    product_df.to_sql(name = "products_temp", con=engine, index=True,
                    if_exists='replace', dtype={"product_price": sqlalchemy.types.Float})

    sql = "INSERT INTO products SELECT * FROM products_temp WHERE product_id NOT IN (SELECT product_id FROM products)"
    execute_sql_db(sql)
    execute_sql_db("DROP TABLE products_temp")
    
def insert_into_orders(orders_df, engine):
    orders_df.to_sql(name = "orders_temp", con=engine, index=True,
                    if_exists='replace', dtype={"date": sqlalchemy.DateTime()})

    sql = """CREATE TABLE orders_temp_2 AS (
            SELECT order_id, cafe.cafe_id, datetime, payment_type, total_price
            FROM orders_temp
            INNER JOIN cafe
            ON cafe.location = orders_temp.location
            );
            DROP TABLE orders_temp"""
    execute_sql_db(sql)

    sql = "INSERT INTO orders SELECT * FROM orders_temp_2 WHERE order_id NOT IN (SELECT order_id FROM orders)"
    execute_sql_db(sql)

    sql = "DROP TABLE orders_temp_2"
    execute_sql_db(sql)

##########################################################################################################################################
# DATA NORMALISATION & CLEANSING FUNCTION

def basic_transform(df_original):
    df_transformed = copy_of_original_data(df_original)
    df_transformed = create_hash_id(df_transformed, "order_id")
    df_transformed = products_price_explode(df_transformed, "productsprice", ",")
    df_transformed = add_product_price_colume(df_transformed)
    df_transformed = drop_column(df_transformed, "card_number")
    df_transformed = drop_column(df_transformed, "fullname")
    df_transformed = set_index(df_transformed, "order_id")
    df_transformed = clean_spaces(df_transformed)
    return df_transformed

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
    location_df = create_hash_id(location_df, "cafe_id")
    location_df.set_index("cafe_id",inplace=True)
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

##########################################################################################################################################
# MAIN 

# 1. Create different dataframe
df_original = load_csv_to_df('src/chesterfield_25-08-2021_09-00-00.csv')
df_transformed = basic_transform(df_original)

product_df = create_product_df(df_transformed)
location_df = create_location_df(df_transformed)
orders_df = create_orders_df(df_transformed)
orders_products_df = create_orders_products_df(df_transformed)


# 2. Insert dataframe into database
engine = create_engine('postgresql://team4gp:team4pw@localhost:5432/team4gp')
insert_into_cafe(location_df, engine)
insert_into_products(product_df, engine)
insert_into_orders(orders_df, engine)


# TODO Handle orders table primary key issues
def insert_into_orders_products(orders_products_df, engine):
    orders_products_df.to_sql(name = "orders_products_temp", con=engine, index=False,
                    if_exists='replace', dtype={"quantity_purchased": sqlalchemy.types.Float})

    sql = """CREATE TABLE op_temp_2 AS (
            SELECT order_id, products.product_id, quantity_purchased
            FROM orders_products_temp
            INNER JOIN products
            ON products.products = orders_products_temp.products
            );
            DROP TABLE orders_products_temp"""
    execute_sql_db(sql)

    sql = "INSERT INTO orders_products SELECT * FROM op_temp_2 WHERE order_id NOT IN (SELECT order_id FROM orders_products)"
    execute_sql_db(sql)
    
    execute_sql_db("DROP TABLE op_temp_2")

insert_into_orders_products(orders_products_df, engine)

    
    

