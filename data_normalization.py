from base64 import encode
import pandas as pd
import numpy as np
import hashlib
from pandas.util import hash_pandas_object

df_original = pd.read_csv('./csv/chesterfield_25-08-2021_09-00-00.csv', names = ["datetime","Location","fullname", "products+price", "total_price","payment_type","card_number"])

def create_hash_id(df_arg):
    """
    Generate a hash id based on the original data (before removing any data)
    hashing method still waiting to be updated
    """
    df_arg["id"]=df_arg.astype(str).sum(1).apply(lambda x:hashlib.md5(x.encode()).hexdigest())
    return df_arg

    # df_arg["id"] = hash_pandas_object(df_arg)
    # df_arg["id"]=df_arg.astype(str).sum(1).str.encode('utf-8').apply(hashlib.md5)
    # df_arg["id"]=df_arg.astype(str).sum(1).apply(hash)
    # df_arg["id"]=df_arg["id"].astype(int).apply(abs)

def drop_column(df_arg, column):
    "Drop any column in the DataFrame"
    df_arg.drop(column, inplace=True, axis=1)
    return df_arg
    
def set_index(df_arg,column):
    "Set specific column as index"
    df_arg.set_index(column, inplace = True)
    return df_arg

# Create function to do 1NF on product & price
def products_price_explode(df, column, split_criteria):
    "Perform 1NF explode to products+price where each role contain one product only"
    df_arg = df.copy()
    df_arg[column] = df_arg[column].map(str)
    df_arg[column] = df_arg[column].str.split(split_criteria)
    df_arg = df_arg.explode(column)
    return df_arg

def add_product_price_colume(df_arg):
    """
    Add 'product_price' colume for product price
    Rename column "products+price" to "products"
    """
    df_arg["product_price"] = df_arg["products+price"].str.split(" - ").str[-1]
    df_arg = remove_price_from_products(df_arg)
    df_arg.rename(columns={'products+price':'products'}, inplace=True)
    return df_arg

def remove_price_from_products(df_arg):
    df_arg["products+price"] = df_arg["products+price"].map(lambda x:x.rstrip(' -0123456789.'))
    return df_arg


df_transformed = products_price_explode(df_original, "products+price", ",")
df_transformed = add_product_price_colume(df_transformed)
df_transformed = create_hash_id(df_transformed)
df_transformed = drop_column(df_transformed, "card_number")
df_transformed = drop_column(df_transformed, "fullname")
df_transformed = set_index(df_transformed, "id")

print(df_original)
print(df_transformed)

# def seperate_date_time(df_arg):
#     df_arg["date"] = df_arg["datetime"].str.split(" ").str[0]
#     df_arg["time"] = df_arg["datetime"].str.split(" ").str[1]
#     df_arg.drop("datetime", inplace=True, axis =1)
#     return df_arg

# # df_1nf = seperate_date_time(df_1nf)
# print(df_1nf)
