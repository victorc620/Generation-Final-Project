import pandas as pd
import numpy as np

df = pd.read_csv('./csv/chesterfield_25-08-2021_09-00-00.csv', names = ["datetime","Location","fullname", "products+price", "total_price","payment_type","card_number"])
# df.insert(0, "Main Key", range(1, 1+ len(df)))

df.drop('card_number', inplace=True, axis=1)
df.drop('fullname', inplace=True, axis=1)

# Create function to do 1NF on product & price
def products_price_explode(df_arg):
    "Perform 1NF explode to products+price where each role contain one product only"
    df_arg["products+price"] = df_arg["products+price"].map(str)
    df_arg["products+price"] = df_arg["products+price"].str.split(",")
    df_arg = df_arg.explode("products+price")
    return df_arg

def add_product_price_colume(df_arg):
    "Add 'product_price' colume for product price"
    df_arg["product_price"] = df_arg["products+price"].str.split(" - ").str[-1]
    df_arg = remove_price_from_products(df_arg)
    return df_arg

def remove_price_from_products(df_arg):
    df_arg["products+price"] = df_arg["products+price"].map(lambda x:x.rstrip(' -0123456789.'))
    return df_arg

df_1nf = products_price_explode(df)
d1_inf = add_product_price_colume(df_1nf)
print(df_1nf)

