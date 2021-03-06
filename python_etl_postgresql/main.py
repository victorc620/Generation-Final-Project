import pandas as pd
from sqlalchemy import create_engine
from database_func import *
from data_normalisation_func import *

# 1. Create different dataframe
df_original = load_csv_to_df('src/chesterfield_25-08-2021_09-00-00.csv')
print(df_original)
# df_transformed = basic_transform(df_original)

# product_df = create_product_df(df_transformed)
# location_df = create_location_df(df_transformed)
# orders_df = create_orders_df(df_transformed)
# orders_products_df = create_orders_products_df(df_transformed)

# # 2. Insert dataframe into database
# # create engine location: postgresql://username:password@host:port/database
# engine = create_engine('postgresql://team4gp:team4pw@localhost:5432/team4gp')

# insert_into_cafe(location_df, engine)
# insert_into_products(product_df, engine)
# insert_into_orders(orders_df, engine)
# insert_into_orders_products(orders_products_df, engine)
