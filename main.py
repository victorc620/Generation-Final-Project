import pandas as pd
from sqlalchemy import create_engine
from database_func import *
from data_normalisation_func import *
import sqlalchemy as sa
from sqlalchemy.engine.url import URL

# 1. Create different dataframe
df_original = load_csv_to_df('src/chesterfield_25-08-2021_09-00-00.csv')
df_transformed = basic_transform(df_original)

product_df = create_product_df(df_transformed)
location_df = create_location_df(df_transformed)
orders_df = create_orders_df(df_transformed)
orders_products_df = create_orders_products_df(df_transformed)

# 2. Insert dataframe into database
# create engine location: postgresql://username:password@host:port/database
# TODO Hide Credentials 
load_dotenv()
host = os.environ.get("host")
user = os.environ.get("user")
password = os.environ.get("password")
db = os.environ.get("database")
url = URL.create(
drivername='redshift+redshift_connector', # indicate redshift_connector driver and dialect will be used
host=host, # Amazon Redshift host
port=5439, # Amazon Redshift port
database=db, # Amazon Redshift database
username=user, # Amazon Redshift username
password=password # Amazon Redshift password
)

engine = sa.create_engine(url)

insert_into_cafe(location_df, engine)
insert_into_products(product_df, engine)
insert_into_orders(orders_df, engine)
insert_into_orders_products(orders_products_df, engine)
