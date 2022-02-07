import pandas as pd
import hashlib
from datetime import datetime

def create_hash_id(df_arg: pd.DataFrame):
    """
    Generate a hash id based on the original data (before removing any data)
    hashing method still waiting to be updated
    """
    df_arg["order_id"]=df_arg.astype(str).sum(1).apply(lambda x:hashlib.md5(x.encode()).hexdigest())
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
    print(df_func)
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

#------------------------------------------------------------------------
# Load csv into python as pandas DataFrame
df_original = load_csv_to_df('team-4-project/src/chesterfield_25-08-2021_09-00-00.csv')

# Perform data_normalization
df_transformed = copy_of_original_data(df_original)
df_transformed = create_hash_id(df_transformed)
df_transformed = products_price_explode(df_transformed, "productsprice", ",")
df_transformed = add_product_price_colume(df_transformed)
df_transformed = drop_column(df_transformed, "card_number")
df_transformed = drop_column(df_transformed, "fullname")
df_transformed = set_index(df_transformed, "order_id")
df_transformed = clean_spaces(df_transformed)
print(df_transformed) 


# products_list = df_transformed["payment_type"].unique()
# print(products_list)
# print(len(products_list))

temp_df = df_transformed[["order_id","datetime", "payment_type", "total_price"]]
print(temp_df)

# tpls = [tuple(x) for x in temp_df.to_numpy()]
# print(tpls[0:2])
# cols = ','.join(list(temp_df.columns))
# print(cols)

# print(type(temp_df))
# print(type(df_transformed))