
from operator import index
from unicodedata import name
from black import assert_equivalent
import pandas as pd
from unittest.mock import Mock
import pandas as pd 
from pandas.testing import assert_frame_equal, assert_series_equal
from pytest import PytestAssertRewriteWarning
import hashlib
from datetime import datetime

def drop_column(df_arg, column):
    "Drop any column in the DataFrame"
    df_arg.drop(column, inplace=True, axis=1)
    return df_arg
    

def test_drop_column():
    # assemble
    df = pd.DataFrame({"foo": [1, 2, 3], "bar": ['a', 'b', 'c']}) 
    expected = pd.DataFrame({"foo": [1, 2, 3]})
    #act
    actual = drop_column(df, "bar")   
    #assert
    assert_frame_equal(expected, actual)

#----------------------------------------------------------------------------------------------------------------
    
def set_index(df_arg,column):
    "Set specific column as index"
    df_arg.set_index(column, inplace = True)
    return df_arg


def test_set_index():
    #assemble 
    idx = pd.Index(['a', 'b', 'c'], name='bar')
    df = pd.DataFrame({"foo": [1, 2, 3], "bar": ['a', 'b', 'c']}) 
    expected = pd.DataFrame({"foo": [1, 2, 3]}, index=idx)
    #act
    actual = set_index(df, "bar")
    #assert 
    assert_frame_equal(expected, actual)
#-----------------------------------------------------------------------------------------------------------------

def products_price_explode(df_arg, column, split_criteria):
    "Perform 1NF explode to products+price where each role contain one product only"
    df_arg[column] = df_arg[column].map(str)
    df_arg[column] = df_arg[column].str.split(split_criteria)
    df_arg = df_arg.explode(column)
    return df_arg


def test_product_price_explode():
    # assemble
    data = {'col1':[1,1,2,2],'col2':["a","b","c","d"]}
    d = {'col1':[1,2], 'col2':["a,b","c,d"]}
    df = pd.DataFrame(data=d)
    idx = pd.Index([0, 0, 1, 1])
    expected = pd.DataFrame(data=data, index=idx)
    # act
    result = products_price_explode(df, 'col2', ',')
    print(result)
    #assert 
    assert_frame_equal(expected, result, check_dtype=False)

#-------------------------------------------------------------------------------------------------------------------------------------
def add_product_price_colume(df_arg):
    """
    Add 'product_price' colume for product price
    Rename column "products+price" to "products"
    """
    df_arg["product_price"] = df_arg["products+price"].str.split(" - ").str[-1]
    df_arg = remove_price_from_products(df_arg)
    df_arg.rename(columns={'products+price':'products'}, inplace=True)
    return df_arg

def test_add_product_price_column():
    #assemble
    data = {'products+price':['coffee - 12', 'tea - 10', 'water - 8']}
    df = pd.DataFrame(data=data)
    expected = pd.DataFrame({'products': ['coffee', 'tea', 'water'], 'product_price':['12', '10', '8']})
    #act
    result = add_product_price_colume(df)
    #assert
    assert_frame_equal(expected, result)
    

#-------------------------------------------------------------------------------------------------------------------------------------
def remove_price_from_products(df_arg):
    df_arg["products+price"] = df_arg["products+price"].map(lambda x:x.rstrip(' -0123456789.'))
    return df_arg

def test_remove_price_from_products():
    #assemble 
    data = {'products+price':['coffee - 12', 'tea - 10', 'water - 8']}
    df = pd.DataFrame(data=data)
    expected = pd.DataFrame({'products+price': ['coffee', 'tea', 'water']})
    #act
    result = remove_price_from_products(df)
    #assert
    assert_frame_equal(expected, result)

#-------------------------------------------------------------------------------------------------------------------------------------

def create_orders_products_df(df_transformed: pd.DataFrame):
    """Generate a orders_df that ready to be uploaded to orders_products table in database"""
    orders_products_df = df_transformed[["products"]]
    orders_products_df = orders_products_df.groupby(["order_id","products"]).size()
    orders_products_df = orders_products_df.reset_index(name="quantity_purchased")
    return orders_products_df

def test_create_orders_products_df():
    #assemble 
    ind = pd.Index([0], name=None)
    df = pd.DataFrame({'order_id': ['test'], 'products': ['test'], 'quantity_purchased': [2]}, index=ind)
    expected = df
    #act
    ind1 = pd.Index(['test', 'test'], name='order_id')
    df1 = pd.DataFrame({'datetime': ['0000-00-00 00:00:00', '0000-00-00 00:00:00'], 'location': ['test', 'test'], 'products': ['test', 'test'], 'payment_type': ['test', 'test'], 'product_price': [1, 1]}, index=ind1)
    actual = create_orders_products_df(df1)
    #assert 
    assert_frame_equal(expected, actual)

   

#--------------------------------------------------------------------------------------------------------------------------------------
def create_orders_df(df_transformed: pd.DataFrame):
    """Generate a orders_df that ready to be uploaded to orders table in database"""
    orders_df = df_transformed[["location","datetime","payment_type","total_price"]]
    orders_df = orders_df.drop_duplicates()
    return orders_df

def test_create_orders_df():
    #assemble 
    ind = pd.Index([0], name='cafe_id')
    expected = pd.DataFrame({'location':['London'], 'datetime':['01-01-2021'], 'payment_type':['cash'], 'total_price':[12]}, index=ind)
    ind1 = pd.Index([0, 1], name='cafe_id')
    df = pd.DataFrame({'location':['London', 'London'], 'datetime':['01-01-2021', '01-01-2021'], 'payment_type':['cash', 'cash'], 'total_price':[12, 12]}, index=ind1)
    #act
    result = create_orders_df(df)
    #assert
    assert_frame_equal(expected, result)
    
#--------------------------------------------------------------------------------------------------------------------------------------

def create_location_df(df_transformed: pd.DataFrame):
    """Generate a location_df that ready to be uploaded to location table in database"""
    loction_array = df_transformed["location"].unique()
    location_df = pd.DataFrame(loction_array, columns= ["location"])
    location_df = create_hash_id(location_df, "cafe_id")
    location_df.set_index("cafe_id",inplace=True)
    return location_df

def test_create_location_df():
    #assemble
    ind = pd.Index(['test'], name='cafe_id')
    df = pd.DataFrame({'datetime':['0000-00-00 00:00:00'], 'location': ['test'], 'products':['test'], 'total_price':[0], 'payment_type':['test'], 'product_price':[0]}, index=ind)
    edf = pd.DataFrame({'location': ['test']}, index=ind)
    expected = edf
    #act
    actual = create_location_df(df)
    #assert
    """droped index here because it's impossible to assert a hashed value"""
    assert_frame_equal(expected.reset_index(drop=True), actual.reset_index(drop=True))
    

#--------------------------------------------------------------------------------------------------------------------------------------

def create_product_df(df_transformed: pd.DataFrame):
    """Generate a product_df that ready to be uploaded to products table in database"""
    product_df = df_transformed[["products","product_price"]]
    product_df.reset_index(inplace=True)
    product_df = product_df.drop(columns = "order_id")
    product_df = product_df.drop_duplicates(subset=['products'])
    product_df = create_hash_id(product_df , "product_id")
    product_df.set_index("product_id", inplace=True)
    return product_df

def test_create_product_df():
    #assemble
    ind = pd.Index(['test'], name='product_id')
    df = pd.DataFrame({'products': ['test'], 'product_price': [0]}, index=ind)
    expected = df
    #act 
    ind1 = pd.Index([0], name='order_id')
    df1 = pd.DataFrame({'datetime':['0000-00-00 00:00:00'], 'location': ['test'], 'products':['test'], 'total_price':[0], 'payment_type':['test'], 'product_price':[0]}, index=ind1)
    actual = create_product_df(df1)
    #assert 
    """droped index here because it's impossible to assert a hashed value"""
    assert_frame_equal(expected.reset_index(drop=True), actual.reset_index(drop=True))


#---------------------------------------------------------------------------------------------------------------------------------------


def clean_spaces(df_args: pd.DataFrame):
    """Data cleansing: Remove left white_space of products"""
    df_args['products'] = df_args['products'].map(lambda x:x.lstrip())
    return df_args

def test_clean_spaces():
    #assemble 
    df = pd.DataFrame({'products':[' coffee', ' tea', ' water']})
    expected = pd.DataFrame({'products':['coffee', 'tea', 'water']})
    #act
    result  = clean_spaces(df)
    #assert
    assert_frame_equal(expected, result)

#----------------------------------------------------------------------------------------------------------------------------------------

def copy_of_original_data(df_arg: pd.DataFrame):
    """make a copy of a dataframe"""
    df_copy = df_arg.copy()
    return df_copy

def test_copy_original_data():
    #assemble
    df = pd.DataFrame({'products':['coffee', 'tea', 'water']})
    expected = pd.DataFrame({'products':['coffee', 'tea', 'water']})
    #act
    result = copy_of_original_data(df)
    #assert
    assert_frame_equal(expected, result)

#---------------------------------------------------------------------------------------------------------------------------------------

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

def test_load_csv_to_df():
    #assemble 
    expected = pd.DataFrame({'datetime': [2021-01-01 01:01:00], 'location': ['test'], 'fullname': ['test'], 'productsprice': [0], 'total_price': [0], 'payment_type': ['test'], 'card_number': [0000]}, datetime='datetime')
    #act
    actual = load_csv_to_df('/media/abali/New Volume/Group project/test/test_data.csv')
    #assert 
    assert_frame_equal(expected, actual)

#---------------------------------------------------------------------------------------------------------------------------------------

def create_hash_id(df_arg: pd.DataFrame, column:str):
    """
    Generate a hash id based on the original data (before removing any data)
    hashing method still waiting to be updated
    """
    df_arg[column]=df_arg.astype(str).sum(1).apply(lambda x:hashlib.md5(x.encode()).hexdigest())
    return df_arg

def test_create_hash_id():
    #assemble 
    df = pd.DataFrame({'test': ['test']})
    expected = df
    #act
    actual = create_hash_id(df, 'test')
    #assert
    assert_frame_equal(expected, actual)


