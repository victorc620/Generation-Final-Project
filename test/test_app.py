from numpy import int64
import pandas as pd
from unittest.mock import patch, Mock
import pandas as pd 
from pandas.testing import assert_frame_equal

def drop_column(df_arg, column):
    "Drop any column in the DataFrame"
    df_arg.drop(column, inplace=True, axis=1)
    return df_arg
    

def test_drop_column():
    # assemble
    df = pd.DataFrame({"foo": [1, 2, 3], "bar": ['a', 'b', 'c']}) 
    mock_drop = Mock()
    mock_drop.return_value = pd.DataFrame({"foo": [1, 2, 3]})
    expected = mock_drop()
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
    mock_product_price_explode = Mock()
    idx = pd.Index([0, 0, 1, 1])
    mock_product_price_explode.return_value = pd.DataFrame(data=data, index=idx)
    expected = mock_product_price_explode()
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
    mock_remove_price_from_product = Mock()
    mock_pandas_rename = Mock()
    data1 = {'product+price':['coffee', 'tea', 'water']}
    df1 = pd.DataFrame(data=data1)
    mock_remove_price_from_product.return_value = df1
    mock_pandas_rename.return_value = pd.DataFrame({'products': ['coffee', 'tea', 'water'], 'product_price':['12', '10', '8']})
    expected = mock_pandas_rename()
    #act
    result = add_product_price_colume(df)
    #assert
    assert_frame_equal(expected, result)
    pass

#-------------------------------------------------------------------------------------------------------------------------------------
def remove_price_from_products(df_arg):
    df_arg["products+price"] = df_arg["products+price"].map(lambda x:x.rstrip(' -0123456789.'))
    return df_arg

def test_remove_price_from_products():
    #assemble 
    data = {'products+price':['coffee - 12', 'tea - 10', 'water - 8']}
    df = pd.DataFrame(data=data)
    mock_map_lambda_func = Mock()
    mock_map_lambda_func.return_value = pd.DataFrame({'products+price': ['coffee', 'tea', 'water']})
    expected = mock_map_lambda_func()
    #act
    result = remove_price_from_products(df)
    #assert
    assert_frame_equal(expected, result)

    

