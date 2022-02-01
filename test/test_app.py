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
    # arrange
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
    #arrange 
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
    # Assemble
    data = {'col1':[1,1,2,2],'col2':["a","b","c","d"]}
    d = {'col1':[1,2], 'col2':["a,b","c,d"]}
    df = pd.DataFrame(data=d)
    mock_product_price_explode = Mock()
    idx = pd.Index([0, 0, 1, 1])
    mock_product_price_explode.return_value = pd.DataFrame(data=data, index=idx)
    expected = mock_product_price_explode()
    # Act
    result = products_price_explode(df, 'col2', ',')
    print(result)

    #Assert 
    assert_frame_equal(expected, result, check_dtype=False)

