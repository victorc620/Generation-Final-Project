import pandas as pd
from data_normalization import products_price_explode, drop_column
from pandas.testing import assert_frame_equal

def test_product_price_explode():
    # Assemble
    d = {'col1':[1,2], 'col2':["a,b","c,d"]}
    df = pd.DataFrame(data=d)
    expected_data = {'col1':[1,1,2,2],'col2':["a","b","c","d"]}
    expected = pd.DataFrame(data=expected_data)
    expected.set_index('col1',inplace=True)
    
    # Act
    result = products_price_explode(df, "col2", ",")
    result.set_index('col1',inplace=True)
    
    #Assert 
    assert_frame_equal(result, expected)
    
def test_drop_column():
    d = {'col1':[1,2], 'col2':["a","b"]}
    df = pd.DataFrame(data=d)
    
    expected_data = {'col2':["a","b"]}
    expected = pd.DataFrame(data=expected_data)
    
    result = drop_column(df, 'col1')
    
    assert_frame_equal(result,expected)