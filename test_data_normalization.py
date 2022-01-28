import pandas as pd
from data_normalization import products_price_explode
from pandas.util.testing import assert_frame_equal

def test_product_price_explode():
    # Assemble
    d = {'col1':[1,2], 'col2':["a,d","c,d"]}
    df = pd.DataFrame(data=d)
    expected_data = {'col1':[1,1,2,2],'col2':["a","b","c","d"]}
    expected = pd.DataFrame(data=expected_data)
    
    # Act
    result = products_price_explode(df, "col2", ",")
    print(result)
    
    #Assert 
    assert_frame_equal(result, expected)
    
test_product_price_explode()