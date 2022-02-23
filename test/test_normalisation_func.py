from cmath import nan
import sys
sys.path.append("/home/runner/work/team-4-project/team-4-project")
from lambda_normalisation import *
from pandas.testing import assert_frame_equal, assert_index_equal

#---------------------------------------------------------------------------------------------------

def test_create_hash_id():
    #assemble 
    df = pd.DataFrame({'test': ['test']})
    expected = df
    #act
    actual = create_hash_id(df, 'test')
    #assert
    assert_frame_equal(expected, actual)
    
#-------------------------------------------------------------------------------------------------------------------------------------

def test_drop_column():
    # assemble
    df = pd.DataFrame({"foo": [1, 2, 3], "bar": ['a', 'b', 'c']}) 
    expected = pd.DataFrame({"foo": [1, 2, 3]})
    #act
    actual = drop_column(df, "bar")   
    #assert
    assert_frame_equal(expected, actual)
    
#-----------------------------------------------------------------------------------------------------------------------------------

def test_set_index():
    #assemble 
    idx = pd.Index(['a', 'b', 'c'], name='bar')
    df = pd.DataFrame({"foo": [1, 2, 3], "bar": ['a', 'b', 'c']}) 
    expected = pd.DataFrame({"foo": [1, 2, 3]}, index=idx)
    #act
    actual = set_index(df, "bar")
    #assert 
    assert_frame_equal(expected, actual)
    
#-----------------------------------------------------------------------------------------------------------------------------------

def test_set_index():
    #assemble 
    idx = pd.Index(['a', 'b', 'c'], name='bar')
    df = pd.DataFrame({"foo": [1, 2, 3], "bar": ['a', 'b', 'c']}) 
    expected = pd.DataFrame({"foo": [1, 2, 3]}, index=idx)
    #act
    actual = set_index(df, "bar")
    #assert 
    assert_frame_equal(expected, actual)

#------------------------------------------------------------------------------------------------------------------------------------

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

def test_add_product_price_column():
    #assemble
    data = {'productsprice':['coffee - 12', 'tea - 10', 'water - 8']}
    df = pd.DataFrame(data=data)
    expected = pd.DataFrame({'products': ['coffee', 'tea', 'water'], 'product_price':['12', '10', '8']})
    #act
    result = add_product_price_colume(df)
    #assert
    assert_frame_equal(expected, result)
    
#----------------------------------------------------------------------------------------------------------------------------------------

def test_remove_price_from_products():
    #assemble 
    data = {'productsprice':['coffee - 12', 'tea - 10', 'water - 8']}
    df = pd.DataFrame(data=data)
    expected = pd.DataFrame({'productsprice': ['coffee', 'tea', 'water']})
    #act
    result = remove_price_from_products(df)
    #assert
    assert_frame_equal(expected, result)
    
#--------------------------------------------------------------------------------------------------------------------------------------

def test_copy_original_data():
    #assemble
    df = pd.DataFrame({'products':['coffee', 'tea', 'water']})
    expected = pd.DataFrame({'products':['coffee', 'tea', 'water']})
    #act
    result = copy_of_original_data(df)
    #assert
    assert_frame_equal(expected, result)
    
#--------------------------------------------------------------------------------------------------------------------------------------

def test_clean_spaces():
    #assemble 
    df = pd.DataFrame({'products':[' coffee', ' tea', ' water']})
    expected = pd.DataFrame({'products':['coffee', 'tea', 'water']})
    #act
    result  = clean_spaces(df)
    #assert
    assert_frame_equal(expected, result)
    
#--------------------------------------------------------------------------------------------------------------------------------------

def test_create_product_df():
    #assemble
    # ind = pd.Index(['test'], name='product_id')
    df = pd.DataFrame({'product_id': ['f6f4061a1bddc1c04d8109b39f581270'],'products': ['test'], 'product_price': [0]})
    expected = df
    #act 
    ind1 = pd.Index([0], name='order_id')
    df1 = pd.DataFrame({'datetime':['0000-00-00 00:00:00'], 'location': ['test'], 'products':['test'], 'total_price':[0], 'payment_type':['test'], 'product_price':[0]}, index=ind1)
    actual = create_product_df(df1)
    #assert 
    """droped index here because it's impossible to assert a hashed value"""
    assert_frame_equal(expected.reset_index(drop=True), actual.reset_index(drop=True), check_dtype=False)

#----------------------------------------------------------------------------------------------------------------------------------------

def test_create_location_df():
    #assemble
    ind = pd.Index(['test'], name='cafe_id')
    df = pd.DataFrame({'datetime':['0000-00-00 00:00:00'], 'location': ['test'], 'products':['test'], 'total_price':[0], 'payment_type':['test'], 'product_price':[0]}, index=ind)
    expected = pd.DataFrame({'cafe_id': ['098f6bcd4621d373cade4e832627b4f6'], 'location': ['test']})
    #act
    actual = create_location_df(df)
    #assert
    """droped index here because it's impossible to assert a hashed value"""
    assert_frame_equal(expected.reset_index(drop=True), actual.reset_index(drop=True))
    
#------------------------------------------------------------------------------------------------------------------------------------------

def test_create_orders_df():
    #assemble 
    ind = pd.Index([0], name='cafe_id')
    expected = pd.DataFrame({'location':['test'], 'datetime':['01-01-2021'], 'payment_type':['cash'], 'total_price':[12]}, index=ind)
    ind1 = pd.Index([0, 1], name='cafe_id')
    df = pd.DataFrame({'location':['London', 'London'], 'datetime':['01-01-2021', '01-01-2021'], 'payment_type':['cash', 'cash'], 'total_price':[12, 12]}, index=ind1)
    #act
    location_df = pd.DataFrame({'cafe_id': ['098f6bcd4621d373cade4e832627b4f6'], 'location': ['test']})
    result = create_orders_df(df, location_df)
    #assert
    assert_frame_equal(expected, result)
    
#-------------------------------------------------------------------------------------------------------------------------------------------

def test_create_orders_products_df():
    #assemble 
    ind = pd.Index([0], name=None)
    df = pd.DataFrame({'order_id': ['test'], 'product_id': ['f6f4061a1bddc1c04d8109b39f581270'], 'quantity_purchased': [2]}, index=ind)
    expected = df
    #act
    ind1 = pd.Index(['test', 'test'], name='order_id')
    df1 = pd.DataFrame({'datetime': ['0000-00-00 00:00:00', '0000-00-00 00:00:00'], 'location': ['test', 'test'], 'products': ['test', 'test'], 'payment_type': ['test', 'test'], 'product_price': [1, 1]}, index=ind1)
    product_df = pd.DataFrame({'product_id': ['f6f4061a1bddc1c04d8109b39f581270'],'products': ['test'], 'product_price': [0]})
    actual = create_orders_products_df(df1, product_df=product_df)
    #assert 
    assert_frame_equal(expected, actual)
    
#-----------------------------------------------------------------------------------------------------------------------------------------

def test_create_orders_df():
    #assemble 
    ind = pd.Index([0], name='cafe_id')
    expected = pd.DataFrame({'order_id': [nan],'cafe_id':[nan], 'date':['01-01-2021'], 'payment_type':['cash'], 'total_price':[12]})
    ind1 = pd.Index([0, 1], name='cafe_id')
    df = pd.DataFrame({'location':['London', 'London'], 'datetime':['01-01-2021', '01-01-2021'], 'payment_type':['cash', 'cash'], 'total_price':[12, 12]}, index=ind1)
    #act
    location_df = pd.DataFrame({'cafe_id': ['098f6bcd4621d373cade4e832627b4f6'], 'location': ['test']})
    result = create_orders_df(df, location_df=location_df)
    #assert
    assert_frame_equal(expected, result, check_dtype=False)
