from db3 import *
from data_1 import *


#FUNCTION FOR INSRT LOCATION ON TABLE 
def insert_product():
    prd = products()
    dic_prd = prd['products']
    id = 0
    for p in dic_prd:
        price = prd["product_price"][id]
        product_id = get_product_id(p)
        if not product_id:
            att = f"('{p}', {price})"
            insert("products", "products, products_price", att)
        id+=1

            
#GET PRODUCTS_ID
def get_product_id(product):
    w = f"products='{product}'"
    product_id = select("products", where = w)
    return product_id