from db3 import *
from data_1 import *


#FUNCTION FOR INSeRT LOCATION ON TABLE 
def insert_product():
    prd = products()
    dic_prd = prd['products']
    for i, p in dic_prd.items():
        price = prd["product_price"][i]
        product_id = get_product_id(p)
        if not product_id:
            att = f"('{i}','{p}', {price})"
            insert("products", "product_id, products, products_price", att)

#GET PRODUCTS_ID
def get_product_id(product):
    w = f"products='{product}'"
    product_id = select("products", where = w)
    return product_id