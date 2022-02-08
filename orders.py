from db3 import *
from data_1 import *
from location_cafe import *
from products import *

#FUNCTION FOR INSRT ON TABLE ORDER
def insert_order():
    dic_ord = orders()
    order_ids = dic_ord['datetime']
    id = 0
    for ids, dt_time in order_ids.items():
        ord_id = get_order_id(ids)
        if not ord_id:
            cafe_name = dic_ord['location'][ids]
            cafe_id = get_cafe_id(cafe_name)
            payment = dic_ord['payment_type'][ids]
            total_price = dic_ord['total_price'][ids]
            cln = "order_id, cafe_id, date, payment_type, total_price"
            att = str(ids), cafe_id[0][0], str(dt_time), str(payment), str(total_price)
            insert("orders", cln, att)
        id+=1

#FUNCTION FOR INSRT ON TABLE ORDERS PRODUCTS
def insert_order_prd():
    ord = load_data()
    dic_ord = ord.to_dict('series')
    order_ids = dic_ord['datetime']
    id = 0
    for ids, dt_time in order_ids.items():
        ord_id = get_order_id(ids)
        prd_name = dic_ord['products'][id]
        prd_id = get_product_id(prd_name)
        cln = "order_id, product_id"
        att = str(ids), prd_id[0][0]
        insert("orders_products", cln, att)
        id+=1            
            
#GET ORDER_ID
def get_order_id(order_id):
    #import pdb; pdb.set_trace()
    w = f"order_id='{order_id}'"
    id = select("orders", where = w)
    return id