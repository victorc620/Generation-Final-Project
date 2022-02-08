from db3 import *
from data_1 import *
from location_cafe import *
from products import *

#FUNCTION FOR INSERT ON TABLE ORDER
def insert_order():
    dic_ord = orders()
    order_ids = dic_ord['datetime']
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

#FUNCTION FOR INSRT ON TABLE ORDERS_PRODUCTS
def insert_order_prd():
    #import pdb; pdb.set_trace()
    dic_ord = orders_products()
    order_ids = dic_ord['order_id']
    for ids, order_id in order_ids.items():
        #import pdb; pdb.set_trace()
        prd_name = dic_ord['products'][ids]
        prd_id = get_product_id(prd_name)
        prd_qtt = dic_ord['quantity_purchased'][ids]
        cln = "order_id, product_id, quantity_purchased"
        att = order_id, prd_id[0][0], prd_qtt
        insert("orders_products", cln, att)           
            
#GET ORDER_ID
def get_order_id(order_id):
    #import pdb; pdb.set_trace()
    w = f"order_id='{order_id}'"
    id = select("orders", where = w)
    return id