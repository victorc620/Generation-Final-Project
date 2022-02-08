from db3 import *
from data_1 import *


#CREAT FUNCTION FOR INSRT LOCATION ON TABLE CAFE
def insert_cafe():
    loc = location()
    dic = loc['location']
    for i, d in dic.items():
        cafe_id = get_cafe_id(d)
        if not cafe_id:
            att = f"('{i}', '{d}')"
            id = insert("cafe", "cafe_id, location", att)

#GET CAFE_ID
def get_cafe_id(local):
    w = f"location='{local}'"
    cafe_id = select("cafe", where = w)
    return cafe_id