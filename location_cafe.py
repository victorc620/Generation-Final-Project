from db3 import *
from data_1 import *


#CREAT FUNCTION FOR INSRT LOCATION ON TABLE CAFE
def insert_cafe():
    loc = location()
    dic = loc['location']
    for d in dic.values():
        cafe_id = get_cafe_id(d)
        if not cafe_id:
            id = insert("cafe", "location", f"('{d}')")

            
#GET CAFE_ID
def get_cafe_id(local):
    #import pdb; pdb.set_trace()
    w = f"location='{local}'"
    cafe_id = select("cafe", where = w)
    return cafe_id