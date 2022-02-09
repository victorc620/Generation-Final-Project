import psycopg2
import sqlalchemy

def fetch_sql_db(sql, val=None):
    connection = psycopg2.connect(host= "localhost", user= "team4gp", password = "team4pw", database = "team4gp", port = "5432")
    cursor = connection.cursor()
    cursor.execute(sql, val)
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    print(rows)
    
def execute_sql_db(sql, val=None):
    connection = psycopg2.connect(host= "localhost", user= "team4gp", password = "team4pw", database = "team4gp", port = "5432")
    cursor = connection.cursor()
    cursor.execute(sql, val)
    connection.commit()
    cursor.close()
    connection.close()
    
def insert_into_cafe(location_df, engine):
    location_df.to_sql(name = "cafe_temp", con=engine, index=True, if_exists='replace')

    sql = "INSERT INTO cafe SELECT * FROM cafe_temp WHERE cafe_id NOT IN (SELECT cafe_id FROM cafe)"
    execute_sql_db(sql)
    execute_sql_db("DROP TABLE cafe_temp")

def insert_into_products(product_df, engine):
    product_df.to_sql(name = "products_temp", con=engine, index=True,
                    if_exists='replace', dtype={"product_price": sqlalchemy.types.Float})

    sql = "INSERT INTO products SELECT * FROM products_temp WHERE product_id NOT IN (SELECT product_id FROM products)"
    execute_sql_db(sql)
    execute_sql_db("DROP TABLE products_temp")
    
def insert_into_orders(orders_df, engine):
    orders_df.to_sql(name = "orders_temp", con=engine, index=True,
                    if_exists='replace', dtype={"date": sqlalchemy.DateTime()})

    sql = """CREATE TABLE orders_temp_2 AS (
            SELECT order_id, cafe.cafe_id, datetime, payment_type, total_price
            FROM orders_temp
            INNER JOIN cafe
            ON cafe.location = orders_temp.location
            );
            DROP TABLE orders_temp"""
    execute_sql_db(sql)

    sql = "INSERT INTO orders SELECT * FROM orders_temp_2 WHERE order_id NOT IN (SELECT order_id FROM orders)"
    execute_sql_db(sql)

    sql = "DROP TABLE orders_temp_2"
    execute_sql_db(sql)

def insert_into_orders_products(orders_products_df, engine):
    orders_products_df.to_sql(name = "orders_products_temp", con=engine, index=False,
                    if_exists='replace', dtype={"quantity_purchased": sqlalchemy.types.Float})

    sql = """CREATE TABLE op_temp_2 AS (
            SELECT order_id, products.product_id, quantity_purchased
            FROM orders_products_temp
            INNER JOIN products
            ON products.products = orders_products_temp.products
            );
            DROP TABLE orders_products_temp"""
    execute_sql_db(sql)

    sql = "INSERT INTO orders_products SELECT * FROM op_temp_2 WHERE order_id NOT IN (SELECT order_id FROM orders_products)"
    execute_sql_db(sql)
    
    execute_sql_db("DROP TABLE op_temp_2")