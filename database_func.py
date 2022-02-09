import psycopg2
import sqlalchemy
import pandas as pd

def fetch_sql_db(sql: str, val=None):
    """Load data from database to python"""
    connection = psycopg2.connect(host= "localhost", user= "team4gp", password = "team4pw", database = "team4gp", port = "5432")
    cursor = connection.cursor()
    cursor.execute(sql, val)
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    return rows
    
def execute_sql_db(sql: str, val=None):
    """execture PostgreSQL command in database"""
    connection = psycopg2.connect(host= "localhost", user= "team4gp", password = "team4pw", database = "team4gp", port = "5432")
    cursor = connection.cursor()
    cursor.execute(sql, val)
    connection.commit()
    cursor.close()
    connection.close()
    
def insert_into_cafe(location_df: pd.DataFrame, engine):
    """
    Insert location_df into cafe table in database
    1. Insert data to temp table
    2. Insert data (that is not in cafe table) from temp table
    3. Drop temp table
    """
    location_df.to_sql(name = "cafe_temp", con=engine, index=True, if_exists='replace')

    sql = "INSERT INTO cafe SELECT * FROM cafe_temp WHERE cafe_id NOT IN (SELECT cafe_id FROM cafe)"
    execute_sql_db(sql)
    execute_sql_db("DROP TABLE cafe_temp")

def insert_into_products(product_df: pd.DataFrame, engine):
    """
    Insert products_df into products table in database
    1. Insert data to temp table
    2. Insert data (that is not in products table) from temp table
    3. Drop temp table
    """
    product_df.to_sql(name = "products_temp", con=engine, index=True,
                    if_exists='replace', dtype={"product_price": sqlalchemy.types.Float})

    sql = "INSERT INTO products SELECT * FROM products_temp WHERE product_id NOT IN (SELECT product_id FROM products)"
    execute_sql_db(sql)
    execute_sql_db("DROP TABLE products_temp")
    
def insert_into_orders(orders_df: pd.DataFrame, engine):
    """
    Insert orders_df into orders table in database
    1. Insert data to orders_temp table
    2. Create orders_temp_2 by joining database cafe table with orders_temp table
    3. Insert data (that is not in orders table) from orders_temp_2
    4. Drop orders_temp and orders_temp_2 table
    """
    orders_df.to_sql(name = "orders_temp", con=engine, index=True,
                    if_exists='replace', dtype={"date": sqlalchemy.DateTime()})

    execute_sql_db("DROP TABLE IF EXISTS orders_temp_2")
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
    """
    Insert orders_products_df into orders table in database
    1. Insert data to op_temp table
    2. Create op_temp_2 by joining database products table with op_temp_2
    3. Insert data (that is not in orders_products table) from op_temp_2
    4. Drop op_temp and orders_temp_2 table
    """
    orders_products_df.to_sql(name = "op_temp", con=engine, index=False,
                    if_exists='replace', dtype={"quantity_purchased": sqlalchemy.types.Float})

    execute_sql_db("DROP TABLE IF EXISTS op_temp_2")
    sql = """CREATE TABLE op_temp_2 AS (
            SELECT order_id, products.product_id, quantity_purchased
            FROM op_temp
            INNER JOIN products
            ON products.products = op_temp.products
            );
            DROP TABLE op_temp"""
    execute_sql_db(sql)

    sql = "INSERT INTO orders_products SELECT * FROM op_temp_2 WHERE order_id NOT IN (SELECT order_id FROM orders_products)"
    execute_sql_db(sql)
    
    execute_sql_db("DROP TABLE op_temp_2")