import os
import psycopg2
import pandas as pd


# def connect():
#     load_dotenv()
#     host = os.environ.get("host")
#     user = os.environ.get("user")
#     password = os.environ.get("password")
#     db = os.environ.get("database")
    
#     conn = redshift_connector.connect(
#     host=host,
#     database=db,
#     user=user,
#     password=password
#     )
#     return conn

def connect():
    host="redshiftcluster-bie5pcqdgojl.cje2eu9tzolt.eu-west-1.redshift.amazonaws.com"
    port="5439"
    password="9Aa19754-8433-11ec-9900-b29c4ad2293b"
    user="team4"
    database="team4_cafe"
    
    conn = psycopg2.connect(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password
    )
    return conn

def insert_value(df, table_name, table_temp):
    """
    execture PostgreSQL command to insert data to redshift database
    # TODO: Find out how to pass sql f-string as argument 
    """
    connection = connect()
    connection.autocommit = True
    cursor = connection.cursor()
    
    np_data = df.to_numpy()
    args_str = b','.join(cursor.mogrify(f'({",".join(["%s"] * len(np_data[0]))})', x) for x in tuple(map(tuple,np_data)))
    cols = [x for x in df.columns]
    insert_tmp = f"insert into {table_temp} ({', '.join(cols)}) values {args_str.decode('utf-8')}"
    
    if table_name == "cafe":
        sql = f"""
            CREATE TEMP TABLE cafe_temp(
            cafe_id VARCHAR(256),
            location VARCHAR(256)
            );
        
            {insert_tmp};
        
            INSERT INTO cafe SELECT * FROM cafe_temp WHERE cafe_id NOT IN (SELECT cafe_id FROM cafe);
            """
        print("cafe inserted")
        
    elif table_name == "products":
        sql = f"""
            CREATE TEMP TABLE products_temp(
            product_id VARCHAR(256) NOT NULL,
            products VARCHAR(256) NOT NULL,
            product_price DOUBLE PRECISION NOT NULL
            );
        
            {insert_tmp};
        
            INSERT INTO products SELECT * FROM products_temp WHERE product_id NOT IN (SELECT product_id FROM products);
            """
        print("products inserted")
    
    elif table_name == "orders":
        sql = f"""
            CREATE TEMP TABLE orders_temp(
            order_id VARCHAR(256) NOT NULL,
            location VARCHAR(256) NOT NULL,
            date TIMESTAMP without time zone,
            payment_type VARCHAR(256) NOT NULL,
            total_price double precision NOT NULL
            );
            
            {insert_tmp};
            
            CREATE TEMP TABLE orders_temp_2 AS (
            SELECT order_id, cafe.cafe_id, date, payment_type, total_price
            FROM orders_temp
            INNER JOIN cafe
            ON cafe.location = orders_temp.location
            );
            
            INSERT INTO orders SELECT * FROM orders_temp_2 
            WHERE order_id NOT IN (SELECT order_id FROM orders);
            """
        print("orders inserted")
        
    elif table_name == "orders_products":
        sql =f"""
            CREATE TEMP TABLE orders_products_temp (
            order_id VARCHAR(256) NOT NULL,
            products VARCHAR(256) NOT NULL,
            quantity_purchased INT
            );
            
            {insert_tmp};
            
            CREATE TEMP TABLE orders_products_temp_2 AS (
            SELECT order_id, products.product_id, quantity_purchased
            FROM orders_products_temp
            INNER JOIN products
            ON products.products = orders_products_temp.products
            );
            
            INSERT INTO orders_products SELECT * FROM orders_products_temp_2 WHERE order_id NOT IN (SELECT order_id FROM orders_products);
            """
        print ("orders_products inserted")
    
    cursor.execute(sql)
    cursor.close()
    connection.close()