import os, boto3
import psycopg2
import pandas as pd

def connect():
    
    aws_client = boto3.client('ssm')
    response = aws_client.get_parameter(
    Name='team4_creds',
    WithDecryption=True)

    cred_string = response['Parameter']['Value']
    cred_string = cred_string.strip("}{").split(",")
    cred_string = [x.strip("\n") for x in cred_string]
    print(cred_string)

    redshift_host = cred_string[0].replace('"',"").replace(" ","").split(":")[1]
    redshift_port = cred_string[1].replace('"',"").replace(" ","").split(":")[1]
    redshift_password = cred_string[2].replace('"',"").replace(" ","").split(":")[1]
    redshift_user = cred_string[3].replace('"',"").replace(" ","").split(":")[1]
    
    conn = psycopg2.connect(
    host=redshift_host,
    port=redshift_port,
    database="team4_cafe",
    user=redshift_user,
    password=redshift_password
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