import os, boto3
import psycopg2
import pandas as pd
import backoff

def backoff_hdlr(details):
    print ("Backoff triggered")

def connect():
    
    aws_client = boto3.client('ssm')
    response = aws_client.get_parameter(
    Name='team4_creds',
    WithDecryption=True)

    cred_string = response['Parameter']['Value']
    cred_string = cred_string.strip("}{").split(",")
    cred_string = [x.strip("\n") for x in cred_string]

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

@backoff.on_exception(backoff.expo, Exception, on_backoff=backoff_hdlr)
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
        try:
            sql = """
            CREATE TEMP TABLE cafe_temp(
            cafe_id VARCHAR(256),
            location VARCHAR(256)
            );"""
            cursor.execute(sql)
            
            sql = "LOCK cafe_temp;"
            cursor.execute(sql)
            
            sql = insert_tmp
            cursor.execute(sql)
            
            sql = "LOCK cafe;"
            cursor.execute(sql)
            
            sql = """INSERT INTO cafe SELECT t.* FROM cafe_temp t
                    LEFT JOIN cafe c ON t.cafe_id = c.cafe_id
                    WHERE c.cafe_id IS NULL;"""
            cursor.execute(sql)
            
            print("Cafe Successfully Inserted~~!")
            
        except Exception as e:
            print(f"Insert Failed, error: {e}")
        
    elif table_name == "products":
        try:
            sql = """
                CREATE TEMP TABLE products_temp(
                product_id VARCHAR(256) NOT NULL,
                products VARCHAR(256) NOT NULL,
                product_price DOUBLE PRECISION NOT NULL
                );"""
            cursor.execute(sql)
            
            sql = "LOCK products_temp;"
            cursor.execute(sql)
            
            sql = insert_tmp
            cursor.execute(sql)
            
            sql = "LOCK products;"
            cursor.execute(sql)
            
            sql = """INSERT INTO products SELECT t.* FROM products_temp t
                LEFT JOIN products p ON t.product_id = p.product_id
                WHERE p.product_id IS NULL;"""
            cursor.execute(sql)
                
            print("Products Successfully Inserted~~!")
            
        except Exception as e:
            print(f"Insert Failed, error: {e}")

    elif table_name == "orders":
        try:
            sql = """
            CREATE TEMP TABLE orders_temp(
            order_id VARCHAR(256) NOT NULL,
            location VARCHAR(256) NOT NULL,
            date TIMESTAMP without time zone,
            payment_type VARCHAR(256) NOT NULL,
            total_price double precision NOT NULL
            );"""
            cursor.execute(sql)
        
            sql ="LOCK orders_temp;"
            cursor.execute(sql)
        
            sql = insert_tmp
            cursor.execute(sql)
            
            sql = """
            CREATE TEMP TABLE orders_temp_2 AS (
            SELECT order_id, cafe.cafe_id, date, payment_type, total_price
            FROM orders_temp
            INNER JOIN cafe
            ON cafe.location = orders_temp.location
            );"""
            cursor.execute(sql)
            
            sql="LOCK orders_temp_2, orders;"
            cursor.execute(sql)
        
            sql = """INSERT INTO orders SELECT t.* FROM orders_temp_2 t
            LEFT JOIN orders o ON t.order_id = o.order_id
            WHERE o.order_id IS NULL;"""
            cursor.execute(sql)
            
            print("Orders Successfully Inserted~~!")
            
        except Exception as e:
            print(f"Insert Failed, error: {e}")
        
        
    elif table_name == "orders_products":
        try:
            sql ="""
                CREATE TEMP TABLE orders_products_temp (
                order_id VARCHAR(256) NOT NULL,
                products VARCHAR(256) NOT NULL,
                quantity_purchased INT
                );"""
            cursor.execute(sql)
                
            sql = "LOCK orders_products_temp;"
            cursor.execute(sql)
            
            sql = insert_tmp
            cursor.execute(sql)
                
            sql = """CREATE TEMP TABLE orders_products_temp_2 AS (
                    SELECT order_id, products.product_id, quantity_purchased
                    FROM orders_products_temp
                    INNER JOIN products
                    ON products.products = orders_products_temp.products
                    );"""
            cursor.execute(sql)
                
            sql = "LOCK orders_products_temp_2, orders_products;"
            cursor.execute(sql)
        
            sql = """INSERT INTO orders_products SELECT t.* FROM orders_products_temp_2 t
                LEFT JOIN orders_products op ON t.order_id = op.order_id
                WHERE op.order_id IS NULL;"""
            cursor.execute(sql)
    
            print("Orders_Products Successfully Inserted~~!")
            
        except Exception as e:
            print(f"Insert Failed, error: {e}")

    cursor.close()
    connection.close()
    