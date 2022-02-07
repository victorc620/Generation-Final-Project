# import sys to get more detailed Python exception info
import sys
# import the connect library for psycopg2
import psycopg2
# import the error handling libraries for psycopg2
from psycopg2 import OperationalError
import psycopg2.extras as extras
import pandas as pd

from data_normalization import temp


# missing port from coonn_params see other files for example
conn_params = {
    "host"      : "localhost",
    "database"  : "test2",
    "user"      : "team4gp",
    "password"  : "team4pw"
}

def show_psycopg2_exception(err):
    # get details about the exception
    err_type, traceback = sys.exc_info()
    # get the line number when exception occured
    line_n = traceback.tb_lineno
    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type)
    # psycopg2 extensions.Diagnostics object attribute
    print ("\nextensions.Diagnostics:", err.diag)
    # print the pgcode and pgerror exceptions
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")

def connect(conn_params):
    """Connecting to Postgresql database server"""
    conn =None
    try:
        # connect to postgresql
        print('connecting to Postgresql server')
        conn = psycopg2.connect(**conn_params)
        print("Connection successfully..................")
    except OperationalError as err:
        show_psycopg2_exception(err)
        conn = None
    return conn

# Define function using cursor.executemany() to insert the dataframe
# def execute_many(conn, datafrm, table):
#     # Creating a list of tupples from the dataframe values
#     tpls = [tuple(x) for x in datafrm.to_numpy()]
#     # dataframe columns with Comma-separated
#     cols = ','.join(list(datafrm.columns))
#     # SQL query to execute
#     sql = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s)" % (table, cols)
#     cursor = conn.cursor()
#     try:
#         cursor.executemany(sql, tpls)
#         conn.commit()
#         print("Data inserted using execute_many() successfully...")
#     except (Exception, psycopg2.DatabaseError) as err:
#         # pass exception to function
#         show_psycopg2_exception(err)
#         cursor.close()
        
# Define function using psycopg2.extras.execute_values() to insert the dataframe.
def execute_values(conn, datafrm, table):
    # Creating a list of tupples from the dataframe values
    tpls = [tuple(x) for x in datafrm.to_numpy()]
    # dataframe columns with Comma-separated
    cols = ','.join(list(datafrm.columns))
    # SQL query to execute
    sql = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, sql, tpls)
        print("Data inserted using execute_values() successfully..")
    except (Exception, psycopg2.DatabaseError) as err:
        # pass exception to function
        show_psycopg2_exception(err)
        cursor.close()



# Connect to the database
conn = connect(conn_params)
conn.autocommit = True
# Run the execute_many method
# execute_many(conn, producttable, 'test')
temp = temp[~temp.Key.isin(temp_df.Key)]
execute_values(conn, temp_df, 'cafe')