import os
# import sys to get more detailed Python exception info
import sys
# import the connect library for psycopg2
import psycopg2
# import the error handling libraries for psycopg2
from psycopg2 import OperationalError, errorcodes, errors
import psycopg2.extras as extras
import pandas as pd
from io import StringIO
import numpy as np
from data_normalization import temp_df
from sqlalchemy import create_engine
from sqlalchemy import exc

# missing port from coonn_params see other files for example
conn_params = {
    "host"      : "localhost",
    "database"  : "test2",
    "user"      : "team4gp",
    "password"  : "team4pw"
}
#Define a function that handles and parses psycopg2 exceptions
def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()    
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
# Using alchemy method
connect_alchemy = "postgresql+psycopg2://%s:%s@%s/%s" % (
    conn_params['user'],
    conn_params['password'],
    conn_params['host'],
    conn_params['database']
)
def using_alchemy(df, table):
    for i in range(len(df)):
        try:
            engine = create_engine(connect_alchemy)
            df.iloc[i:i+1].to_sql(name = table, con=engine, index=False, if_exists='append')
            print("Data inserted using to_sql()(sqlalchemy) done successfully...")
        except exc.IntegrityError as e:
            pass #or any other action
        # except OperationalError as err:
        #     # passing exception to function
        #     show_psycopg2_exception(err)
        
# Connect to the database
engine = create_engine(connect_alchemy)
# Importing data using_alchemy method

using_alchemy(temp_df,"orders")