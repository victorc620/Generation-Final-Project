import sys
sys.path.append("C:/Users/abali/OneDrive/Desktop/Group project")
from lambda_database_func import *
import psycopg2
from pandas.testing import assert_frame_equal
from unittest.mock import patch

#-----------------------------------------------------------------------------------------------------------------------------------------
def fetch_sql_db(sql: str, val=None):
    """Load data from database to python"""
    connection = psycopg2.connect(host= "localhost", user= "team4gp", password = "team4pw", database = "team4gp", port = "5432")
    cursor = connection.cursor()
    cursor.execute(sql, val)
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    return rows

@patch('psycopg2.connect')
def test_fetch_sql_db(connect_mock):
    cursor = connect_mock.return_value.cursor.return_value
    cursor.fetchall.return_value = [(), ()]
    rows = fetch_sql_db('test')
    cursor.execute.assert_called_with('test', None)
    
#-----------------------------------------------------------------------------------------------------------------------------------------
    
@patch('psycopg2.connect')
def test_fetch2_sql_db(connect_mock):
    cursor = connect_mock.return_value.cursor.return_value
    cursor.fetchall.return_value = [(), ()]
    rows = fetch_sql_db('test')
    assert rows == [(), ()]
    
#---------------------------------------------------------------------------------------------------------------------------------------
def execute_sql_db(sql: str, val=None):
    """execture PostgreSQL command in database"""
    connection = psycopg2.connect(host= "localhost", user= "team4gp", password = "team4pw", database = "team4gp", port = "5432")
    cursor = connection.cursor()
    cursor.execute(sql, val)
    connection.commit()
    cursor.close()
    connection.close()

@patch('psycopg2.connect')
def test_execute_sql_db(connect_mock):
    cursor = connect_mock.return_value.cursor.return_value
    cursor.fetchall.return_value = [(), ()]
    rows = execute_sql_db('test', None)
    cursor.execute.assert_called_with('test', None)
    
#----------------------------------------------------------------------------------------------------------------------------------------

@patch('psycopg2.connect')
def test_execute2_sql_db(connect_mock):
    cursor = connect_mock.return_value.cursor.return_value
    cursor.fetchall.return_value = [(), ()]
    rows = execute_sql_db('test', None)
    assert rows == None

