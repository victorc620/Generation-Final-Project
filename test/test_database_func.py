import sys
sys.path.append("C:/Users/abali/OneDrive/Desktop/Group project")
from database_func import *
import psycopg2
from unittest.mock import patch

#-----------------------------------------------------------------------------------------------------------------------------------------

@patch('psycopg2.connect')
def test_fetch_sql_db(connect_mock):
    cursor = connect_mock.return_value.cursor.return_value
    cursor.fetchall.return_value = [(), ()]
    rows = fetch_sql_db('test')
    # assert rows == [(), ()]
    cursor.execute.assert_called_with('test', None)
    
#-----------------------------------------------------------------------------------------------------------------------------------------
    
@patch('psycopg2.connect')
def test_fetch2_sql_db(connect_mock):
    cursor = connect_mock.return_value.cursor.return_value
    cursor.fetchall.return_value = [(), ()]
    rows = fetch_sql_db('test')
    assert rows == [(), ()]
    
#---------------------------------------------------------------------------------------------------------------------------------------
    
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

