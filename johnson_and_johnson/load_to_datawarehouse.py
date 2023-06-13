import mysql.connector
import pandas as pd
import json 
from config.config_connection.mysql_connection import (
    user,
    password,
    host,
    database
)
from common import (
    create_table,
    insert_values,
    update_sql_table_with_null_values,
    replace_empty_with_nan
)
 

# paths 
sql_query_due_date_event = "/Users/firaterman/Documents/git_workspace/ferman/johnson_and_johnson/due_date_event.sql"
sql_query_event_database_enhanced = "/Users/firaterman/Documents/git_workspace/ferman/johnson_and_johnson/event_database_enhanced.sql"

# Connection with MySQL using the required configuration
connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

cursor = connection.cursor()
# Define variables from json

def create_table_due_date_event():
    try:
        # Read the SQL script file and modify the query
        with open(sql_query_due_date_event, 'r') as f:
            create_table_query = f.read()
        print(create_table_query)
        cursor.execute(create_table_query)
    except Exception as e:
        print(f"Error updating SQL table: {str(e)}")
        
def create_table_event_database_enhanced():
    try:
        # Read the SQL script file and modify the query
        with open(sql_query_event_database_enhanced, 'r') as f:
            create_table_query = f.read()
        print(create_table_query)
        cursor.execute(create_table_query)
    except Exception as e:
        print(f"Error updating SQL table: {str(e)}")
        
# Create the table
create_table_due_date_event()
create_table_event_database_enhanced()

# commit the changes
connection.commit()
if cursor:
    cursor.close()
if connection.is_connected():
    connection.close()