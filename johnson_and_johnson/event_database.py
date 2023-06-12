import datetime
import json
import mysql.connector
import pandas as pd
import pytz
from config.config_connection.mysql_connection import (
    user,
    password,
    host,
    database
)
from common import (
    convert_date_columns,
    replace_null_date,
    convert_date_utc
)

# paths 
json_config_event_database = '/Users/firaterman/Documents/git_workspace/ferman/johnson_and_johnson/config/config_database/config_event_database.json'
xls_event_database_file_path = '/Users/firaterman/Downloads/jj/events_database.xlsx'

# Connection with MySQL using the required configuration
connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

# Import of json file containing the database name, table name, columns names and their types
with open(json_config_event_database) as f:
    table_data = json.load(f)
cursor = connection.cursor()
# Define the variable 
database_name = table_data['database_name']
table_name = table_data['table_name']
columns = table_data['columns']

# Import the xlsx file and create a dataframe with lowercasing column names
df = pd.read_excel(xls_event_database_file_path)
df.columns = df.columns.str.lower()

# Convert the date_created and due_date to the timestamp format yyyy-mm-dd HH:MM:SS
actual_date_format = {
    'date_created': "%d/%m/%Y %I:%M %p",
    'due_date': "%Y/%d/%m %I:%M %p"
}
new_date_format = "%Y-%m-%d %H:%M:%S"
df = convert_date_columns(df, actual_date_format, new_date_format)


# Replace null value in date columns with a default date 
date_columns = ["date_created", "due_date"]
default_date = "1970-01-02 00:00:00"
df = replace_null_date(df, date_columns, default_date)

# Convert the date column to UTC for standardisazation, avoiding ambiguity, global compatibility, timezonee conversion
local_timezone = datetime.datetime.now().astimezone().tzinfo
df = convert_date_utc(df, date_columns, local_timezone)

# Create the list of columns
df_columns = df.columns.tolist()

def create_table():
    # Generate SQL command
    create_table_query = f"CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (\n"
    #primary_key = None
    for column in columns:
        create_table_query += f"{column['name']} {column['data_type']}, \n"
            # TODO:
            # if column.get('primary_key'):
            #     create_table_query += f"{column['name']} {column['data_type']} PRIMARY KEY, \n"
            # else:
            #     create_table_query += f"{column['name']} {column['data_type']}, \n"
                
    create_table_query = create_table_query.rstrip(', \n') + "\n)"
    print(create_table_query)
    cursor.execute(create_table_query)
    

def insert_values():
    # Check if number of columns are the same between the dataframe and the json file
        ######
    # Create a cursor to execute SQL queries
    cursor = connection.cursor()

    # Insert values into the table
    for _, row in df.iterrows():
        column_names = ', '.join(df_columns)
        values = ', '.join(f"'{row[col]}'" for col in df_columns)

        insert_query = f"INSERT INTO {database_name}.{table_name} ({column_names}) VALUES ({values})"
        cursor.execute(insert_query)

print(df)
# Create the table
create_table()
# Insert values into the table
insert_values()
connection.commit()
if cursor:
    cursor.close()
if connection.is_connected():
    connection.close()