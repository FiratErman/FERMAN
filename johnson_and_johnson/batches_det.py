import mysql.connector
import pandas as pd
import json 
from config.config_connection.mysql_connection import (
    user,
    password,
    host,
    database
)

# paths 
json_config_batches_det= '/Users/firaterman/Documents/git_workspace/ferman/johnson_and_johnson/config/config_database/config_batches_det.json'
json_batches_det_file_path = '/Users/firaterman/Downloads/jj/batches_det.json'

# Connection with MySQL using the required configuration
connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

# Import of json file containing the database name, table name, columns names and their types
with open(json_config_batches_det) as f:
    table_data = json.load(f)
cursor = connection.cursor()

# Import the json file and create a dataframe with lowercasing column names
df = pd.read_json(json_batches_det_file_path)
df.columns = df.columns.str.lower()

# Convert the date_created and due_date to the timestamp format yyyy-mm-dd HH:MM:SS
df['date_created'] = pd.to_datetime(df['date_created'], format="%d/%m/%Y %I:%M %p")
df['due_date'] = pd.to_datetime(df['due_date'], format="%Y/%d/%m %I:%M %p")
df['date_created'] = df['date_created'].dt.strftime("%Y-%m-%dT%H:%M:%S")
df['due_date'] = df['due_date'].dt.strftime("%Y-%m-%dT%H:%M:%S")

# Create the list of columns
df_columns = df.columns.tolist()



database_name = table_data['database_name']
table_name = table_data['table_name']
columns = table_data['columns']
#data = table_data['data_type']

def create_table():
    # Generate SQL command
    create_table_query = f"CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (\n"
    #primary_key = None
    for column in columns:
        create_table_query += f"{column['name']} {column['data_type']}, \n"
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

# Create the table
create_table()
# Insert values into the table
insert_values()
connection.commit()
if cursor:
    cursor.close()
if connection.is_connected():
    connection.close()