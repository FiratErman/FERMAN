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
# Define the variable from the json
database_name = table_data['database_name']
table_name = table_data['table_name']
columns = table_data['columns']


# Import the json file and create a dataframe with lowercasing column names
df = pd.read_json(json_batches_det_file_path)
# Put all columns name in lowercase for consistency purpose
df.columns = df.columns.str.lower()
# Replace empty space with NaN for dataframe columns
df = replace_empty_with_nan(df)

# Create the list of columns
df_columns = df.columns.tolist()

# Drop duplicates
df.drop_duplicates(inplace=True)



# Create the table
create_table(database_name, table_name, columns, cursor)
# Insert values into the table
insert_values(df, df_columns, database_name, table_name, cursor)
# Update the sql table with null values
update_sql_table_with_null_values(df, df_columns, cursor, database_name, table_name)

# commit the changes
connection.commit()
if cursor:
    cursor.close()
if connection.is_connected():
    connection.close()