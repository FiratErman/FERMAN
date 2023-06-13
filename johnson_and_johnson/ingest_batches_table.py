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
    convert_date_utc,
    create_table,
    insert_values,
    update_sql_table_with_null_values,
    replace_empty_with_nan
)

# paths 
json_config_batches_table= '/Users/firaterman/Documents/git_workspace/ferman/johnson_and_johnson/config/config_database/config_batches_table.json'
csv_batches_table_file_path = '/Users/firaterman/Downloads/jj/batches_table.csv'

# Connection with MySQL using the required configuration
connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

# Import of json file containing the database name, table name, columns names and their types
with open(json_config_batches_table) as f:
    table_data = json.load(f)
cursor = connection.cursor()
# Define the variable from the json
database_name = table_data['database_name']
table_name = table_data['table_name']
columns = table_data['columns']

# Import the csv file and create a dataframe with lowercasing column names
df = pd.read_csv(csv_batches_table_file_path)
# Put all columns name in lowercase for consistency purpose
df.columns = df.columns.str.lower()
# Replace empty space with NaN for dataframe columns
df = replace_empty_with_nan(df)

# Create the list of columns
df_columns = df.columns.tolist()

# Convert the date_created and due_date to the timestamp format yyyy-mm-dd HH:MM:SS
actual_date_format = {
    'release_date': "%d/%m/%Y %I:%M %p"

}
new_date_format = "%Y-%m-%d %H:%M:%S"
df = convert_date_columns(df, actual_date_format, new_date_format)

# Replace null value in date columns with a default date 
date_columns = ["release_date"]
default_date = "1970-01-02 00:00:00"
df = replace_null_date(df, date_columns, default_date)

# Convert the date column to UTC for standardisazation, avoiding ambiguity, global compatibility, timezonee conversion
local_timezone = datetime.datetime.now().astimezone().tzinfo
df = convert_date_utc(df, date_columns, local_timezone)

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