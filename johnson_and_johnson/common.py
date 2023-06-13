import pandas as pd 
import pytz
import numpy as np

def replace_empty_with_nan(df):
    df.replace('', np.nan, inplace=True)
    return df

def convert_date_columns(df, actual_date_format, new_date_format):
    for col, fmt in actual_date_format.items():
        df[col] = pd.to_datetime(df[col], format=fmt)
        df[col] = df[col].dt.strftime(new_date_format)
    return df

def replace_null_date(df, columns_list, default_date):
    default_date = pd.to_datetime(default_date)
    for col in columns_list:
        df[col].fillna(default_date, inplace=True)
    return df

def convert_date_utc(df, date_columns, local_timezone):
    utc_timezone = pytz.timezone('UTC') # UTC time zone
    for column in date_columns:
        df[column] = pd.to_datetime(df[column]).dt.tz_localize(local_timezone).dt.tz_convert(utc_timezone)
    return df 

def create_table(database_name, table_name, columns, cursor):
    try:
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
    except Exception as e:
        print(f"Error updating SQL table: {str(e)}")
    
# TODO: put this into the common file
def insert_values(df, df_columns, database_name, table_name, cursor):
    # Check if number of columns are the same between the dataframe and the json file

    try:
        # Insert values into the table
        for _, row in df.iterrows():
            column_names = ', '.join(df_columns)
            values = ', '.join(f"'{row[col]}'" for col in df_columns)
            insert_query = f"INSERT INTO {database_name}.{table_name} ({column_names}) VALUES ({values})"
            cursor.execute(insert_query)
    except Exception as e:
        print(f"Error updating SQL table: {str(e)}")

# TODO: put this into the common file
def update_sql_table_with_null_values(df, df_columns, cursor, database_name, table_name ):
    try:
        # Disable safe update mode only for the current session
        cursor.execute("SET SQL_SAFE_UPDATES=0;")

        # Generate the set clause, for each column of the labs dataframe containing 'nan' values, that will be used to update the MySQL table 
        set_clauses_no_date_columns = ', '.join([f"{column} = NULLIF({column}, 'NaN')" for column in df_columns if df[column].dtype != 'datetime64[ns, UTC]'])

        # Generate the set clause for datetime columns     
        set_clauses_date_columns = ', '.join([f"{column} = NULLIF({column}, '1970-01-01 23:00:00')" for column in df_columns if df[column].dtype == 'datetime64[ns, UTC]'])

        # Check if df_columns contains a datetime type
        if any(df[column].dtype == 'datetime64[ns, UTC]' for column in df_columns):
            set_clause= set_clauses_no_date_columns + "," + set_clauses_date_columns
        else:
            set_clause = set_clauses_no_date_columns

        # Execute the update query 
        update_query = f"""
            UPDATE {database_name}.{table_name}
            SET {set_clause}
            ;
        """
        cursor.execute(update_query)
    except Exception as e:
        print(f"Error updating SQL table: {str(e)}")

