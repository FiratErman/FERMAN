import pandas as pd 
import pytz

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