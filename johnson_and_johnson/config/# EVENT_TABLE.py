# EVENT_TABLE
- format Date created != Due date
- count number of id per Parent_ID:
- lowercasing columns name in order to make it easier to work with (consistency)

def lowercase_column_names(df):
    df.columns = map(str.lower, df.columns)
    return df

- when defining the type of the sql table
    - title : length is always 30 if not null, let's put varchar(30) to avoid any risk
        If you try to insert a value that exceeds this limit, such as a string with more than 70 characters, it will result in a data truncation error.

THINK TO PUT IN THE PYTHON SCRIPTS SOME ASSERTION , TRY EXCEPT, ESPECIALY ON THE NUMBER OF COLUMNS ETC, SOME CHECKS