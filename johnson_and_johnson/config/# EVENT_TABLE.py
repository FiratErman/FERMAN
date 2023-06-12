
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


Yes, storing datetime values in UTC (Coordinated Universal Time) format is a widely recommended practice in database systems. Storing datetime values in UTC provides several benefits:

Standardization: UTC is a standardized time reference that is independent of any specific timezone or daylight saving time changes. Storing datetime values in UTC ensures consistency and simplifies data handling across different timezones.

Avoiding Ambiguity: Local timezones can have variations due to daylight saving time (DST) changes, which can lead to ambiguity during DST transitions. Storing datetime values in UTC avoids such ambiguity and provides a consistent reference point.

Global Compatibility: UTC is a widely recognized and used time standard in various applications, systems, and industries. Storing datetime values in UTC makes the data more compatible and interoperable with systems in different timezones.

Timezone Conversion: Storing datetime values in UTC allows for easier conversion to different timezones as needed. It provides a common reference for converting datetime values to local or specific timezones based on user preferences or application requirements.

While storing datetime values in UTC is a common practice, the choice of datetime format ultimately depends on the specific needs and requirements of your application or system. It's important to consider factors such as data analysis, reporting, and user experience when deciding on the datetime format to use.


# Each time I am creating a table it is appending 

# TODO: replace null values