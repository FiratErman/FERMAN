-- This is auto-generated code
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://datalakewsdb048.dfs.core.windows.net/files/sales/csv/2019.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0'
    ) AS [result]



/*
**************************************
*****************JSON*****************
**************************************
*/

-- This is auto-generated code
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://datalakewsdb048.dfs.core.windows.net/files/sales/json/**',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0'
    ) AS [result]

/*
It won't work like this so we need to modify the script in order to make it work with json
*/

 SELECT
     TOP 100 *
 FROM
     OPENROWSET(
         BULK 'https://datalakewsdb048.dfs.core.windows.net/files/sales/json/',
         FORMAT = 'CSV',
         FIELDTERMINATOR ='0x0b',
         FIELDQUOTE = '0x0b',
         ROWTERMINATOR = '0x0b'
     ) WITH (Doc NVARCHAR(MAX)) as rows


/*
JSON_VALUE function to extract individual field values from the JSON data.
*/
 SELECT JSON_VALUE(Doc, '$.SalesOrderNumber') AS OrderNumber,
        JSON_VALUE(Doc, '$.CustomerName') AS Customer,
        Doc
 FROM
     OPENROWSET(
         BULK 'https://datalakewsdb048.dfs.core.windows.net/files/sales/json/',
         FORMAT = 'CSV',
         FIELDTERMINATOR ='0x0b',
         FIELDQUOTE = '0x0b',
         ROWTERMINATOR = '0x0b'
     ) WITH (Doc NVARCHAR(MAX)) as rows




/*
**************************************
*****************PARQUET*****************
**************************************
*/

     -- This is auto-generated code
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://datalakewsdb048.dfs.core.windows.net/files/sales/parquet/year=2019/**',
        FORMAT = 'PARQUET'
    ) AS [result]


 SELECT YEAR(OrderDate) AS OrderYear,
        COUNT(*) AS OrderedItems
 FROM
     OPENROWSET(
         BULK 'https://datalakewsdb048.dfs.core.windows.net/files/sales/parquet/**',
         FORMAT = 'PARQUET'
     ) AS [result]
 GROUP BY YEAR(OrderDate)
 ORDER BY OrderYear


 SELECT YEAR(OrderDate) AS OrderYear,
        COUNT(*) AS OrderedItems
 FROM
     OPENROWSET(
         BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/parquet/year=*/',
         FORMAT = 'PARQUET'
     ) AS [result]
 WHERE [result].filepath(1) IN ('2019', '2020')
 GROUP BY YEAR(OrderDate)
 ORDER BY OrderYear




/*
**************************************
*****************CREATE EXTERNAL DATA SOURCE*****************
**************************************
*/

  
 /*
 So far, you’ve used the OPENROWSET function in a SELECT query to
  retrieve data from files in a data lake. The queries have been run in 
  the context of the master database in your serverless SQL pool. 
  This approach is fine for an initial exploration of the data, 
  but if you plan to create more complex queries it may be more effective 
  to use the PolyBase capability of Synapse SQL to create objects in a database
  that reference the external data location.
 */
 CREATE DATABASE Sales
   COLLATE Latin1_General_100_BIN2_UTF8;
 GO;

 Use Sales;
 GO;

 CREATE EXTERNAL DATA SOURCE sales_data WITH (
     LOCATION = 'https://datalakewsdb048.dfs.core.windows.net/files/sales/'
 );
 GO;






 /*
The query uses the external data source to connect to the data lake
*/
SELECT
*
FROM OPENROWSET(
    BULK'csv/*.csv',
    DATA_SOURCE = 'sales_data',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0'
) AS orders


 SELECT *
 FROM  
     OPENROWSET(
         BULK 'parquet/year=*/',
         DATA_SOURCE = 'sales_data',
         FORMAT='PARQUET'
     ) AS orders
 WHERE orders.filepath(1) = '2019'

 /*
 create an external table
 The external data source makes it easier to access the files in the data lake,
  but most data analysts using SQL are used to working with tables in a database.
  Fortunately, you can also define external file formats and external tables that
  encapsulate rowsets from files in database tables.
 */

 CREATE EXTERNAL FILE FORMAT CsvFormat
     WITH (
         FORMAT_TYPE = DELIMITEDTEXT,
         FORMAT_OPTIONS(
         FIELD_TERMINATOR = ',',
         STRING_DELIMITER = '"'
         )
     );
 GO;

 CREATE EXTERNAL TABLE dbo.orders
 (
     SalesOrderNumber VARCHAR(10),
     SalesOrderLineNumber INT,
     OrderDate DATE,
     CustomerName VARCHAR(25),
     EmailAddress VARCHAR(50),
     Item VARCHAR(30),
     Quantity INT,
     UnitPrice DECIMAL(18,2),
     TaxAmount DECIMAL (18,2)
 )
 WITH
 (
     DATA_SOURCE =sales_data,
     LOCATION = 'csv/*.csv',
     FILE_FORMAT = CsvFormat
 );
 GO;



 SELECT YEAR(OrderDate) AS OrderYear,
        SUM((UnitPrice * Quantity) + TaxAmount) AS GrossRevenue
 FROM dbo.orders
 GROUP BY YEAR(OrderDate)
 ORDER BY OrderYear;