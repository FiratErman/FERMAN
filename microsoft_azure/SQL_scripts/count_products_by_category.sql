-- This is auto-generated code
SELECT 
    TOP 100 *
FROM OPENROWSET(
    BULK 'https://datalakeyr2wefx.blob.core.windows.net/files/product_data/products.csv',
    FORMAT = 'CSV',
    PARSER_VERSION='2.0', 
    HEADER_ROW= TRUE
) AS [result];

SELECT
    COUNT(*) as ProductCount,
    Category
FROM OPENROWSET(
    BULK 'https://datalakeyr2wefx.blob.core.windows.net/files/product_data/products.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS [result]
GROUP BY Category
ORDER BY ProductCount DESC