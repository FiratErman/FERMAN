/* 
********** step 1 : Create Schema for master bidabi **********
*/

CREATE SCHEMA master_bidabi;


/*
********** Create our first table : customers informations **********
*/

CREATE TABLE master_bidabi.customers
(
	name VARCHAR(50),
    surname VARCHAR(50),
    city VARCHAR (30),
    age INT,
    Postcal_code INT,
    job VARCHAR(45)
)
;


/*
********** Read the customers table : empty table **********
*/

SELECT * FROM master_bidabi.customers;


/*
********** fill the table with rows **********
*/

INSERT INTO master_bidabi.customers (name, surname, city, age, Postcal_code,job )
VALUES ("anne", "presi", "Paris", 24, 75010, "presidente"),
	   ("jean martin martin", "lobo", "Rennes", 40, 50500, "patissier")
;


/* 
********** add additional rows to the table **********
*/

INSERT INTO master_bidabi.customers (job, Postcal_code, age, name, surname)
VALUES ("Taxi driver", 60604, 80, "Karim", "Bidop");


/* 
**********multiple example : select some rows **********
*/
/*example 1 : select all columns from the table*/
SELECT
	*
FROM master_bidabi.customers;
/*example 2 : select all columns from the table and take only the first row*/
SELECT
	*
FROM master_bidabi.customers limit 1;
/*example 2 : select all columns from the table and take only the first row*/
SELECT
	name,
    surname,
    job
FROM master_bidabi.customers limit 2;


/* 
**********create a specific table **********
*/
CREATE TABLE master_bidabi.specific_customers AS 
SELECT
	name,
    surname,
    job
FROM master_bidabi.customers limit 2;


/* 
**********create a third table  from the customers table and create columns**********
*/
CREATE TABLE master_bidabi.customers_enhanced AS 
SELECT
	*,
    name as nom,
    age+15 AS age_in_15_years,
    age+20 AS age_in_20_years,
    age+25 AS age_in_25_years
FROM master_bidabi.customers;




