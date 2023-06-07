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

/* add a column "able to request a credit" 
our age limit is 60
*/
DROP TABLE IF EXISTS master_bidabi.customers_enhanced_2;
CREATE TABLE master_bidabi.customers_enhanced_2 AS 
SELECT 
	*,
    CASE WHEN age_in_15_years<60 AND age_in_20_years<60 AND age_in_25_years<60  THEN "yes"
    ELSE "no"
    END AS age_eligibility_in_all_case,
    CASE WHEN age_in_15_years<60 OR age_in_20_years<60 OR age_in_25_years<60  THEN "yes"
	ELSE "no"
    END AS age_at_least_one_eligibility,
	CASE WHEN age_in_25_years<50 THEN 10
		WHEN  age_in_25_years<55 AND age_in_25_years>50 THEN 5
		WHEN age_in_25_years> 60 THEN 0
    END AS age_additional_available_years,
    CASE WHEN name='anne' AND surname='presi' THEN 'stage'
		WHEN name='jean martin martin' AND surname='lobo' THEN 'cdi'
        WHEN name='Karim' AND surname='Bidop' THEN 'retraite'
	END AS work_eligibility,
	CASE WHEN name='anne' AND surname='presi' THEN 'no'
		WHEN name='jean martin martin' AND surname='lobo' THEN 'no'
        WHEN name='Karim' AND surname='Bidop' THEN 'yes'
	END AS have_another_credit,
	CASE WHEN name='anne' AND surname='presi' THEN 'no'
		WHEN name='jean martin martin' AND surname='lobo' THEN 'yes'
        WHEN name='Karim' AND surname='Bidop' THEN 'no'
	END AS disease
FROM master_bidabi.customers_enhanced;

	
/* create table with the final_decision column*/
CREATE TABLE master_bidabi.customers_final_decision AS 
SELECT 
	*,
    CASE WHEN age_at_least_one_eligibility="yes" AND work_eligibility="cdi" AND have_another_credit="no" AND disease="no" THEN "yes"
	ELSE "no"
    END AS final_decision
FROM master_bidabi.customers_enhanced_2


/*create a table from customers_final_decision and filter by people living in Paris*/
DROP TABLE IF EXISTS master_bidabi.customers_in_paris;
CREATE TABLE master_bidabi.customers_in_paris
SELECT 
	*
FROM master_bidabi.customers_final_decision
WHERE city="Paris";

/*create a table from customers_final_decision and filter by people living in Paris and age_in_10_years < 40 */
/*first method*/
DROP TABLE IF EXISTS master_bidabi.customers_in_paris_and_age_inf_40;
CREATE TABLE master_bidabi.customers_in_paris_and_age_inf_40 AS 
SELECT 
	*
FROM master_bidabi.customers_final_decision
WHERE 
	city="Paris" AND age+10 < 40 ;

/*second method: nested table (fr: table imbrique)*/
DROP TABLE IF EXISTS master_bidabi.customers_in_paris_and_age_inf_40;
CREATE TABLE master_bidabi.customers_in_paris_and_age_inf_40 AS 
SELECT 
	* 
FROM
(SELECT 
	*,
	age+10 AS age_in_10_years
FROM master_bidabi.customers_final_decision)table1
WHERE city="Paris" AND age_in_10_years<40;


/*create a table from customers_final_decision and create a column region="ile_de_france" if city="Paris" else "other region/country"*/
DROP TABLE IF EXISTS master_bidabi.customers_in_paris_and_age_inf_40;
CREATE TABLE master_bidabi.customers_in_paris_and_age_inf_40 AS 
SELECT 
	*,
    CASE WHEN city='Paris' THEN 'ile_de_france'
    ELSE 'other region/country'
    END AS region
FROM master_bidabi.customers_final_decision;



/*GROUP BY*/
/*count customers*/
SELECT
	count(name)
FROM master_bidabi.customers_final_decision;

/*count customers by city*/
SELECT
	count(name) AS nbr_of_customers,
    city
FROM master_bidabi.customers_final_decision
GROUP BY city ;

/*count customers by city by work_eligibility*/

SELECT
	count(name) AS nbr_of_customers,
    city,
    work_eligibility
FROM master_bidabi.customers_final_decision
GROUP BY city, work_eligibility
;

SELECT
	*
FROM master_bidabi.customers_final_decision


SELECT
	count(*),
    final_decision,
    disease, age
FROM master_bidabi.customers_final_decision
GROUP BY final_decision, disease, age

/*UNION: number of columns is IMPORTANT*/
SELECT
	*
FROM master_bidabi.customers_final_decision
UNION ALL
SELECT
	*
FROM master_bidabi.customers_final_decision

/*SUM + group by : let's imagine age is the salary for this example*/
SELECT
	 SUM(age) as salary_in_Keuros,
     name
FROM 
(
SELECT
	*
FROM master_bidabi.customers_final_decision
UNION ALL
SELECT
	*
FROM master_bidabi.customers_final_decision
)table1
GROUP BY name;


/*INNER JOIN*/
DROP TABLE IF EXISTS master_bidabi.customers_books;
CREATE TABLE master_bidabi.customers_books
(
	id VARCHAR(10),
    name VARCHAR(50),
    surname VARCHAR(50),
    books VARCHAR (30)
)
;


INSERT INTO master_bidabi.customers_books (id ,name, surname, books)
VALUES ("330AP","anne", "presi", "agatha christie : tome1"),
	("330AP","anne", "presi", "agatha christie : tome2"),
	("330AP","anne", "presi", "agatha christie : tome3"),
	("330AP","anne", "presi", "agatha christie : tome4"),
	("330AP","anne", "presi", "Le seigneur des anneaux"),
	("780JMM","jean martin martin", "lobo", "star wars: tome1"),
	("780JMM","jean martin martin", "lobo", "star wars: tome2"),
	("780JMM","jean martin martin", "lobo", "star wars: tome3"),
	("780JMM","jean martin martin", "lobo", "star wars: tome4"),
	("780JMM","jean martin martin", "lobo", "Le seigneur des anneaux"),
	("450KB","Karim", "Bidop", "star wars: tome1"),
	("450KB","Karim", "Bidop", "star wars: tome2"),
	("450KB","Karim", "Bidop", "star wars: tome3"),
	("450KB","Karim", "Bidop", "star wars: tome4"),
	("450KB","Karim", "Bidop","agatha christie : tome2"),
	("450KB","Karim", "Bidop","agatha christie : tome3")
;

DROP TABLE IF EXISTS master_bidabi.customers_final_decision_with_id;
CREATE TABLE master_bidabi.customers_final_decision_with_id AS
SELECT
    *,
    CASE WHEN name='anne' AND surname='presi' THEN '330AP'
		WHEN name='jean martin martin' AND surname='lobo' THEN '780JMM'
        WHEN name='Karim' AND surname='Bidop' THEN '450KB'
	END AS id
FROM master_bidabi.customers_final_decision;


SELECT 
	table1.*,
    table2.books
FROM  master_bidabi.customers_final_decision_with_id AS table1
INNER JOIN
master_bidabi.customers_books AS table2
ON table1.id = table2.id

