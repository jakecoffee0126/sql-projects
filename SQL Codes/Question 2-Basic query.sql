DESCRIBE customers;
SHOW COLUMNS FROM customers;

-- Find all the customers from British Columbia
SELECT [Customer Number], [First Name], [Last Name], Province, [Postal Code], Phone, email
FROM Customers
WHERE Province='BC'
-- OR WHERE [Postal Code] LIKE 'V%'


/*MYSQL

SELECT *
FROM question100.products
WHERE price > 50 AND `SKU - Description` LIKE '%DUTY%';

*/