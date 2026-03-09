--Window function+CTE

WITH CTE AS (SELECT [Customer Number], '20' +LEFT(convert (varchar(6),cast([order date] as int)), 2) + '-' 
                   + SUBSTRING(convert (varchar(6),cast([order date] as int)), 3,2) AS orderDate, [Shipped Quantity],[Unit Retail] 
			 FROM orders WHERE [order date] between '210101' and '211231') 
SELECT DISTINCT [Customer Number], OrderDate, ROUND(SUM([shipped quantity]*[Unit Retail]) OVER(PARTITION BY [Customer Number] ORDER BY OrderDate),2) AS TotalSale 
FROM CTE ORDER BY [Customer Number],orderDate


/****** QUESTION 31 How do calculate monthly running total sales per customer ******/
--SELF-JOIN+CTEs

WITH CTE1 AS (SELECT [Customer Number],  '20' +LEFT(convert (varchar(6),cast([order date] as int)), 2) + '-' 
                                + SUBSTRING(convert (varchar(6),cast([order date] as int)), 3,2) AS orderDate, [Order date], [Shipped Quantity],[Unit Retail]								
FROM orders
WHERE [order date] between '210101' and '211231'),
CTE2 AS(SELECT [Customer Number], Orderdate, CAST(REPLACE(orderDate, '-', '') AS float) AS YearMonth, ROUND(SUM([shipped quantity]*[Unit Retail]),2) AS TotalSale
FROM CTE1 
Group by [Customer Number],orderDate)
SELECT C1.[Customer Number], C1.orderDate, SUM(C2.TotalSale) AS TotalSale
FROM CTE2 C1
JOIN CTE2 C2
ON C1.[Customer Number]=C2.[Customer Number] AND C1.YearMonth>=C2.YearMonth
GROUP BY C1.[Customer Number],C1.orderDate
ORDER BY C1.[Customer Number],C1.orderDate
