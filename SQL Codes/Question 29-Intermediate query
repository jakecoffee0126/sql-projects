/****** How to find the percentage of immediate orders in the first orders of all customers  ******/
--CTE + Window Function approach

WITH CTE AS (SELECT [Customer Number],[order number],[Order Date],[Target Ship Date], RANK() OVER(PARTITION BY [Customer Number] ORDER BY [Order Date]) AS rk
FROM [CHTS ].[dbo].[orders])

SELECT ROUND(SUM(CASE WHEN [Order Date]=[Target Ship Date] THEN 1 ELSE 0 END)*100.0/COUNT(*), 2 ) AS immediate_percentage
FROM CTE
WHERE rk=1

 ****** Question 29-How to find the percentage of immediate orders in the first orders of all customers  ******/
--Using SQL Variables, Windows function, self-join, Subquery

DECLARE @TotalCustomers int;
DECLARE @ImmediateOrderCustomers int;

-- Total number of first-day orders
SET @TotalCustomers = ( SELECT COUNT(*)
                        FROM Orders O1
						INNER JOIN (
							SELECT [Customer number], MIN([Order Date]) AS FirstOrderDate
							FROM Orders
							GROUP BY [Customer number]) O2
							ON O1.[Customer Number] = O2.[Customer Number] AND O1.[Order Date] = O2.FirstOrderDate);
-- Number of first-day orders shipped immediately
SET @ImmediateOrderCustomers = (SELECT COUNT(IIF(O1.[Order Date] = O1.[Target ship date], 1, NULL))
                                FROM Orders O1
								INNER JOIN (
									SELECT [Customer number], MIN([Order Date]) AS FirstOrderDate
									FROM Orders
									GROUP BY [Customer number]) O2
									ON O1.[Customer Number] = O2.[Customer Number] AND O1.[Order Date] = O2.FirstOrderDate);
-- Percentage
SELECT ROUND(100.0 * @ImmediateOrderCustomers / @TotalCustomers, 2) AS immediate_percentage; 
