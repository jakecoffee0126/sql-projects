WITH CTE1 AS (SELECT [Customer Number], '20' +LEFT(convert (varchar(6),cast([order date] as int)), 2) + '-' 
                   + SUBSTRING(convert (varchar(6),cast([order date] as int)), 3,2) + '-' 
				   + RIGHT(convert (varchar(6),cast([order date] as int)), 2)AS OrderDate, [Unit Retail], [Shipped Quantity]
FROM orders WHERE [order date] between '220101' and '221231'),

CTE2 AS (SELECT OrderDate, SUM([Unit Retail]*[shipped quantity]) As DailyTotal
FROM CTE1
GROUP BY OrderDate)

SELECT OrderDate, SUM(DailyTotal) OVER(ORDER BY OrderDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS Past7DaySale,
                  ROUND(SUM(DailyTotal*1.0) OVER(ORDER BY OrderDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)/7,2) AS AverageSale
FROM CTE2
ORDER BY OrderDate
OFFSET 6 ROWS
WITH DailyAmount AS (
    SELECT visited_on, SUM(amount) AS total_amount
    FROM Customer
    GROUP BY visited_on
)

SELECT 
    d1.visited_on,
    SUM(d2.total_amount) AS amount,
    ROUND(AVG(CAST(d2.total_amount AS FLOAT)), 2) AS average_amount
FROM DailyAmount d1
JOIN DailyAmount d2 
    ON d2.visited_on BETWEEN DATEADD(DAY, -6, d1.visited_on) AND d1.visited_on
GROUP BY d1.visited_on
HAVING COUNT(*) = 7 -- ensure full 7-day window
ORDER BY d1.visited_on;



WITH CTE1 AS (SELECT [Customer Number], '20' +LEFT(convert (varchar(6),cast([order date] as int)), 2) + '-' 
                   + SUBSTRING(convert (varchar(6),cast([order date] as int)), 3,2) + '-' 
				   + RIGHT(convert (varchar(6),cast([order date] as int)), 2)AS OrderDate, [Unit Retail], [Shipped Quantity]
FROM orders WHERE [order date] between '220101' and '221231'),

CTE2 AS (SELECT OrderDate, SUM([Unit Retail]*[shipped quantity]) As DailyTotal
FROM CTE1
GROUP BY OrderDate)

SELECT 
    C1.OrderDate,
    SUM(C2.DailyTotal) AS Past7DaySale,
    ROUND(AVG(CAST(C2.DailyTotal AS FLOAT)), 2) AS AverageSale
FROM CTE2 C1
JOIN CTE2 C2 
    ON C2.OrderDate BETWEEN DATEADD(DAY, -6, C1.OrderDate) AND C1.OrderDate
GROUP BY C1.OrderDate
HAVING COUNT(*) = 7 -- ensure full 7-day window
ORDER BY C1.OrderDate;
