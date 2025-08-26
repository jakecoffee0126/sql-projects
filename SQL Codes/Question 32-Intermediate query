/****** Question 32 Sales Analysis- How to find weekly order counts in different product categories  ******/

WITH CTE AS (SELECT o.[customer number],  o.[shipped quantity],o.[product number],o.[part number], P.Category,   
    DATEPART(WEEK, CAST('20' + LEFT(CAST(o.[Order Date] AS varchar(6)), 2) + '-' +
                        SUBSTRING(CAST(o.[Order Date] AS varchar(6)), 3, 2) + '-' +
                        RIGHT(CAST(o.[Order Date] AS varchar(6)), 2) AS date)) AS WeekNumber
FROM Orders o
INNER JOIN Products P
ON O.[Product Number]=P.[Product Number] AND O.[Part number]=P.[Part number]
WHERE [Order Date] between '210101' AND '211231')
SELECT Category, WeekNumber, SUM([shipped quantity]) AS OrderQuantiy
FROM CTE
WHERE Category='FARM' OR Category='LAWN & GARDEN'
GROUP BY Category, WeekNumber
ORDER BY Category, OrderQuantiy DESC

/****** Question 32 Sales Analysis- How to find weekly order counts in different product categories  ******/

WITH CTE AS (SELECT o.[customer number], o.[shipped quantity],o.[product number],o.[part number], P.Category,
    
    DATEPART(WEEK, CAST('20' + LEFT(CAST(o.[Order Date] AS varchar(6)), 2) + '-' +
                        SUBSTRING(CAST(o.[Order Date] AS varchar(6)), 3, 2) + '-' +
                        RIGHT(CAST(o.[Order Date] AS varchar(6)), 2) AS date)) AS WeekNumber
FROM Orders o
INNER JOIN Products P
ON O.[Product Number]=P.[Product Number] AND O.[Part number]=P.[Part number]
WHERE [Order Date] between '210101' AND '211231')
SELECT DISTINCT Category, WeekNumber, SUM([shipped quantity])OVER(PARTITION BY Category, weeknumber) AS OrderQuantiy
FROM CTE
WHERE Category='FARM' OR Category='LAWN & GARDEN'
ORDER BY category, OrderQuantiy desc
