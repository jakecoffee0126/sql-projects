--METHOD 1 USING Separate CTEs
WITH CTE AS (SELECT [Customer Number]
FROM (SELECT  TOP(10) O.[Customer Number],P.Category, SUM(O.[Shipped Quantity]*O.[unit retail]) AS TotalSale    
FROM [CHTS ].[dbo].[orders] O
INNER JOIN Products P
ON P.[Product Number]=O.[Product Number] AND P.[Part Number]=O.[Part number]
WHERE P.Category='ELECTRICAL'
GROUP BY O.[Customer Number],P.Category
ORDER BY TotalSale DESC) a
INTERSECT
SELECT  [Customer Number]
FROM (SELECT TOP(10) O.[Customer Number],P.Category, SUM(O.[Shipped Quantity]*O.[unit retail]) AS TotalSale    
FROM [CHTS ].[dbo].[orders] O
INNER JOIN Products P
ON P.[Product Number]=O.[Product Number] AND P.[Part Number]=O.[Part number]
where P.Category='AUTOMOTIVE'
GROUP BY O.[Customer Number],P.Category
ORDER BY TotalSale DESC)b)

SELECT O.[Customer Number], P.Category, SUM(O.[Shipped Quantity] * O.[unit retail]) AS TotalSale
FROM [orders] O
INNER JOIN Products P
    ON P.[Product Number] = O.[Product Number]  AND P.[Part Number] = O.[Part number]
INNER JOIN CTE C
    ON C.[Customer Number] = O.[Customer Number]
WHERE P.Category IN ('ELECTRICAL', 'AUTOMOTIVE')
GROUP BY O.[Customer Number], P.Category
ORDER BY  P.Category, TotalSale DESC

--Method 2 Using INTERSECT
WITH CTE1 AS (SELECT  TOP(10) O.[Customer Number],P.Category, SUM(O.[Shipped Quantity]*O.[unit retail]) AS TotalSale    
FROM [CHTS ].[dbo].[orders] O
INNER JOIN Products P
ON P.[Product Number]=O.[Product Number] AND P.[Part Number]=O.[Part number]
WHERE P.Category='ELECTRICAL'
GROUP BY O.[Customer Number],P.Category
ORDER BY TotalSale DESC),

CTE2 AS(SELECT TOP(10) O.[Customer Number],P.Category, SUM(O.[Shipped Quantity]*O.[unit retail]) AS TotalSale    
FROM [CHTS ].[dbo].[orders] O
INNER JOIN Products P
ON P.[Product Number]=O.[Product Number] AND P.[Part Number]=O.[Part number]
where P.Category='AUTOMOTIVE'
GROUP BY O.[Customer Number],P.Category
ORDER BY TotalSale DESC),

CTE3 AS (SELECT *FROM CTE1
UNION ALL
SELECT * FROM CTE2)

--Method 3 Using window Function
WITH CTE AS (SELECT O.[Customer Number],P.Category, SUM(O.[Shipped Quantity]*O.[unit retail]) AS TotalSale    
FROM [CHTS ].[dbo].[orders] O
INNER JOIN Products P
ON P.[Product Number]=O.[Product Number] AND P.[Part Number]=O.[Part number]
WHERE P.Category='ELECTRICAL' OR P.Category='AUTOMOTIVE'
GROUP BY O.[Customer Number],P.Category),

CTE1 AS(SELECT [Customer Number],Category, TotalSale, RANK() OVER(PARTITION BY Category ORDER BY Totalsale DESC) as rk
FROM CTE),

CTE2 AS (SELECT * FROM CTE1 WHERE rk<=10 )

SELECT [Customer Number],Category, TotalSale 
FROM CTE2
WHERE [Customer Number] IN (SELECT [Customer Number] FROM CTE2 GROUP BY [Customer Number] HAVING COUNT([Customer number])>=2)
ORDER BY Category, TotalSale DESC

SELECT [customer number], Category, TotalSale
FROM CTE3
WHERE [customer Number] In (select [Customer Number] from CTE1) AND [customer Number] In (select [Customer Number] from CTE2)
ORDER BY Category, TotalSale DESC
