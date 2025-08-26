
WITH CTE1 AS (SELECT C.[customer number], P.[product number], P.[part number], P.[SKU - Description], P.Category
FROM Customers C
INNER JOIN Orders O  
    ON C.[customer number]=O.[Customer Number]
INNER JOIN Products P
    ON P.[Product Number]=O.[product number] AND P.[Part Number]=O.[part number] ),
CTE2 AS (SELECT [customer number], [product number], [part number], [SKU - Description], Category, count(*) as cnt,
                DENSE_RANK() OVER(PARTITION BY [customer number] Order by COUNT(*) DESC) as rk
FROM CTE1
GROUP BY [customer number], [product number], [part number], [SKU - Description], Category)
SELECT [customer number], [product number], [part number], [SKU - Description], Category, cnt
FROM CTE2
WHERE rk=1
ORDER BY [customer number]
