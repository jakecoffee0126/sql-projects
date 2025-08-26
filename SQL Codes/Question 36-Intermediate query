WITH CTE1 AS (SELECT C.[customer number],C.[First Name], O.[Shipped Quantity], P.[product number], P.[part number], P.[SKU - Description],P.price, P.Category
FROM Customers C
INNER JOIN Orders O  
    ON C.[customer number]=O.[Customer Number]
INNER JOIN Products P
    ON P.[Product Number]=O.[product number] AND P.[Part Number]=O.[part number] ),
CTE2 AS (SELECT [customer number], [First Name],[product number], [part number], [SKU - Description], 
                Category, SUM([shipped quantity]*price) AS TotalSale,
				DENSE_RANK() OVER(PARTITION BY [customer number] Order by SUM([shipped quantity]*price) DESC) as rk
FROM CTE1
GROUP BY [customer number], [First Name],[product number], [part number], [SKU - Description], Category)


SELECT [customer number], [First Name],[product number], [part number], [SKU - Description], Category, TotalSale
FROM CTE2
WHERE rk=1
ORDER BY TotalSale DESC, [Customer Number] ASC
