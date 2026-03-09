
WITH CTE AS (SELECT distinct O.[Store Number], S.[store name], P.[Category Number],P.Category, ROUND(SUM(O.[Shipped Quantity]*O.[Unit Retail])OVER(PARTITION BY P.[Category number], O.[store number] order by O.[store number]),2) AS TotalSale     
FROM [orders] O
INNER JOIN Products P
  ON O.[Product Number]=P.[Product Number]
INNER JOIN Stores S
  ON O.[Store Number]=S.[Store Number]
  WHERE [Order Date] BETWEEN '230101' AND '231231'
),

CTE1 AS (SELECT [Store number],  [store name], [category number], Category, TotalSale,  DENSE_RANK() OVER(PARTITION BY [Category number] ORDER BY TotalSale DESC) AS rk
FROM CTE)

SELECT [Store number],[store name], [category number], Category, TotalSale
FROM CTE1
WHERE rk=1
ORDER BY TotalSale DESC 
