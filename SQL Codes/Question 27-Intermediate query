With CTE AS (SELECT [Store Number]
      ,count(distinct CAST([product number] AS VARCHAR) + '-' + CAST([part number] AS VARCHAR)) AS ItemCount
FROM [CHTS ].[dbo].[orders]
GROUP BY [Store number]),    

CTE1 AS(SELECT [Store Number], [Product Number] ,[Part number], SUM([Shipped Quantity]*[Unit Retail]) TotalSale    
FROM [CHTS ].[dbo].[orders]
GROUP BY [Store Number], [Product Number] ,[Part number]),

CTE2 AS(SELECT [Store Number], [Product Number] ,[Part number], TotalSale, RANK() OVER(PARTITION BY [Store Number] ORDER BY TotalSale DESC) AS rk
        FROM CTE1)

SELECT CTE.[Store Number], S.[Store Name], CTE.[ItemCount] ,CTE2.[Product Number],CTE2.[Part number], CTE2.TotalSale
FROM CTE2
INNER JOIN stores S
ON CTE2.[Store Number]=S.[Store Number]
INNER JOIN CTE
ON CTE2.[Store Number]=CTE.[Store Number]
WHERE rk<=3
ORDER BY  ItemCount DESC, TotalSale DESC
