WITH CTE AS (SELECT  [Customer Number]
      ,[Store Number]
      ,[order number],[Order Date]
     
      ,SUM([Shipped Quantity]*[Unit Retail]) AS OrderedQuantity
    
  FROM [CHTS ].[dbo].[orders]
  GROUP BY  [Customer Number],[Store Number],[order number], [Order Date])
SELECT [Customer Number]
      ,[Store Number]
      ,[order number],[Order Date], OrderedQuantity 
FROM CTE
WHERE OrderedQuantity> (select AVG(orderedQuantity) AS AverageQuantity FROM CTE)

ORDER BY [Customer Number],[Order Date]

