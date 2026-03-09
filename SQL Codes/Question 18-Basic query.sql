SELECT distinct [Customer Number]     
      ,COUNT([order number]) AS OrderCount, ROUND(SUM([Unit Cost]*[Shipped Quantity]),2) AS TotalAmount 
 FROM [CHTS ].[dbo].[orders]
 WHERE [Order Date] BETWEEN '240101' AND '241231'
 GROUP BY [Customer Number]
 HAVING COUNT([order number])>1000 AND SUM([Unit Cost]*[Shipped Quantity])>10000
 ORDER BY OrderCount DESC
