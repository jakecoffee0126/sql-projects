SELECT
      [Product Number]  ,[Part number], 
	  MAX([Unit Retail]) AS MaxPrice, Min([Unit Retail]) AS MinPrice, 
	  ROUND(SUM([Unit Retail]*[Shipped Quantity])*1.0/SUM([Shipped Quantity]),2) AS AveragePrice 
FROM [CHTS ].[dbo].[orders]
WHERE [Shipped Quantity]>0
GROUP BY [Product Number]  ,[Part number]
ORDER BY [Product Number]
