SELECT TOP(10) [Customer Number], max([Ordered Quantity] ) AS MaxQuantity     
FROM [CHTS ].[dbo].[orders]
GROUP BY [Customer Number]
ORDER BY  max([Ordered Quantity] ) DESC
  
 
--SELECT [Customer Number], max([Ordered Quantity] ) AS MaxQuantity     
--FROM [CHTS ].[dbo].[orders]
--GROUP BY [Customer Number]
--ORDER BY  max([Ordered Quantity] ) DESC 
--OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY
