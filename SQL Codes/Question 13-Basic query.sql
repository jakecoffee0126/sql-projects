SELECT [Customer Number],[order number],[Order Date],[Requested Quantity]
FROM (SELECT  [Customer Number]     
              ,[order number]    
              ,[Order Date]     
              ,[Requested Quantity]
              ,dense_rank() over(partition by [order date] order by [Requested quantity] DESC) AS rk     
       FROM [CHTS ].[dbo].[orders]
       WHERE [order date] BETWEEN '230101' AND '231231') A
  WHERE rk=1
  ORDER BY [Order Date]
