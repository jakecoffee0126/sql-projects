--Method 1
SELECT DISTINCT a.[Customer Number],a.[Order Date], a.TotalQuantity,c.[first name], c.[Last Name]     
FROM (SELECT [Customer Number]     
      ,[order number]     
      ,[Order Date]     
      ,sum([Requested Quantity]) over(partition by [customer number], [ORDER DATE]) as TotalQuantity, dense_rank()over(partition by [customer number] order by [order date] desc) as rk     
      
  FROM [CHTS ].[dbo].[orders]) A
  inner join customers C
  ON A.[Customer Number]=C.[customer number]
  WHERE rk<=3
  ORDER BY [Customer Number], [Order Date] DESC

--Method 2
WITH CTE AS (
SELECT [Customer number], [order date], Max([order number]) as [order number]
        SUM([requested quantity]) as Total_Requested_Quantity
FROM Orders
GROUP BY [Customer Number], [Order date]),

CTE1 AS ( SELECT [Customer number], [order number], [order date], Total_Requested_Quantity, 
        ROW_NUMBER() OVER(PARTITION BY [customer number] ORDER BY [Order date] DESC) AS rn
       FROM CTE)
SELECT CTE1.[Customer number], CTE1.[order date], CTE1.Total_Requested_Quantity AS [Requested Quantity], C.[First Name], C.[Last Name]
FROM CTE1
JOIN Customers C
ON CTE1.[Customer number]=C.[Customer Number]
WHERE CTE1.RN<=3
ORDER BY CTE1.[Customer Number] ASC, CTE1.[Order date] DESC
