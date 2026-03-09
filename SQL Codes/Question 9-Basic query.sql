SELECT [Customer Number], Min([order Date]) AS FirstOrderDate, SUM([Ordered Quantity]) AS TotalQuantity
FROM Orders
GROUP BY [Customer Number]
ORDER BY TotalQuantity DESC

--SELECT DISTINCT [Customer Number], Min([order Date])OVER(PARTITION BY [Customer Number]) AS FirstOrderDate, 
       SUM([Ordered Quantity])OVER(PARTITION BY [Customer Number]) AS TotalQuantity
--FROM Orders
--ORDER BY TotalQuantity DESC
