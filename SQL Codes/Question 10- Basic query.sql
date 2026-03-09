SELECT 
  ROUND(COUNT(DISTINCT o.[Customer Number]) * 100.0 / COUNT(DISTINCT c.[Customer Number]), 2) AS Percentage
FROM customers c
LEFT JOIN orders o
  ON c.[Customer Number] = o.[Customer Number]
  AND o.[Order Date] BETWEEN '230101' AND '231231';


--SELECT  ROUND(count(distinct[customer number])*100.00/(select count([customer number]) from customers),2)
--FROM orders
--WHERE [Order Date] between '200101' and '201231'
