
SELECT O.[customer Number],C. [First Name], C.[Last Name],  COUNT(distinct P.[category number]) AS TotalCategoryCount
FROM Orders O
INNER JOIN Products P                   
ON P.[Product number]=O.[product number]
INNER JOIN Customers C
ON O.[customer number]=C.[customer number]

GROUP BY O.[CUSTOMER NUMBER],C. [First Name], C.[Last Name]        

HAVING COUNT(DISTINCT P.Category)=(SELECT count(distinct [category number]) from Products)

ORDER BY O.[customer number]
