SELECT C.[Customer Number], C.[First Name], C.[Last Name], C.[Address]
FROM Customers C
LEFT JOIN Orders O
     ON C.[Customer Number]=O.[Customer Number]
LEFT JOIN Products P
     ON P.[Product Number] =O.[Product Number] AND P.[Part number]=O.[Part number]
WHERE O.[Order Date] BETWEEN '220101' AND '231231'
GROUP BY C.[Customer Number], C.[First Name], C.[Last Name],C.[Address]
HAVING SUM(IIF(O.[Order Date] BETWEEN '220101' AND '221231' , O.[Shipped Quantity]*P.price, 0))>=100000 AND 
       SUM(IIF(O.[Order Date] BETWEEN '230101' AND '231231', O.[Shipped Quantity]*P.price, 0))>=100000
ORDER BY C.[First Name]



SELECT C.[Customer Number], C.[First Name], C.[Last Name],C.Address
FROM Customers C
LEFT JOIN Orders O
     ON C.[Customer Number]=O.[Customer Number]
LEFT JOIN Products P
     ON P.[Product Number] =O.[Product Number] AND P.[Part number]=O.[Part number]
WHERE O.[Order Date] BETWEEN '220101' AND '231231'
GROUP BY C.[Customer Number], C.[First Name], C.[Last Name], C.[Address]
HAVING SUM(CASE WHEN O.[Order Date] BETWEEN '220101' AND '221231' THEN O.[Shipped Quantity]*P.price ELSE 0 END)>=100000 AND 
       SUM(CASE WHEN O.[Order Date] BETWEEN '230101' AND '231231' THEN O.[Shipped Quantity]*P.price ELSE 0 END)>=100000
ORDER BY C.[First Name]
