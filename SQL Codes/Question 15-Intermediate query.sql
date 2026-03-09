SELECT [Product Number]
      ,[Part number],     
      [order date],[Price] 
  FROM (select P.[product number], P.[Part number], P.[Price], O.[order number], O.[order date], rank() over(partition by P.[product number], P.[part number] order by O.[order date] DESC) AS rk
        FROM Products P
		INNER JOIN Orders O
		ON P.[Product Number]=O.[Product Number] AND P.[Part number]=O.[Part number]) A
WHERE rk=1
ORDER BY [Product Number], [Part number], [order number]




--SELECT DATEDIFF(DAY, 
--                CONVERT(DATE, '210723', 12), 
--                CAST(GETDATE() AS DATE)
--               ) AS DaysFromToday;
