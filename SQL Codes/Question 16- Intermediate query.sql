WITH cte AS (
  SELECT [Product Number],
         [Part Number],
         [Order Date],
         [Price],
         DATEDIFF(DAY, CONVERT(DATE, CAST([Order Date] AS VARCHAR(10)), 12), GETDATE()) / 365.0 AS YearsFromToday
  FROM (
    SELECT P.[Product Number], P.[Part Number], P.[Price], O.[Order Number], O.[Order Date],
           RANK() OVER (PARTITION BY P.[Product Number], P.[Part Number] ORDER BY O.[Order Date] DESC) AS rk
    FROM Products P
    INNER JOIN Orders O
      ON P.[Product Number] = O.[Product Number]
     AND P.[Part Number] = O.[Part Number]
  ) A
  WHERE rk = 1
)
SELECT [Product Number], [Part Number], [Order Date], [Price]
FROM cte
WHERE YearsFromToday > 3
ORDER BY [Product Number], [Part Number];
