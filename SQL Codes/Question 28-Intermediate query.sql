--METHOD 1
WITH OrderYears AS (SELECT [product number],[Part number],YEAR(CONVERT(date, CAST([Order date] AS char(8)), 12)) AS YearValue
                    FROM Orders),
CTE AS(SELECT [product number], [Part number],YearValue, COUNT(*) AS OrderCount
       FROM OrderYears
       GROUP BY [product number], [Part number],YearValue)

SELECT DISTINCT C1.[Product number], C1.[Part number], C1.YearValue AS FirstYear, C1.OrderCount, C2.YearValue AS NextYear, C2.OrderCount AS NextOrderCount
FROM  CTE C1
INNER JOIN CTE C2
ON C1.[Product Number]=C2.[Product Number] AND C1.[Part number]=C2.[Part number] AND C1.YearValue=C2.YearValue-1
WHERE C1.OrderCount>=10 AND C2.OrderCount>=10
ORDER BY [Product Number]


--METHOD 2
-- First CTE: clean up and extract YearValue once
WITH OrderYears AS (SELECT [product number], [Part number], YEAR(CONVERT(date, CAST([Order date] AS char(8)), 12)) AS YearValue
                    FROM Orders),
-- Second CTE: aggregate and calculate LEAD values
CTE AS (SELECT [product number],[Part number],YearValue AS FirstYear, COUNT(*) AS OrderCount,
                LEAD(YearValue) OVER (PARTITION BY [product number], [Part number] ORDER BY YearValue) AS NextYear,
                LEAD(COUNT(*)) OVER (PARTITION BY [product number], [Part number] ORDER BY YearValue) AS NextOrderCount
        FROM OrderYears
        GROUP BY [product number], [Part number], YearValue)
-- Final select
SELECT DISTINCT [Product Number], [Part number], FirstYear, OrderCount, NextYear, NextOrderCount
FROM CTE
WHERE FirstYear + 1 = NextYear   AND OrderCount >= 10  AND NextOrderCount >= 10
ORDER BY [Product Number];
