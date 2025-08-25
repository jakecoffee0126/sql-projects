SELECT P.[Product Number], P.[Part number], P.[SKU - Description], P.[Price]
FROM Products P
LEFT JOIN Orders O
ON O.[Product Number] = P.[Product Number]
  AND O.[Part number] = P.[Part number]
  AND O.[Order Date] BETWEEN '220101' AND '241231'
WHERE O.[Order Date] IS NULL

--SELECT P.[Product Number], P.[Part number], P.[SKU - Description], P.[Price]
--FROM Products P
--WHERE NOT EXISTS (
--    SELECT 1
--    FROM Orders O
--    WHERE O.[Product Number] = P.[Product Number]
--      AND O.[Part number] = P.[Part number]
--      AND O.[Order Date] BETWEEN '220101' AND '241231'
--)
