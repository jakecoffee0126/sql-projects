WITH GrossProfitMargin AS (SELECT [Product Number],[Part number],  [SKU - Description], 
       ROUND(SUM(([Unit Retail]-[Unit Cost])*[Shipped Quantity])/ SUM([Unit Retail]*[Shipped Quantity])*100.0, 2) AS GPM	        
FROM [CHTS ].[dbo].[orders]
WHERE [Unit Retail]*[Shipped Quantity]>0 
GROUP BY [Product Number],[Part number],  [SKU - Description]),

OrderCount AS(SELECT [Product Number],[Part number],  [SKU - Description], COUNT(*) AS FQ
FROM [CHTS ].[dbo].[orders]
GROUP BY [Product Number],[Part number],  [SKU - Description]),

ProductSorting AS(SELECT G.[Product Number],G.[Part number],  G.[SKU - Description],G.GPM, O.FQ AS OrderFrequency,
       CASE WHEN G.GPM>=30 AND O.FQ>=50 THEN 'Main'
	        WHEN (G.GPM>=30 AND O.FQ<50) OR (10<=G.GPM AND G.GPM<30 AND O.FQ>=50)  THEN 'Keep'
			WHEN ((10<=G.GPM AND G.GPM<30) AND (10<=O.FQ AND O.FQ<50)) OR (G.GPM<10 AND O.FQ>=50) THEN 'Keep Watch'
			WHEN G.GPM<10 AND O.FQ<5 THEN 'Discontinue'
			ELSE 'Price Adjustment'
			END AS Decision
FROM OrderCount O
INNER JOIN GrossProfitMargin G
ON O.[Product Number]=G.[Product Number] AND O.[Part number]=G.[Part number])
SELECT [Product Number],[Part number],  [SKU - Description], GPM, OrderFrequency
FROM ProductSorting
WHERE Decision='Discontinue'
ORDER BY [Product Number]
