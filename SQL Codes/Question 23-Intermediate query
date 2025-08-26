--Step 1: Calculate total sales per product per store in 2022
WITH CTE AS (SELECT [Store Number],[Product Number],[Part number],
                    SUM([Unit Retail]*[Shipped Quantity]) AS TotalSale    
              FROM [CHTS ].[dbo].[orders]
              WHERE [Order Date] between '220101' and '221231'
              GROUP BY [Store Number],[Product Number],[Part number]),

-- Step 2: Rank products in each store by total sales   
	CTE1 AS (SELECT [Store Number],[Product Number],[Part number], TotalSale, 
	                DENSE_RANK() over (partition by [store number] order by TotalSale DESC) AS rk
              FROM CTE),

--Step 3: Filter to top 3 products per store
    CTE2 AS (SELECT [Store Number],[Product Number],[Part number], TotalSale, rk
              FROM CTE1
              WHERE rk<=3)

--Step 4: Count how many stores each product appears in the top 3
SELECT TOP(5) [Product Number],[Part number], COUNT([Store number]) AS cnt
FROM CTE2
GROUP BY [Product Number],[Part number]
ORDER BY cnt DESC


WITH CTE AS (SELECT [Store Number],[Product Number],[Part number],
                    SUM([Unit Retail]*[Shipped Quantity]) AS TotalSale    
              FROM [CHTS ].[dbo].[orders]
              WHERE [Order Date] between '220101' and '221231'
              GROUP BY [Store Number],[Product Number],[Part number]),
    CTE1 AS (SELECT [Store Number],[Product Number],[Part number], TotalSale, 
	                DENSE_RANK() over (partition by [store number] order by TotalSale DESC) AS rk
              FROM CTE),

    CTE2 AS (SELECT [Store Number],[Product Number],[Part number], TotalSale, rk
              FROM CTE1
              WHERE rk<=3)
SELECT TOP(5) [Product Number],[Part number], COUNT([Store number]) AS cnt
FROM CTE2
GROUP BY [Product Number],[Part number]
ORDER BY cnt DESC

