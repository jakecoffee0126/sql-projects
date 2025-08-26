
SELECT DISTINCT  E.DepartmentId, D.Department, COUNT(*) over(partition by E.DepartmentId) AS Teemsize
FROM [employees] E
INNER JOIN Departments D
ON E.DepartmentId=D.DepartmentId
ORDER BY Teemsize DESC

SELECT  E.DepartmentId, D.Department, COUNT(*) AS Teemsize
FROM [employees] E
INNER JOIN Departments D
ON E.DepartmentId=D.DepartmentId
GROUP BY E.DepartmentId, D.Department
ORDER BY Teemsize DESC
