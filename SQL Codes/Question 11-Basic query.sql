SELECT 
      E.[DepartmentId], D.Department 
      ,round(AVG(SALARY),2) AS AvgSalary
  FROM employees E
  INNER JOIN Departments D
  on D.DepartmentId=E.DepartmentId
  GROUP BY E.DepartmentId, D.Department
  ORDER BY AvgSalary DESC


SELECT 
      distinct E.DepartmentId, D.Department
      ,ROUND(AVG(E.SALARY) over (partition by E.DepartmentId),2) as AvgSalary
  FROM employees E
  INNER JOIN Departments D
  ON E.DepartmentId=D.DepartmentId
  ORDER BY AvgSalary DESC
