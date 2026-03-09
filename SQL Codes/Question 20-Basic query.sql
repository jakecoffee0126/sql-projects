---Question 20 how to identify employees who are managers, along with how many people report directly to them and what their team’s average age is?


SELECT E.[EmployeeId]
      , E.[Name], COUNT(M.ManagerId) AS Reports_to, Round(Avg(M.age),0) AS Average_age
FROM [employees] E
INNER JOIN Employees M
ON E.EmployeeId=M.ManagerId
Group by E.EmployeeId, E.Name
ORDER BY E.EmployeeId

SELECT E.Employeeid,E.name ,count(M.ManagerId)as reports_count,round(avg(M.AGE),0) as average_age
from Employees E ,Employees M 
where E.EmployeeId =M.ManagerId 
group by  E.EmployeeId ,E.name 
order by  E.EmployeeId 
