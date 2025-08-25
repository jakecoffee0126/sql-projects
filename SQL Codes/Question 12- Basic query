SELECT Name, SALARY
  FROM employees
  where employeeId NOT in (select ManagerId from employees where ManagerId is not null) AND 
        Salary>(select min(salary) from employees where employeeId IN (SELECT ManagerId from employees))



WITH Managers AS (
    SELECT DISTINCT ManagerId
    FROM employees
    WHERE ManagerId IS NULL),
   
  MinManagerSalary AS(
   SELECT MIN(Salary) AS MinSalary
   FROM employees
   WHERE EmployeeId IN (SELECT ManagerId FROM Managers),

  NonManagers AS(
  SELECT *
  FROM employees
  WHERE EmployeeId NOT IN (SELECT ManagerId FROM Managers)
SELECT Name, SALARY
FROM NonManagers
WHERE Salary>(SELECT MinSalary FROM MinManagerSalary)
ORDER BY Name;
