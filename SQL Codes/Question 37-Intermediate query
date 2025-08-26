CREATE FUNCTION getNthHighestSalary(@N INT)
RETURNS INT
AS
BEGIN
   DECLARE @result INT;
   WITH RankedSalary AS (
        SELECT DISTINCT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rk
		FROM employees)
   SELECT @result=salary
   FROM RankedSalary
   WHERE rk=@N;

   RETURN @result;
END;
GO

SELECT dbo.getNthHighestSalary(3) AS NthHighestSalary


/****** QUESTION 37 How to find the Nth highest salary of the employees  ******/

CREATE FUNCTION getNthHighestSalary1(@N INT) RETURNS INT AS
BEGIN
    RETURN (      
        SELECT DISTINCT salary
        FROM Employees
        ORDER BY salary DESC
        OFFSET @N-1 ROWS
        FETCH NEXT 1 ROWS ONLY
    );
END;

select dbo.getNthHighestSalary1(3)
