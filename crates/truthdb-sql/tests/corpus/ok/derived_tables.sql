SELECT s.id, s.total FROM (SELECT id, amount + 1 AS total FROM sales WHERE amount > 0) s WHERE s.id < 10 ORDER BY s.id;
SELECT t.dept, d.total FROM sales t JOIN (SELECT dept, SUM(amount) AS total FROM sales GROUP BY dept) d ON t.dept = d.dept;
