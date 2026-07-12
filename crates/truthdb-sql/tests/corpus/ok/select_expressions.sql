SELECT
    CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END AS grade,
    CAST(price AS DECIMAL(10, 2)) AS p,
    UPPER(name) AS n,
    ISNULL(nickname, 'none') AS nick
FROM students
WHERE name LIKE 'A%' AND id IN (1, 2, 3) AND score BETWEEN 50 AND 100 AND name NOT LIKE '%z%'
ORDER BY grade
