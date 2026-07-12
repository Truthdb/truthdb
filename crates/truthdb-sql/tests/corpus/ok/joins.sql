SELECT c.name, o.amount, s.*
FROM cust AS c
INNER JOIN ord o ON c.id = o.cust_id
LEFT OUTER JOIN ship s ON o.id = s.ord_id
CROSS JOIN region
WHERE o.amount > 100
ORDER BY c.name
