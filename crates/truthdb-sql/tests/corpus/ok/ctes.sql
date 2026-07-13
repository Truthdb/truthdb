WITH big AS (SELECT id, amount FROM sales WHERE amount >= 20) SELECT id FROM big ORDER BY id;
WITH a AS (SELECT id FROM sales), b AS (SELECT id FROM a WHERE id < 3) SELECT t.id FROM sales t JOIN b ON t.id = b.id;
