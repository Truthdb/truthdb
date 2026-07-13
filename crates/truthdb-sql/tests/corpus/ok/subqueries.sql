SELECT id FROM nums WHERE v = (SELECT MAX(v) FROM nums);
SELECT id, (SELECT COUNT(*) FROM picks) AS pc FROM nums WHERE id IN (SELECT target FROM picks) AND EXISTS (SELECT 1 FROM picks);
SELECT id FROM nums WHERE id NOT IN (SELECT target FROM picks WHERE target > 0);
