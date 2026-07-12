SELECT TOP 5 id, name AS product, price * 2
FROM products
WHERE price > 50 AND in_stock = 1 OR name IS NOT NULL
ORDER BY price DESC, id;
