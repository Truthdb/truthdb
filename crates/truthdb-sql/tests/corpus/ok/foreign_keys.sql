CREATE TABLE orders (
    id INT NOT NULL PRIMARY KEY,
    cust_id INT REFERENCES customers,
    prod_id INT CONSTRAINT fk_prod REFERENCES products (id),
    CONSTRAINT fk_cust FOREIGN KEY (cust_id) REFERENCES customers (id)
);
ALTER TABLE orders ADD CONSTRAINT fk2 FOREIGN KEY (prod_id) REFERENCES products (id);
ALTER TABLE orders ADD FOREIGN KEY (cust_id) REFERENCES customers;
