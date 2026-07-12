CREATE UNIQUE INDEX ix_users_email ON users (email);
CREATE INDEX ix_orders_customer ON orders (customer_id, created_at DESC);
DROP INDEX ix_users_email ON users
