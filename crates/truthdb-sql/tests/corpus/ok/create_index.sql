CREATE UNIQUE INDEX ix_users_email ON users (email);
CREATE INDEX ix_orders_customer ON orders (customer_id, created_at DESC);
DROP INDEX ix_users_email ON users
;
CREATE INDEX ix_users_lookup ON users (email) INCLUDE (id, display_name);
CREATE UNIQUE INDEX ix_code ON items (code DESC) INCLUDE (price)
