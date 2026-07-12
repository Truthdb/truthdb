ALTER TABLE items ADD CONSTRAINT ck_qty CHECK (qty >= 0);
ALTER TABLE items ADD CHECK ((price - qty) > 0);
ALTER TABLE items DROP CONSTRAINT ck_qty;
