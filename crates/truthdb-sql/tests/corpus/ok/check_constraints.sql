CREATE TABLE items (
    id INT NOT NULL PRIMARY KEY,
    qty INT CHECK (qty >= 0),
    price INT CONSTRAINT ck_pos CHECK (price > 0),
    CONSTRAINT ck_price CHECK ((price - qty) > 0),
    CONSTRAINT ck_range CHECK (qty > 0 AND (price < 100 OR price > 200))
);
