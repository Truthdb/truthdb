CREATE TABLE t (
    id INT NOT NULL PRIMARY KEY,
    email VARCHAR(50) UNIQUE,
    code INT,
    CONSTRAINT uq_code UNIQUE (code),
    UNIQUE (email, code)
);
