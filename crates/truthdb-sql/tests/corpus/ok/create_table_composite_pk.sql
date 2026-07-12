CREATE TABLE order_lines (
    order_id INT NOT NULL,
    line_no SMALLINT NOT NULL,
    qty INT,
    PRIMARY KEY (order_id, line_no)
);
