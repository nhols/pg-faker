-- Table with composite primary key and unique constraint
CREATE TABLE composite_pk (
    a INTEGER,
    b INTEGER,
    c TEXT,
    PRIMARY KEY (a, b),
    UNIQUE (c)
);
