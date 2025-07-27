CREATE TABLE parent (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);
CREATE TABLE child (
    id SERIAL PRIMARY KEY,
    parent_id INTEGER REFERENCES parent(id),
    name VARCHAR(100)
);
