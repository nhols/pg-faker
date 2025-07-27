-- Table with circular foreign keys
CREATE TABLE circ1 (
    id SERIAL PRIMARY KEY,
    circ2_id INTEGER
);
CREATE TABLE circ2 (
    id SERIAL PRIMARY KEY,
    circ1_id INTEGER REFERENCES circ1(id)
);
ALTER TABLE circ1 ADD CONSTRAINT fk_circ2 FOREIGN KEY (circ2_id) REFERENCES circ2(id);
