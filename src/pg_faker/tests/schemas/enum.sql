-- Create a custom enum type
CREATE TYPE ice_cream_flavour AS ENUM ('vanilla', 'chocolate', 'strawberry', 'mint', 'cookie_dough');

-- Example table using the custom enum type
CREATE TABLE person (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    favourite_flavour ice_cream_flavour NOT NULL
);