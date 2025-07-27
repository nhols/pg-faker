CREATE TABLE array_test (
    id SERIAL PRIMARY KEY,
    int_array INTEGER[],
    text_array TEXT[],
    jsonb_array JSONB[]
);