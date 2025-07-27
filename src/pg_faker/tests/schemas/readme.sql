CREATE TABLE "user" (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE post (
    id SERIAL PRIMARY KEY,
    author_id INTEGER NOT NULL REFERENCES "user"(id),
    title VARCHAR(255) NOT NULL,
    content TEXT,
    published_at TIMESTAMPTZ,
    UNIQUE (author_id, title)
);
