-- Saved queries: named, reusable SELECT statements. The statement is stored
-- verbatim; it is validated by the API when saved and again, like every
-- statement, by the validator and worker when it is run.
CREATE TABLE saved_queries (
    id          uuid        PRIMARY KEY,
    name        text        NOT NULL,
    description text        NOT NULL DEFAULT '',
    statement   text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT saved_queries_name_chk CHECK (char_length(name) BETWEEN 1 AND 120),
    CONSTRAINT saved_queries_description_chk CHECK (char_length(description) <= 500)
);

CREATE UNIQUE INDEX saved_queries_name_idx ON saved_queries (lower(name));
CREATE INDEX saved_queries_updated_idx ON saved_queries (updated_at DESC);
