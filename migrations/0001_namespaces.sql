-- Namespaces are the logical grouping/tenant boundary. The first release has
-- exactly one namespace ("default"), created once and never mutated. Its id is
-- a fixed constant so registration is deterministic across restarts.
CREATE TABLE namespaces (
    id         uuid        PRIMARY KEY,
    slug       text        NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO namespaces (id, slug)
VALUES ('00000000-0000-0000-0000-000000000001', 'default')
ON CONFLICT (slug) DO NOTHING;
