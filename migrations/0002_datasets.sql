-- Datasets are named, namespace-scoped collections of Parquet data. A name is
-- unique per namespace across both ACTIVE and ARCHIVED datasets, so archived
-- object keys can never be shadowed by a new dataset.
CREATE TABLE datasets (
    id              uuid        PRIMARY KEY,
    namespace_id    uuid        NOT NULL REFERENCES namespaces(id),
    name            text        NOT NULL,
    source_prefix   text        NOT NULL,
    format          text        NOT NULL,
    status          text        NOT NULL DEFAULT 'ACTIVE',
    retain_original boolean     NOT NULL DEFAULT false,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT datasets_namespace_name_key UNIQUE (namespace_id, name)
);
