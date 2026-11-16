-- Dataset versions are immutable snapshots. Version numbers are monotonic per
-- dataset and never reused. Only READY versions are exposed to discovery,
-- schema, and preview.
CREATE TABLE dataset_versions (
    id                uuid        PRIMARY KEY,
    dataset_id        uuid        NOT NULL REFERENCES datasets(id),
    version           integer     NOT NULL,
    object_prefix     text        NOT NULL,
    format            text        NOT NULL,
    partition_columns jsonb       NOT NULL DEFAULT '[]'::jsonb,
    status            text        NOT NULL,
    error_summary     text,
    row_count         bigint,
    created_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT dataset_versions_dataset_version_key UNIQUE (dataset_id, version)
);
