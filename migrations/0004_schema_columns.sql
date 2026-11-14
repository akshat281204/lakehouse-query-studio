-- Schema columns are the captured, immutable column descriptions for a single
-- dataset version. Position is unique per version.
CREATE TABLE schema_columns (
    id           uuid        PRIMARY KEY,
    version_id   uuid        NOT NULL REFERENCES dataset_versions(id) ON DELETE CASCADE,
    position     integer     NOT NULL,
    name         text        NOT NULL,
    type         text        NOT NULL,
    nullable     boolean     NOT NULL,
    is_partition boolean     NOT NULL DEFAULT false,
    CONSTRAINT schema_columns_version_position_key UNIQUE (version_id, position)
);
