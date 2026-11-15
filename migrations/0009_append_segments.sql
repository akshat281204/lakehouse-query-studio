-- Appending data and direct Parquet import.
--
-- A dataset version is the files under its own object_prefix plus the files of
-- every prefix in base_segments (oldest first). A replacing upload has no base
-- segments; an append publishes a new version whose base_segments are the base
-- version's base_segments followed by the base version's own object_prefix, so
-- an append copies no data and earlier versions stay exactly as they were.
ALTER TABLE dataset_versions
    ADD COLUMN base_segments jsonb NOT NULL DEFAULT '[]'::jsonb;

-- 'replace' (the default, and every job before this migration) publishes only the
-- uploaded data; 'append' adds it to the latest READY version, which the job
-- records as its base so a concurrent change is detected when it publishes.
ALTER TABLE ingestion_jobs
    ADD COLUMN mode            text NOT NULL DEFAULT 'replace',
    ADD COLUMN base_version_id uuid REFERENCES dataset_versions(id),
    ADD CONSTRAINT ingestion_jobs_mode_chk CHECK (mode IN ('replace', 'append'));
