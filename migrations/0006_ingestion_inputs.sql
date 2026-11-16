-- Conversion jobs record the uploaded input's format and the requested
-- partition columns so a claimed job is self-describing and retries reuse them.
ALTER TABLE ingestion_jobs
    ADD COLUMN input_format      text,
    ADD COLUMN partition_columns jsonb NOT NULL DEFAULT '[]'::jsonb;

-- Workers claim runnable jobs by status and lease expiry.
CREATE INDEX ingestion_jobs_claim_idx ON ingestion_jobs (status, lease_expires_at, created_at);
