-- Ingestion jobs are the durable unit of work that registers or converts data
-- into a dataset version. Phase 2 records a synchronous register job; the
-- asynchronous lease/fencing machinery arrives with the ingestion worker in a
-- later phase.
CREATE TABLE ingestion_jobs (
    id                uuid        PRIMARY KEY,
    dataset_id        uuid        NOT NULL REFERENCES datasets(id),
    target_version_id uuid        REFERENCES dataset_versions(id),
    kind              text        NOT NULL,
    status            text        NOT NULL,
    input_ref         text,
    failure_reason    text,
    retryable         boolean     NOT NULL DEFAULT false,
    error_summary     text,
    retry_of          uuid        REFERENCES ingestion_jobs(id),
    attempt           integer     NOT NULL DEFAULT 0,
    fencing_token     uuid,
    lease_owner       text,
    lease_expires_at  timestamptz,
    created_at        timestamptz NOT NULL DEFAULT now(),
    started_at        timestamptz,
    finished_at       timestamptz
);
