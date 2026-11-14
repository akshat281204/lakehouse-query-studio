-- Durable query jobs. The statement is stored verbatim and is the only source
-- the worker executes. Leases and fencing tokens follow the ingestion pattern:
-- every transition out of RUNNING is conditional on the current token.
CREATE TABLE query_jobs (
    id                  uuid        PRIMARY KEY,
    statement           text        NOT NULL,
    statement_hash      text        NOT NULL,
    bindings            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    dataset_version_ids jsonb       NOT NULL DEFAULT '[]'::jsonb,
    status              text        NOT NULL,
    failure_reason      text,
    error_summary       text,
    rows_returned       bigint,
    truncated           boolean     NOT NULL DEFAULT false,
    result_ref          text,
    result_bytes        bigint,
    bytes_staged        bigint,
    plan_summary        text,
    duration_ms         bigint,
    attempt             integer     NOT NULL DEFAULT 0,
    fencing_token       uuid,
    lease_owner         text,
    lease_expires_at    timestamptz,
    cancel_requested    boolean     NOT NULL DEFAULT false,
    created_at          timestamptz NOT NULL DEFAULT now(),
    started_at          timestamptz,
    finished_at         timestamptz,
    CONSTRAINT query_jobs_status_chk CHECK (status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED','TIMED_OUT'))
);

CREATE INDEX query_jobs_active_idx ON query_jobs (created_at) WHERE status IN ('QUEUED', 'RUNNING');
CREATE INDEX query_jobs_history_idx ON query_jobs (created_at DESC);
