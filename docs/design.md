# Lakehouse Query Studio — Design Contract

> Phase 0 deliverable. Normative contract for data layout, catalog/version
> semantics, ingestion, query job lifecycle, the SQL safety boundary, result
> retention, and the single-node limit. Later phases implement against this
> document and must not silently contradict it.

## 1. Scope and status

This document resolves the design ambiguities listed in
`plan/00-design-contract.md`. It is descriptive (a design and contract), not
implementation code. It applies to the first release only: a single-node
DuckDB query layer over S3-compatible object storage.

Explicitly out of scope for this release (recorded here so nothing is promised
later):

- Distributed/multi-node query execution; any future engine (Trino, etc.) is
  deferred until a measured need justifies it.
- Multi-tenant identity, per-user authentication/authorization, RBAC, billing,
  governance, or a semantic BI layer. v1 is a single-operator, single-namespace
  deployment; the caller trust model and the enforceable exposure boundary are
  defined in §15.
- Arbitrary user-installed DuckDB extensions or unrestricted file/URL access.
- Iceberg/Delta/Hudi catalog semantics.

## 2. Terminology

| Term | Meaning |
|---|---|
| namespace | Logical grouping/tenant boundary. Exactly one default namespace (`default`) in v1. |
| dataset | A named, namespace-scoped logical collection of Parquet data with one or more versions. |
| dataset version | An immutable, versioned snapshot of a dataset pointing at a concrete object prefix. |
| ingestion job | Durable, asynchronous unit of work that registers or converts data into a dataset version. |
| query job | Durable, asynchronous unit of work that executes one validated read-only SQL statement. |
| object store | MinIO (local) or S3-compatible remote. Source of Parquet data. |
| catalog | PostgreSQL: durable metadata and job state. Never stores result payloads or credentials. |

## 3. Durable entities

Every entity that survives a process restart is listed here with its lifecycle
and state machine. "Durable" means stored in PostgreSQL (catalog) or object
storage (immutable data); in-memory state is always reconstructible.

### 3.1 Entity inventory

| Entity | Storage | Key fields | Terminal states |
|---|---|---|---|
| namespace | PostgreSQL | id, slug | ACTIVE (permanent in v1; namespace archival/delete is out of scope, §3.2) |
| dataset | PostgreSQL | id (UUID), namespace_id, name, source_prefix, format, status, retain_original (bool, default false), created_at, updated_at | ACTIVE, ARCHIVED |
| dataset_version | PostgreSQL | id, dataset_id, version (monotonic int), object_prefix, format, partition_columns, status, schema_snapshot_id, error_summary, created_at | READY, FAILED |
| schema_column | PostgreSQL | id, version_id, position, name, type, nullable, is_partition | — (rows) |
| ingestion_job | PostgreSQL | id (UUID), dataset_id, target_version, kind (register\|convert), status, input_ref, failure_reason, retryable, error_summary, retry_of, attempt, fencing_token, timestamps (created_at, started_at, finished_at), lease_owner, lease_expires_at, write_credential_expires_at, write_credential_reserved_until, write_credential_revoked, upload_released_at, source_upload_consumed_at | SUCCEEDED, FAILED, CANCELLED (state model §6.1) |
| query_job | PostgreSQL | id (UUID), statement (verbatim SQL text), statement_hash, dataset_version_ids, status, failure_reason, retryable, rows_returned, truncated, bytes_scanned, result_ref, plan_summary, attempt, fencing_token, timestamps (created_at, started_at, finished_at), lease_owner, lease_expires_at | SUCCEEDED, FAILED, CANCELLED, TIMED_OUT (state model §13) |
| object (Parquet) | object store | key under `lake/…` | — (immutable) |
| upload (original input) | object store | key under `uploads/<namespace>/<dataset>/<job-id>/<sanitized-original-name>` | retained-for-deployment (`retain_original=true`) or released/deleted (lifecycle §10) |
| staging object | object store | key under `staging/<namespace>/<dataset>/<job-id>/attempt=<n>/…` | — (swept after job terminal + write-quiescence, §4 rule 9) |
| spooled result | object store | key under `exports/…/attempt=<n>/…` | — (TTL-expired) |

`attempt` and `fencing_token` are the lease-fencing fields defined in §6.1 and
§13: `attempt` is a monotonically increasing integer incremented on every job
claim, and `fencing_token` is a random value regenerated on every claim. Only the
holder of a job's current `fencing_token` may transition it or publish its
output. `write_credential_expires_at` (ingestion jobs only) is the expiry of the
attempt-scoped, short-lived object-store write credential that authorizes writes
to `staging/` and `lake/`; it is the single authority for the write-quiescence
guarantee in §4 rule 8 and §6.1. It is NULL until the attempt's credential is
actually issued, and then records the credential's true expiry, which is never
earlier than the attempt's current `lease_expires_at` (§6.1) — the credential
always covers the entire lease window; a NULL value means no
credential was ever issued for that attempt, so there is no write capability to
quiesce and the sweeper treats the prefix as already quiescent (§4 rule 8).
`write_credential_session_ids` (ingestion jobs
only) is the set of provider session/credential IDs issued for this attempt —
the initial credential plus every renewal credential. An attempt can hold several
concurrently valid credentials, because a renewal's earlier credential remains
valid until its own expiry. `write_credential_expires_at` is the **maximum** true
expiry across all issued credentials and is monotonic: a new issuance may only
raise it, never lower it. `write_credential_revoked` is set `true` only after the
provider confirms revocation of **every** ID in
`write_credential_session_ids`; only then is `write_credential_expires_at`
advanced to `now()`. If any issued credential is unrevoked or unconfirmed, the
bound stays at its previous maximum and the sweeper waits for that expiry plus
`CLEANUP_GRACE` (§4 rule 8).
`write_credential_reserved_until` (ingestion jobs only) is the conservative upper
bound for a credential the worker is about to request. The worker persists it
**before** calling STS, conditional on the current `fencing_token`; every
credential it then requests is bounded by it. Because the reservation is durable
before the credential exists, a renewal that issues a credential and then loses
the fence cannot leave an unbounded credential behind: recovery and the sweeper
wait on `max(write_credential_expires_at, write_credential_reserved_until)`, so
even a credential issued during a fenced renewal is covered (§4 rule 8, §6.1).

`upload_released_at` and `source_upload_consumed_at` (ingestion jobs only) are the
two fields that make upload release and retry mutually exclusive (§10).
`upload_released_at` is set, in a locked catalog transaction, when an upload
prefix is declared due and handed to the object-store lifecycle rule; once set it
is a durable barrier that prevents any later retry from referencing the source
upload. `source_upload_consumed_at` is set by a retry the moment it has finished
re-staging the source upload into its own staging prefix, after which it no
longer reads the source; the sweeper may not delete the source while any
dependent retry has not yet consumed it (§10).

Two object-store entities are durable in their own right and are inventoried
above rather than being subsumed under "object (Parquet)": the **upload
(original input)** under `uploads/<namespace>/<dataset>/<job-id>/…` and the
**staging object** under `staging/<namespace>/<dataset>/<job-id>/attempt=<n>/…`.
Both survive process restarts and have distinct cleanup lifecycles, so each is
listed with its owning catalog reference and lifecycle/terminal conditions:

- `upload` is owned by the `ingestion_job` whose id appears in its key
  (`uploads/<namespace>/<dataset>/<job-id>/`). Its terminal condition and
  retention are fully determined by the job outcome and the dataset's
  `retain_original` policy: deleted on `READY` when `retain_original=false`,
  deleted after `UPLOAD_RETRY_WINDOW` following a terminal non-success when
  `retain_original=false`, or retained for the life of the deployment when
  `retain_original=true` (§10). It is released only by the serialized
  release/retry exclusion protocol in §10, which sets `upload_released_at`, and
  is never deleted while a dependent retry has not re-staged it.
- `staging` objects are owned by the `ingestion_job` (job id) together with the
  specific `attempt` (the `attempt=<n>` segment). Their terminal condition is
  "the owning job is terminal (`SUCCEEDED`/`FAILED`/`CANCELLED`) **and** the
  attempt's write credential has expired"; they are reclaimed by the
  stale-attempt sweeper after write-quiescence (§4 rules 6, 8, and 9), never at
  the terminal transition itself.

### 3.2 Dataset lifecycle

The single `default` namespace is created once at bootstrap and is **permanently
`ACTIVE`**; v1 defines no namespace transition, archive, or delete operation, and
a namespace row is never mutated after creation. Multi-namespace lifecycle is out
of scope (§1).

Dataset status: `ACTIVE` ↔ `ARCHIVED`.

- A dataset is created (ACTIVE) when its first version is requested.
- `ARCHIVED` is a reversible soft delete implemented at the catalog layer only.
  Archiving a dataset sets its status to `ARCHIVED`: it stops appearing in
  discovery, schema/preview, and query views, and no new ingestion or query job
  may target it. Archive **does not delete any version rows or any objects**;
  it only flips the status. Because the underlying versions and objects are
  untouched, unarchive (returning the dataset to `ACTIVE`) is always reversible
  and requires no data restoration. Hard delete and object GC are out of scope
  in v1 (§10).
- A dataset's name remains reserved while it is `ARCHIVED`: because names are
  unique across active and archived datasets (§5), a new dataset can never be
  created with an archived dataset's name, so archived object keys cannot be
  shadowed or overwritten.
- A dataset may have zero `READY` versions while its first version is being
  built or has failed; it is still an ACTIVE dataset record.

### 3.3 Dataset version lifecycle

Version status: `PENDING → BUILDING → READY | FAILED`.

| From | Event | To | Notes |
|---|---|---|---|
| (new) | ingestion/registration requested | PENDING | version row created, schema not yet exposed |
| PENDING | worker picks up job | BUILDING | schema inspection (register) or conversion (convert) in progress |
| BUILDING | all Parquet written + schema captured + catalog committed in one transaction | READY | becomes queryable atomically (§6) |
| BUILDING | unrecoverable error or cancellation | FAILED | error_summary set; never READY; staging and attempt-prefix objects reclaimed by the stale-attempt sweeper after write-quiescence (§4 rules 6 and 8), not at the transition itself |

- `READY` is the only state in which a version is exposed to the query engine
  or the dataset browser's schema/preview.
- `FAILED` is terminal for that version; a retry creates a new ingestion job on
  a fresh version number (an explicit `retry_of` link records the lineage).
- Version numbers are monotonically increasing integers per dataset and are
  never reused.

## 4. Object key layout

All keys are relative to the configured bucket. Layout is ASCII, URL-safe, and
deterministic. Namespace and dataset key components are **not** produced by
ad-hoc normalization: the API requires each name to already be in the canonical
key grammar — lowercase ASCII `[a-z0-9-]`, no leading/trailing hyphen, at most
63 characters — and rejects any other name. The key component is the name
itself, so the mapping from name to key component is the identity function and
therefore injective: two distinct accepted names can never share a key prefix.

```
# Converted / registered Parquet (immutable, the query source; attempt-isolated)
#   partition segments are percent-encoded name=value pairs (§4.1)
lake/<namespace>/<dataset>/version=<n>/attempt=<n>/[<enc-col>=<enc-value>/...]/part-<uuid>.parquet

# Original uploads (job-scoped durable input; retention per §10 — deleted on READY
# or after UPLOAD_RETRY_WINDOW, or kept for the life of the deployment when
# retain_original=true)
uploads/<namespace>/<dataset>/<job-id>/<sanitized-original-name>

# Ingestion staging (temporary, attempt-scoped; reclaimed by the sweeper after write-quiescence once the job is terminal — success included — with the worker's own success-time delete as a courtesy only)
staging/<namespace>/<dataset>/<job-id>/attempt=<n>/part-<uuid>.parquet

# Spooled query results (temporary; TTL; attempt-scoped)
exports/<namespace>/<query-id>/attempt=<n>/result.parquet
```

### 4.1 Partition key encoding (normative)

Hive-style partition directories are `name=value/` segments, but partition
column names and values are unrestricted UTF-8 strings that may contain `/`,
`=`, `%`, spaces, or non-ASCII bytes, so raw names and values cannot be used as
path components without making the key grammar ambiguous. Every partition
segment is produced by one reversible encoding applied to both the column name
and the value:

- `enc(s)` percent-encodes the UTF-8 bytes of `s`, leaving only the unreserved
  bytes `[A-Za-z0-9._~-]` unchanged and escaping every other byte — including
  `/` (`%2F`), `=` (`%3D`), `%` (`%25`), `+`, space, and every non-ASCII byte —
  as an uppercase `%HH` sequence. `enc` is injective.
- A **null** partition value is encoded as the reserved token `%00`. The value
  domain excludes U+0000 (NUL): ingestion rejects any partition value containing
  NUL, so no real value can encode to `%00`. Decoding therefore maps `%00` to
  null and every other encoded string back to its exact original value, making
  the mapping reversible over the allowed domain.
- A partition segment is `enc(name)=enc(value)`, where partition column names are
  non-empty. Because `enc` never emits `/` or `=`, each segment contains exactly
  one `=` and `/` occurs only as a segment separator, so parsing splits on the
  single `=` and percent-decodes each side. Segments are written in the
  dataset-declared partition column order, so the decoded column of each segment
  is recovered from its position as well as its name.
- Encoding is applied by the worker at write time and reversed at catalog/read
  time; the catalog's `schema_column` and partition metadata store the decoded
  name and value, never the encoded form.

**Testability.** Tests must cover: round-trip `decode(enc(s)) == s` over a corpus
including `/`, `=`, `%`, `+`, spaces, the empty string, and non-ASCII/Unicode
(CJK, combining marks, emoji); `enc` of any such value contains no unescaped `/`
or `=`; null round-trips to `%00` and back; and two distinct values never share
an encoded segment. Namespace and dataset components are already
ASCII-restricted (§4), so partition names and values are the only user-controlled
path components and use this rule.

The `attempt=<n>/` segment on `staging/`, `lake/`, and `exports/` keys is the
job's attempt counter (§6.1, §13); every (re)claim writes under its own attempt
segment, so a stale worker from a previous attempt cannot write under, or
clobber, a live attempt's output. A `dataset_version` becomes queryable only
when its fenced catalog commit records the winning attempt's prefix in
`object_prefix`; a losing/stale attempt prefix of a `READY` version, and every
attempt prefix of a `FAILED` version, are unreferenced and are removed by the
cleanup sweeper once the version is terminal and the attempt's write credential
has expired (rules 6 and 8). CSV export is streamed
from the spool and is not a separate object (§9).

Rules:

1. `lake/`, `uploads/`, `staging/`, and `exports/` are disjoint top-level
   prefixes; query execution is scoped to `lake/` only, and result serving is
   scoped to the single query's `exports/` prefix (§8, §9).
2. Hive-style partition directories are percent-encoded `enc(name)=enc(value)/`
   segments (§4.1) in the dataset-declared partition order; unpartitioned
   datasets write files directly under `version=<n>/attempt=<n>/`.
3. Part files are written once and never mutated; a version commit is a
   fenced catalog pointer update to the winning attempt prefix, not an object
   rewrite.
4. Original uploads are retained only when the dataset/namespace policy sets
   `retain_original=true`; otherwise upload objects are removed after the
   version reaches `READY`. For a job that never reaches `READY` (failed,
   cancelled, or crash-recovered), the upload is retained for the bounded
   `UPLOAD_RETRY_WINDOW` and then removed — never left indefinitely (§10).
5. No object key appears in client-visible data (§8).
6. **Stale-attempt cleanup (conclusively abandoned attempts only)**: a sweeper
   deletes a `lake/<namespace>/<dataset>/version=<n>/attempt=<m>/` prefix only
   when the catalog proves that attempt is *conclusively abandoned* — it can no
   longer be published and is not the version's committed prefix. The abandoned
   set is exactly:
   - every attempt prefix of a version whose status is terminal `FAILED` (a
     `FAILED` version can never become `READY`, §3.3), and
   - every attempt prefix whose `attempt` differs from a `READY` version's
     committed `object_prefix` attempt (a losing/stale attempt).
   A version in `PENDING` or `BUILDING` has **no** abandoned attempt prefix: its
   winning `object_prefix` is not yet committed and a live worker may be writing
   under it, so the sweeper deletes nothing for it. This bounds orphan retention
   (stale/losing attempts, and the whole prefix set of a failed version) without
   ever touching a live or newly committed attempt.
7. **Cleanup is serialized against publication**: the sweeper's delete decision
   is re-verified against the catalog and serialized with the fenced commit that
   publishes a version, so a live or newly committed attempt can never be
   deleted. Concretely, the sweeper takes a per-version lock (the same lock the
   fenced `BUILDING → READY` commit holds while it records `object_prefix`),
   re-reads the version's `status` and `object_prefix` under that lock,
   recomputes the abandoned set from rule 6, and only then deletes those
   prefixes (subject to the write-quiescence precondition in rule 8). Because
   publication acquires the same lock before making a version
   `READY`, the state the sweeper re-checked cannot change to reference a prefix
   between the re-check and the delete. The sweep is idempotent (deleting an
   already-deleted prefix is a no-op) and may run on any schedule; each pass
   recomputes the abandoned set from the catalog, so a crashed sweeper re-runs
   safely.
8. **Cleanup waits for write-quiescence (no writes after delete)**: the sweeper
   deletes an abandoned attempt prefix only after it can prove that no credential
   remains that could write to it. Ingestion writes to `lake/` and `staging/` are
   authorized solely by an attempt-scoped, short-lived STS credential whose TTL
   is the full lease window plus a slack margin at issuance time, so its expiry
   is never earlier than the attempt's current `lease_expires_at` (§6.1); the
   credential always covers the entire lease window. There is no long-lived,
   deployment-wide `lake/` write credential on the ingestion path. The worker
   stores that credential's actual expiry in
   `write_credential_expires_at` (§3.1), and persists a conservative
   `write_credential_reserved_until` **before** requesting any credential. The
   sweeper deletes an abandoned attempt prefix only when
   `now() > max(write_credential_expires_at, write_credential_reserved_until) +
   CLEANUP_GRACE`, where `CLEANUP_GRACE` (default 15 minutes) strictly exceeds
   the object store's
   documented maximum time to complete an already-accepted write for the write
   modes below. After the credential has expired the object store rejects any
   further authenticated write to that prefix, so a delayed (fenced) worker
   cannot write after the sweeper's delete.
   - **Write modes are bounded.** Attempt-scoped writes to `staging/` and
     `lake/` use single-part `PutObject` only; multipart uploads are disabled
     for these prefixes and large inputs are split into part files of at most
     64 MiB each. A single-part PUT accepted before credential expiry therefore
     completes within the provider's documented request bound, which
     `CLEANUP_GRACE` exceeds. If multipart writes are ever introduced, the
     contract requires the writer to record the upload IDs, the sweeper to abort
     every listed upload, and the sweeper to wait for abort completion before
     deleting; until then multipart on attempt prefixes is a protocol violation
     the object-store policy denies.
   - **Delete-then-verify loop.** The sweeper deletes the prefix, waits
     `CLEANUP_GRACE`, and re-lists it. If any object is present (only possible
     for a write accepted before credential expiry), it re-applies the rule-6
     decision and deletes again; it declares the prefix quiescent only after two
     consecutive empty listings. It never reports cleanup complete on a single
     delete, and a prefix that fails to quiesce raises an operational alert
     rather than being silently abandoned.
   Fencing governs *publication* (the token check in §6.1); credential expiry is
   what governs *writes*. Because each renewal issues a new credential while the
   previous one remains valid until its own expiry, `write_credential_expires_at`
   is the **maximum** expiry across every credential in
   `write_credential_session_ids`, never lowered by a newer issuance. Revocation
   is optional hardening that shortens the window: only if the STS provider
   confirms revocation of **every** session in `write_credential_session_ids`
   does the fencing transaction record `write_credential_revoked=true` and set
   `write_credential_expires_at = now()`; if any session is unrevoked or
   unconfirmed, it **keeps the maximum expiry and the reservation unchanged**
   (never lowers either to `now()`), and the sweeper waits for that bound plus
   `CLEANUP_GRACE`. A version in `PENDING`/`BUILDING` has no
   expired-credential abandoned prefix and is never
   swept (rule 6). The sweeper's bound is always
   `max(write_credential_expires_at, write_credential_reserved_until)`, so a
   credential issued by a renewal that lost the fence is covered by the
   reservation even though its actual expiry was never persisted.
9. **Staging cleanup for terminal jobs (success included).** The
   `staging/<namespace>/<dataset>/<job-id>/attempt=<n>/` prefix is *conclusively
   abandoned* exactly when its `ingestion_job` is terminal — `SUCCEEDED`,
   `FAILED`, or `CANCELLED` — because a terminal job can never write staging
   again. The worker's own best-effort delete of a successful attempt's staging
   (after the fenced `READY` commit) is a courtesy, **not** the retention
   guarantee: the stale-attempt sweeper is the authoritative backstop for the
   staging of **every** terminal job, success included, and deletes it under the
   same two preconditions as `lake/` — serialized against the fenced
   `BUILDING → READY` commit by the per-version lock (rule 7), and only after
   write-quiescence
   `now() > max(write_credential_expires_at, write_credential_reserved_until) +
   CLEANUP_GRACE` (rule 8). Serialization runs in both directions: the sweeper
   re-reads the job's `status` under the per-version lock and deletes a staging
   prefix only if the job is already terminal in that read, so it can never
   delete the staging of an attempt whose `RUNNING → SUCCEEDED` transition (the
   fenced `READY` commit) has not yet landed. A worker that crashes after the
   fenced `READY`/`SUCCEEDED` commit but before deleting its own staging
   therefore strands nothing: once the job is `SUCCEEDED`, the sweeper reclaims
   the staging prefix after write-quiescence exactly as it reclaims a failed
   attempt's staging. The successful attempt's `lake/` prefix is the committed
   `object_prefix` and is **never** swept (rules 6 and 7); only its `staging/`
   prefix is.

## 5. Catalog and version semantics

- A version's object prefix is the winning attempt prefix,
  `lake/<namespace>/<dataset>/version=<n>/attempt=<n>/`, recorded in
  `dataset_version.object_prefix` by the fenced commit (§6.1).
- The schema snapshot (columns, types, nullability, partition columns) is
  captured and stored with the version and becomes immutable once the version
  is `READY`.
- The catalog never stores credentials, secret keys, or raw result payloads.
- Timestamps are stored as UTC; `created_at`/`updated_at` on all mutable rows.
- Uniqueness: `(namespace, dataset name)` is unique across **all** datasets,
  active and archived. Dataset names are never reused: archiving reserves the
  name permanently (§3.2), so `lake/<namespace>/<dataset>/…` keys can never
  collide with an archived dataset's objects. Uniqueness is enforced on the
  canonical name, which is also the literal key component (§4), so there is no
  normalization step that could map two distinct names to one prefix. Version
  number is unique per dataset; job ids are UUIDs.

## 6. Ingestion behavior

Two kinds of ingestion job:

| Kind | Input | Action | Output |
|---|---|---|---|
| `register` | an existing Parquet prefix in the object store | validate prefix, inspect schema, capture partition metadata, then materialize the version under its canonical attempt prefix | version `READY` whose `object_prefix` is its own `lake/<namespace>/<dataset>/version=<n>/attempt=<n>/` prefix (source objects are copied there; copying may be skipped only when the source prefix is byte-for-byte the target canonical attempt prefix, e.g. a resumed same-attempt write) |
| `convert` | uploaded CSV/JSON/JSONL | validate, infer schema, convert to Parquet under staging, then commit | version `READY` with new `lake/…` objects |

Common rules:

- Ingestion is asynchronous and persisted as an `ingestion_job`. Each attempt is
  paired with exactly one target `dataset_version` (status
  `PENDING → BUILDING → READY | FAILED`, §3.3).
- Conversion writes to `staging/<namespace>/<dataset>/<job-id>/attempt=<n>/`
  first and then writes the complete Parquet parts to the attempt-isolated
  durable prefix
  `lake/<namespace>/<dataset>/version=<n>/attempt=<n>/`. Publication is the
  fenced catalog commit (§6.1): the version becomes `READY` and its
  `object_prefix` points at the winning attempt prefix in one conditional
  transaction. A partial or losing attempt is never referenced; its objects are
  removed by the stale-attempt sweeper once the version is terminal and the
  attempt's write credential has expired (§4 rules 6 and 8),
  never while it is `PENDING`/`BUILDING`. A partial version is never exposed as
  `READY`.
- Failure never leaves a `READY` version; a partial version is never exposed as
  `READY`. Staging and `lake/…/version=<n>/attempt=<n>/` objects of a failed,
  cancelled, or crashed attempt are reclaimed **only** by the stale-attempt
  sweeper after write-quiescence — `max(write_credential_expires_at, write_credential_reserved_until) + CLEANUP_GRACE`
  (§4 rules 6 and 8) — never at the cancel/fail transition itself, which occurs
  while the attempt write credential may still authorize writes. Staging of a
  *successful* attempt is best-effort deleted by the transitioning worker after
  the `READY` commit (it has stopped writing, so no further staging write can
  occur), but that self-delete is a courtesy, not the guarantee: if the worker
  crashes after the fenced `READY`/`SUCCEEDED` commit but before deleting its
  staging, the stale-attempt sweeper reclaims the staging prefix once the job is
  `SUCCEEDED` and write-quiescence has passed (§4 rule 9), so successful staging
  is never left indefinitely.
- Partition columns are explicit and user-selected with a safe default (no
  partitioning, or only low-cardinality columns); high-cardinality partitioning
  is not applied by default.

### 6.1 Ingestion job state machine

Ingestion status: `QUEUED → RUNNING → SUCCEEDED | FAILED | CANCELLED`. The job
is the unit of work; its target version is the artifact it produces or fails to
produce, and the two states move together.

Every `ingestion_job` carries two lease-fencing fields and two write-quiescence
fields (inventoried in §3.1). The fencing fields make stale-worker *commits*
impossible; the write-credential fields make stale-worker *writes* impossible
(§4 rule 8):

- `attempt` — a monotonically increasing integer, incremented on every job
  claim. The initial claim is attempt 1.
- `fencing_token` — a random value (UUID) regenerated on every claim. Only the
  holder of the job's **current** `fencing_token` may transition the job or its
  target version out of a non-terminal state.
- `write_credential_expires_at` — the actual expiry of the attempt-scoped,
  short-lived write credential that authorizes this attempt's writes to
  `staging/` and `lake/`. It is NULL until the credential is actually issued;
  once issued, the credential's TTL is the full lease window plus a slack
  margin at issuance time, so its expiry is never earlier than the attempt's
  current `lease_expires_at` — the credential always covers the entire lease
  window.
  The field records the true expiry returned by the provider, persisted before
  any write (see "Credential issuance" below). The stale-attempt sweeper deletes
  an abandoned attempt prefix only after this time plus `CLEANUP_GRACE` (§4 rule
  8); a NULL value means no credential was ever issued, so there is no write
  capability to quiesce and the sweeper may reclaim the (empty) prefix
  immediately.
- `write_credential_revoked` — whether the STS provider confirmed per-session
  revocation of the attempt credential. Set `true` only after a confirmed
  revocation, and only then is `write_credential_expires_at` advanced to the
  revocation instant (`now()`); when revocation is unsupported or unconfirmed,
  both fields keep their original values and the sweeper waits for natural
  expiry plus `CLEANUP_GRACE` (§4 rule 8).

**Credential issuance (normative ordering: reserve → issue → persist).** The
lease deadline is set by the claim — `lease_expires_at = claim_time +
LEASE_DURATION` — and the write credential is sized to cover that full window
plus a slack margin, never the reverse. Issuance is a three-step protocol whose
first step is durable *before* any credential exists:

1. **Reserve (conditional, must commit first).** The worker computes the bound
   `reserved_until = lease_expires_at + CREDENTIAL_SLACK` (default slack 30s,
   configurable, covering STS latency and clock skew) and persists it keyed on
   the current token and `status='RUNNING'`:
   `UPDATE … SET write_credential_reserved_until=? WHERE id=? AND fencing_token=?
   AND status='RUNNING'`. A zero-row result means fenced — **no credential is
   requested**.
2. **Issue.** Only after the reservation commits does the worker request an
   attempt-scoped write credential whose TTL is
   `reserved_until − now()` (so its nominal expiry is `reserved_until`). It
   **verifies** the provider's actual expiry `E` satisfies both
   `E ≥ lease_expires_at` (the credential covers the full lease window) and
   `E ≤ reserved_until` (it does not outlive the durable reservation); an expiry
   outside either bound is rejected (treated as an issuance failure).
3. **Persist actual expiry.** The worker records the credential's true expiry
   and adds the new session ID in one conditional update keyed on the current
   token and `status='RUNNING'`, never lowering an existing bound:
   `UPDATE … SET write_credential_expires_at =
   GREATEST(COALESCE(write_credential_expires_at,'epoch'), ?),
   write_credential_session_ids = write_credential_session_ids || ?,
   write_credential_reserved_until=NULL
   WHERE id=? AND fencing_token=? AND status='RUNNING'`. **No `staging/` or
   `lake/` write may occur before that commit succeeds.**

The reservation is what closes the renewal race: if the job is fenced between
steps 1 and 3 (for example, the old lease expires and recovery runs while the STS
call is in flight), the conditional updates in steps 1 and 3 fail or the
recovery pass runs, but the credential issued in step 2 is bounded by the
already-durable `reserved_until`. Recovery never lowers that bound; it carries
`max(write_credential_expires_at, write_credential_reserved_until)` forward, so
the sweeper cannot delete the prefix until every credential that could possibly
have been issued (including one whose expiry was never persisted) has expired,
plus `CLEANUP_GRACE` (§4 rule 8). The worker also best-effort revokes a
credential it obtained but could not persist.

**Renewal re-issues the credential with every lease extension (commit-last).**
The lease is a sliding window renewed by heartbeat (§12.1), and each renewal
re-issues the attempt write credential because that credential must keep covering
the extended lease window. Renewal repeats the reserve → issue → persist ordering
above, with one normative difference: **the lease is extended only in the
persist step, atomically with the replacement credential's expiry — never in the
reserve step.** Step 1 computes the target new deadline
`D = now() + LEASE_DURATION`, persists only
`write_credential_reserved_until = D + CREDENTIAL_SLACK`, and leaves
`lease_expires_at` unchanged; step 2 issues the credential; step 3 is a single
conditional update (keyed on the current token and `status='RUNNING'`) that sets
`lease_expires_at = D` **and**
`write_credential_expires_at = GREATEST(COALESCE(write_credential_expires_at, 'epoch'), <actual expiry>)`
**and** appends the new session ID to `write_credential_session_ids` (and clears
the reservation) together, where the actual expiry satisfies
`D ≤ <actual expiry> ≤ D + CREDENTIAL_SLACK`. Because the previous credential
stays valid until its own (possibly later) expiry, the persisted expiry is the
**maximum** across every credential in `write_credential_session_ids` — never
lowered by a newer issuance — and every session ID is retained so revocation can
cover the full set (§3.1, §4 rule 8). Because only a
committed step 3 extends the lease, the lease can never be extended without a
replacement credential that already covers the new window: if the STS call
fails or the provider returns an expiry outside `[D, D + CREDENTIAL_SLACK]`,
step 3 is not committed and the lease is not extended, and the worker retries
the combined renewal within the current window or stops writing and fails
retryably (§12.1). An active
ingestion therefore never reaches the state "lease alive, write credential
expired" — the invariant `write_credential_expires_at ≥ lease_expires_at` holds
at every instant a credential exists — and a renewal that loses the fence after
step 2 leaves a credential bounded by the durable step-1 reservation, which the
sweeper honors (§4 rule 8).

**Credential-issuance failure.** If the credential cannot be issued with an
expiry that covers the full lease window and respects the reservation —
`lease_expires_at ≤ E ≤ reserved_until` at claim, or `D ≤ E ≤ D + CREDENTIAL_SLACK`
at renewal — because the STS call fails, the requested TTL falls below a minimum
usable threshold `MIN_WRITE_CREDENTIAL_TTL` (default 30s, configurable), or the
provider returns an expiry outside those bounds — the worker performs **no
write**. It releases the claim in one conditional
transaction keyed on the current token and `status='RUNNING'`:
`UPDATE … SET status='FAILED', failure_reason='credential_issue_failed',
retryable=true, attempt=attempt+1, fencing_token=NULL, lease_owner=NULL,
lease_expires_at=NULL
WHERE id=? AND fencing_token=? AND status='RUNNING'`,
and in the same transaction marks the target version
`FAILED WHERE id=<target_version> AND status='BUILDING'`.
`write_credential_expires_at` is **not lowered**: it keeps the maximum expiry of
any credential previously issued for the attempt (NULL only if none was ever
issued), while any `write_credential_reserved_until` committed in step 1 is
**retained, never lowered**, so the sweeper's bound is the larger of the two and
it cannot delete before every credential that could have been issued is expired.
The sweeper may reclaim the (empty) attempt prefix once that bound plus
`CLEANUP_GRACE` has passed (§4 rule 8). A failed issuance therefore never leaves
an unbounded credential. (The mirrored case — the credential is issued but the
persist update is fenced — aborts with the same no-write rule: the worker
discards the credential, best-effort revokes it, and never writes; the durable
reservation from step 1 bounds its expiry, so it cannot outlive the sweep
bound.)

All worker-initiated transitions and the final commit are **conditional** on the
token and the expected current status, for example
`UPDATE … WHERE id = ? AND fencing_token = ? AND status = 'RUNNING'`. A
conditional update that affects **zero rows** means the worker has been fenced —
its token is no longer current — and it **must** abort: it discards local state
and never transitions state. While its own attempt-scoped write credential is
still valid, it also best-effort deletes its own `staging/…/attempt=<n>/` and any
objects it wrote under its own `lake/…/version=<n>/attempt=<n>/` prefix; this
self-delete is a courtesy, not the guarantee. The stale-attempt sweeper
(§4 rules 6 and 8) is the authoritative backstop that reclaims an orphaned
attempt prefix — but only after the attempt's write credential has expired plus
`CLEANUP_GRACE`, so the sweeper never races a still-credentialed writer. Expiry
is the *trigger* for fencing, but the fence is *enforced* at commit time by the
token check, so a merely delayed (not dead) worker is fenced the instant a
recovery pass re-claims its job, before it can commit a stale result.

| From | Event | To | Notes |
|---|---|---|---|
| (new) | ingestion requested (`register` or `convert`) | QUEUED | `ingestion_job` row created with `attempt = 0` and no token; target `dataset_version` created as `PENDING`; `retry_of` set when this is a retry |
| QUEUED | worker claims the job | RUNNING | conditional `UPDATE … SET status='RUNNING', lease_owner=?, lease_expires_at=now()+LEASE_DURATION, attempt=attempt+1, fencing_token=<fresh UUID> WHERE id=? AND status='QUEUED'`; exactly one winner. The claim sets the initial lease deadline (later renewed by heartbeat, §12.1) only — it does **not** set `write_credential_expires_at`, which stays NULL until the credential is actually issued. The worker then issues the attempt-scoped write credential against that deadline and persists its actual expiry before writing (see "Credential issuance" above); version `PENDING` → `BUILDING` |
| RUNNING | all Parquet written to `lake/…/version=<n>/attempt=<n>/` + schema captured, then commit | SUCCEEDED | one conditional transaction sets the version `READY` with `object_prefix` = this attempt's prefix and the job `SUCCEEDED`, both `WHERE … fencing_token=? AND status='RUNNING'`; zero rows → fenced, abort (rules 6 and 8 sweep any straggler after write-credential expiry) |
| RUNNING | cancel accepted | CANCELLED | conditional on token; version → `FAILED` with `error_summary = "cancelled"`. No object deletion here: cancellation occurs while the attempt write credential may still authorize writes, so staging/lake reclamation is deferred to the stale-attempt sweeper, which deletes only after write-quiescence — `max(write_credential_expires_at, write_credential_reserved_until) + CLEANUP_GRACE`, shortened by revoking the attempt credential where supported (§4 rules 6 and 8) |
| RUNNING | unrecoverable error | FAILED | conditional on token; `failure_reason` + `error_summary` set; version → `FAILED`. No object deletion here: staging/lake reclamation is deferred to the stale-attempt sweeper after write-quiescence — `max(write_credential_expires_at, write_credential_reserved_until) + CLEANUP_GRACE`, shortened by revoking the attempt credential where supported (§4 rules 6 and 8) |
| RUNNING | lease expires (worker crashed or partitioned) | FAILED (retryable) | recovery pass invalidates the token (increments `attempt`, clears `fencing_token`) **and atomically marks the target version `FAILED`** in one transaction; `write_credential_expires_at` is set to `now()` and `write_credential_revoked=true` **only when** the STS provider confirms revocation of **every** session in `write_credential_session_ids`; otherwise recovery sets `write_credential_expires_at = max(recorded expiry, write_credential_reserved_until)` and keeps the reservation, so a credential issued by a renewal that lost the fence is still covered (§4 rule 8); the old worker is fenced by the token change and can no longer write once every possible credential has expired |

- `QUEUED` jobs survive worker restart: state lives in PostgreSQL and the queue
  is reconstructed by scanning for `QUEUED` rows.
- A `RUNNING` job's lease is a sliding window renewed by the holding worker's
  heartbeat (§12.1). Ingestion renewal is commit-last: the worker first
  refreshes the attempt write credential and then, in the **same** conditional
  update that persists the credential's new expiry, extends `lease_expires_at`,
  preserving `write_credential_expires_at ≥ lease_expires_at` and ensuring the
  credential never expires before the lease. A renewal that affects zero rows
  means the worker was fenced; it must abort without writing.
- A worker that crashes mid-attempt leaves the job `RUNNING` with an expired
  lease. A recovery pass (run by any worker's lease/heartbeat cycle) performs a
  single atomic transaction that (a) conditionally updates the job keyed on the
  expired lease, applying **exactly one** of two mutually exclusive expiry rules,
  and (b) in the same transaction marks the target version
  `FAILED WHERE id=<target_version> AND status='BUILDING'`:

  - **Revocation confirmed** — the STS provider supports per-session revocation
    and confirms **every** session in `write_credential_session_ids` is revoked:
    `UPDATE … SET status='FAILED', failure_reason='worker_crash', retryable=true,
    attempt=attempt+1, fencing_token=NULL, lease_owner=NULL, lease_expires_at=NULL,
    write_credential_expires_at=now(), write_credential_revoked=true
    WHERE id=? AND status='RUNNING' AND lease_expires_at < now()`.
  - **Revocation unsupported or unconfirmed** — the same update without setting
    `now()`, instead carrying the reservation forward:
    `UPDATE … SET status='FAILED', failure_reason='worker_crash', retryable=true,
    attempt=attempt+1, fencing_token=NULL, lease_owner=NULL, lease_expires_at=NULL,
    write_credential_expires_at = GREATEST(COALESCE(write_credential_expires_at,
    'epoch'), COALESCE(write_credential_reserved_until, 'epoch'))
    WHERE id=? AND status='RUNNING' AND lease_expires_at < now()`,
    which never lowers the expiry bound, keeps `write_credential_reserved_until`,
    and leaves `write_credential_revoked` false.

  `now()` is recorded as the expiry only in the confirmed-revocation branch;
  there is no code path that lowers or overwrites the issuance-recorded expiry
  without such confirmation, so the sweeper always waits for
  `max(write_credential_expires_at, write_credential_reserved_until) +
  CLEANUP_GRACE` when revocation was not confirmed (§4 rule 8). This includes a
  credential issued by a renewal that was fenced before persisting its expiry:
  the durable reservation covers it. Job and
  version can therefore never diverge: a crashed attempt cannot leave a
  permanently `BUILDING` version. Deletion of the old attempt's `staging/` and
  `lake/…/version=<n>/attempt=<old>/` prefixes is **not** done by this pass; it
  is deferred to the stale-attempt sweeper, which deletes only after that
  reservation/expiry bound plus `CLEANUP_GRACE` (§4 rule 8). Fencing the token
  stops the delayed worker from *publishing*; expiry/revocation of the
  attempt-scoped write credential is what stops it from *writing*, so the sweeper
  never deletes a prefix a still-credentialed writer can write to, and a delayed
  worker whose credential has expired cannot write after the delete. Because the
  old token no longer matches, any later commit by the delayed worker affects
  zero rows and is detected; the worker aborts without transitioning state.
  Interrupted jobs are never silently `SUCCEEDED`, and a partitioned-but-alive
  worker cannot overwrite the terminal state the recovery pass set or write into
  a swept prefix after its credential has expired.
- **Delayed-worker / delete race test**: an adversarial test fences an attempt
  (invalidates the token, marks the version `FAILED`, sets
  `write_credential_expires_at` in the past and/or revokes the credential), keeps
  the "stale" worker alive and issuing `PutObject` to its old attempt prefix,
  runs the sweeper, and asserts that (1) every post-fence write is rejected by
  the object store with an authorization error, (2) the sweeper's delete leaves
  the prefix empty and no new object appears afterward, and (3) no orphan object
  is observable after the sweeper completes. A companion test asserts that a live
  (`BUILDING`) attempt's prefix is never deleted while its credential is
  unexpired.
- **Lease/heartbeat test**: a long ingestion whose worker heartbeats across
  several `LEASE_DURATION`s never loses its write credential: after the claim
  and after every renewal the test asserts the invariant
  `write_credential_expires_at ≥ lease_expires_at` (the credential never expires before the lease), that the recorded expiry
  advances with each renewal, and that `PutObject` to the attempt prefix keeps
  succeeding until the job completes. A companion test stops the heartbeat and
  asserts that, after the lease expires, the recovery pass re-claims the job
  (`FAILED`, retryable), the old token no longer commits, and the sweeper deletes
  the prefix only after `max(write_credential_expires_at, write_credential_reserved_until) + CLEANUP_GRACE` (§4 rule 8).
- **Fenced-renewal reservation test**: a worker commits a renewal reservation,
  obtains a credential, and is fenced before persisting the credential's actual
  expiry. The test asserts that recovery carries the reservation bound forward,
  that the sweeper deletes only after
  `max(write_credential_expires_at, write_credential_reserved_until) +
  CLEANUP_GRACE`, and that no delete occurs while the orphaned credential is
  still valid — a write it issues before natural expiry is either rejected
  (revocation confirmed) or occurs before the sweep bound, never after it.
- **Renewal all-or-nothing test (commit-last)**: a renewal whose STS call fails
  or whose provider returns an expiry outside `[D, D + CREDENTIAL_SLACK]` commits
  **no** lease extension — the lease and the credential expiry are written
  together in step 3 only after a credential that covers the new window exists —
  and the worker either retries the combined renewal or stops writing and fails
  retryably; it never reaches the state "lease alive, write credential expired".
  The test also asserts that after every committed renewal the invariant
  `write_credential_expires_at ≥ lease_expires_at` holds (the credential never
  expires before the lease), and that `lease_expires_at` is observed never to
  advance unless `write_credential_expires_at` advances in the same transaction
  to a value at least as late (§12.1). A
  companion test drives a renewal that loses the fence after step 2 and asserts
  the lease was not extended, recovery fences the job when the (unextended)
  lease expires, and the sweeper deletes the prefix only after
  `max(write_credential_expires_at, write_credential_reserved_until) +
  CLEANUP_GRACE` (§4 rule 8).
- **Renewal-then-revocation cleanup test**: an attempt issues credential A at
  claim and credential B at a renewal, so A and B are both valid and A expires
  after B's issuance. Recovery confirms revocation of B only. The test asserts
  that `write_credential_expires_at` remains the maximum expiry (A's), that the
  sweeper does not delete before A expires plus `CLEANUP_GRACE`, and that a write
  using A before its expiry is either rejected (both sessions revoked) or occurs
  before the sweep bound, never after the prefix is deleted. A companion case
  confirms that only when **every** session ID is confirmed revoked is the bound
  advanced to `now()` and cleanup allowed to proceed.
- **Commit-then-crash staging cleanup test**: a `convert` worker writes staging,
  writes the complete `lake/…/version=<n>/attempt=<n>/` prefix, commits the
  fenced `READY`/`SUCCEEDED` transition, and then crashes **before** deleting its
  `staging/…/attempt=<n>/` prefix. The test asserts that the sweeper — once the
  job is `SUCCEEDED` and
  `now() > max(write_credential_expires_at, write_credential_reserved_until) +
  CLEANUP_GRACE` — deletes the staging prefix (§4 rule 9), while the committed
  `lake/…/attempt=<n>/` prefix (the `object_prefix`) is never deleted (§4 rules 6
  and 7). A companion test asserts the sweeper never deletes the staging of a
  still-`RUNNING`/`BUILDING` attempt (serialization, rule 7), and that a
  successful attempt whose worker *did* delete its own staging is a no-op for the
  sweeper (idempotent delete).

### 6.2 Retry and version numbering

- A retry is a **new** `ingestion_job` with `retry_of` set to the failed job,
  targeting a **fresh** `dataset_version` with the next monotonic version number.
  The failed version stays `FAILED` (terminal) and is never exposed.
- Because only `READY` versions are visible to the query engine and the browser
  (§3.3), a retry after failure or crash yields at most one new `READY` version
  for the same logical input; the failed attempt never becomes visible, so retry
  cannot create duplicate visible versions.
- Retry is offered only for `FAILED` jobs marked `retryable` (worker crash,
  transient object-store error). A `CANCELLED` job is terminal and not retried;
  the operator submits a fresh ingestion instead.
- A retry that re-reads the source upload is subject to the release/retry
  mutual-exclusion protocol in §10: it may be created only while the source
  job's `upload_released_at` is NULL (serialized on the source job's row lock),
  and a retry requested after the upload was released must re-upload its input
  as a fresh `convert` rather than reference the deleted source.
- A retried job writes to a fresh `staging/…/attempt=<n>/` directory under a new
  job id, so it never collides with a crashed attempt's staging prefix; any stale
  staging prefix from a crashed attempt is reclaimed by the stale-attempt sweeper
  once the attempt's write credential has expired (§4 rules 6 and 8), never by
  the recovery pass and never while a still-credentialed writer could write to
  it. Because each job — and each attempt within a job — has its own
  `fencing_token` and its own attempt-scoped staging prefix, a stale worker from
  a prior attempt can neither publish objects under the new attempt's prefix nor
  transition the new job/version: its conditional commits match zero rows and
  are detected (§6.1).

## 7. SQL safety boundary (normative)

### 7.1 Allowed statements

Exactly one of the following, and nothing else:

- a single `SELECT` statement; or
- a single `SELECT` statement preceded by a `WITH` common-table-expression
  clause.

Rejected in all cases: `INSERT`, `UPDATE`, `DELETE`, `MERGE`, any DDL
(`CREATE`, `DROP`, `ALTER`), `COPY`, `EXPORT DATABASE`, `ATTACH`, `DETACH`,
`INSTALL`, `LOAD`, `FORCE INSTALL`, `PRAGMA`, `SET`, `RESET`, `CALL`,
transaction statements (`BEGIN`, `COMMIT`, `ROLLBACK`), `USE`, multiple
statements (including semicolon-separated), user-supplied `EXPLAIN`/
`EXPLAIN ANALYZE`, and any `VALUES`-top-level statement.

### 7.2 Allowed functions (exhaustive, deny-by-default)

The function surface is a **closed, compile-time allowlist**. The worker
resolves every function reference — after Unicode case-folding and alias
normalization — against this fixed list and rejects any name that is not
present. The list is a constant compiled into the worker; it is **not** derived
by enumerating DuckDB's runtime function catalog, so a later DuckDB release
that adds functions does not silently widen the surface. Everything not
enumerated below — unlisted aliases, user-defined functions, table macros, and
extension-provided functions — is denied with `query_denied`.

**Operators.** Comparison (`=`, `<>`/`!=`, `<`, `<=`, `>`, `>=`); logical
(`AND`, `OR`, `NOT`); arithmetic (`+`, `-`, `*`, `/`, integer division `//`,
`%`, `^`, unary `+`/`-`); string concatenation (`||`); `IS [NOT] NULL`;
`IS [NOT] DISTINCT FROM`; `[NOT] BETWEEN`; `[NOT] IN`; `[NOT] LIKE`;
`[NOT] ILIKE`; `[NOT] SIMILAR TO`; regex match `~`, `!~`, `~*`, `!~*`; and the
subscript `[]` operator for 1-based list/array indexing and map key lookup.
`CASE`/`WHEN`/`THEN`/`ELSE` is a language construct and is allowed.

**Casts and type predicates.** `CAST(expr AS type)`, the `::` operator,
`TRY_CAST`, and `typeof`.

**String.** `lower`, `upper`, `trim`, `ltrim`, `rtrim`, `length` (alias `len`),
`substring` (alias `substr`), `concat`, `concat_ws`, `replace`, `reverse`,
`split` (aliases `string_split`, `str_split`), `string_to_array`, `position`
(alias `strpos`), `left`, `right`, `lpad`, `rpad`, `repeat`, `starts_with`,
`ends_with`, `contains`, `prefix`, `suffix`, `regexp_matches`,
`regexp_replace`, `regexp_extract`, `regexp_full_match`.

**Numeric / math.** `abs`, `sign`, `round`, `floor`, `ceil` (alias `ceiling`),
`sqrt`, `pow` (alias `power`), `exp`, `ln`, `log`, `log2`, `log10`, `mod`,
`greatest`, `least`.

**Date / time.** `current_date`, `current_time`, `current_timestamp` (alias
`now`), `date_trunc` (alias `datetrunc`), `date_part` (alias `datepart`),
`date_diff` (alias `datediff`), `date_add` (alias `dateadd`), `date_sub` (alias
`datesub`), `extract`, `strftime`, `make_date`, `make_time`, `make_timestamp`,
`epoch`, `to_timestamp`, `year`, `month`, `day`, `hour`, `minute`, `second`,
`dayofmonth`, `dayofweek`, `dayofyear`, `week`, `quarter`. `extract` and
`date_part` accept only the fields `year, month, day, hour, minute, second,
millisecond, microsecond, dow, isodow, doy, week, quarter, epoch, decade,
century, millennium`.

**Null / conditional.** `coalesce`, `nullif`, `ifnull`, `if`, and `CASE`
(construct).

**Aggregates** (usable with `DISTINCT`, `ORDER BY`, and `FILTER (WHERE …)`
where DuckDB permits them). `count` (including `count(*)`), `sum`, `avg` (alias
`mean`), `min`, `max`, `approx_count_distinct`, `stddev` (alias `stddev_samp`),
`stddev_pop`, `variance` (alias `var_samp`), `var_pop`, `median`, `mode`,
`quantile_cont` (alias `quantile`), `quantile_disc`, `arg_min` (alias
`min_by`), `arg_max` (alias `max_by`), `string_agg` (alias `group_concat`),
`list` (alias `array_agg`), `first`, `last`, `any_value`, `bool_and`,
`bool_or`, `bit_and`, `bit_or`, `bit_xor`.

**Window functions** (only inside an `OVER (...)` clause). `row_number`,
`rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile`, `lag`, `lead`,
`first_value`, `last_value`, `nth_value`.

**Nested types (struct / list / map), read-only accessors only.**

- Struct: field dereference `s.field` (and chained `s.a.b`), and
  `struct_extract(s, 'field')`.
- List/array: subscript `l[1]` (1-based), `list_extract(l, idx)`, and
  `array_extract(l, idx)`; `length`/`len` for list size.
- Map: subscript `m['key']`, `map_extract(m, 'key')`, and
  `map_extract_value(m, 'key')`.
- Expansion: `unnest` (see table functions below).

No other `struct_*`, `list_*`, `array_*`, or `map_*` function is allowed;
`struct_pack` and other constructors are denied.

**Table functions.** Exactly one is allowed: `unnest`. All other table
functions are denied, including `range`, `generate_series`, and every reader
and meta function listed in the denial set below.

**Explicitly denied (also covered by deny-by-default, listed for clarity).**

- File/table readers and metadata: `read_csv`, `read_csv_auto`, `read_json`,
  `read_json_auto`, `read_ndjson`, `read_ndjson_auto`, `read_parquet`,
  `parquet_scan`, `parquet_metadata`, `parquet_schema`, `read_text`,
  `read_blob`, `glob`, `list_files`, `sniff_csv`, `sniff_parquet`, and any
  other `read_*` function.
- Network / URL: `http_get`, `http_post`, any `httpfs` function, and any
  function accepting a `url`, `path`, `filename`, or `secret` argument.
- Session / system / config / secret: `current_setting`, `set_config`,
  `current_schema`, `current_catalog`, `current_user`, `session_user`,
  `version`, `gen_random_uuid`, `uuid`, `random`, `random_normal`, and every
  `duckdb_*` introspection function.
- Extension / module management (also statement-denied): `INSTALL`, `LOAD`.

**Deny-by-default rule.** After alias normalization, any function name not
present in the lists above is rejected. This includes future DuckDB functions,
unlisted aliases, collations, and anything provided by a loaded extension (the
only loaded extension in v1 is the trusted, pinned `httpfs`, whose network
functions are denied above; no user-loadable extension is loaded). The allowlist is enforced at validation time
and again by the engine-level controls in §7.4.

### 7.3 Relation scope (what a query may reference)

- The only relations a query may reference are the generated catalog relations
  for `READY` dataset versions (one per version, exposed to user SQL as
  `catalog.<dataset>_v<n>` or an equivalent internal name). These are realized
  by server-side re-binding to the version's registered prefix (§7.4 layer 2),
  never by user-issued DDL.
- The parser resolves every table reference and rejects any reference to a raw
  `s3://` URL, `http(s)://` URL, `file://` path, local filesystem path, or any
  table not present in the generated catalog for that query.

### 7.4 Engine-level enforcement (defense in depth, layered)

The boundary is enforced by four enforcement layers, preceded by a
trusted-extension setup step (layer 0). No single layer is trusted; the
parser/validator is the primary SQL control and the per-query scoped credential
is the independently enforced object-store control. A keyword blacklist is never
the boundary.

> **DuckDB version pin — prerequisite, not yet satisfied.** This phase ships no
> `go.mod` pin, so nothing in this section is "validated against a pinned
> version"; the settings and Secrets-Manager behavior named below are the
> *intended* enforcement controls, not verified facts. They become enforceable
> only after two prerequisites are met: (1) phase 4 selects and pins an exact
> DuckDB version (its full `major.minor.patch` and the corresponding binding
> commit) in `go.mod` and the build, and (2) that exact version's availability
> and behavior for every named control — `access_mode`, `disabled_filesystems`,
> `autoload_known_extensions`, `autoinstall_known_extensions`,
> `allow_unsigned_extensions`, `lock_configuration`, `allowed_configs`,
> `memory_limit`, `threads`, `max_execution_time`,
> `enable_global_s3_configuration`, the Secrets Manager
> (`CREATE SECRET … SCOPE`, where `SCOPE` is a path prefix), and the trusted
> `httpfs` extension (its pinned binary and SHA-256 checksum, the explicit
> `LOAD` path, and the loaded-version verification via `duckdb_extensions()`) — is
> verified, with
> any discrepancy reconciled back into this document before implementation
> relies on it. Until then, the engine-level layer (layer 4) and the scoped
> secret selection (layer 3) are stated design targets, and the only boundary
> that may be described as *enforceable* today is what runs in worker code
> before the engine — the parser/validator allowlist and deny-by-default (layer 1)
> and server-side table binding (layer 2) — each to be re-verified against the
> pinned version. Phase 6 then pins each control with an adversarial test, so a
> later DuckDB upgrade that changes behavior fails the test rather than silently
> widening the surface.

0. **Trusted HTTPFS extension (the only loaded extension; pinned and loaded by
   trusted worker code before the connection is sealed).** Reading
   S3-compatible objects requires DuckDB's `httpfs` extension, which ships with
   the engine but is not enabled by default. The same extension also ships the
   `http_get`/`http_post` network functions (denied to user SQL in §7.2), and any
   extension load must be denied to user SQL. The worker therefore loads `httpfs`
   itself on a code path user SQL can never reach, and only afterwards seals the
   connection:

   - **Pinned, local binary.** The worker image bundles the `httpfs` extension
     binary for the exact pinned DuckDB version, and its SHA-256 checksum is
     verified at build/deploy time. The binary is staged into the connection's
     extension directory, which is read-only to the query process; it is loaded
     from local disk, never downloaded from the extension repository at runtime.
   - **Explicit, verifiable load.** Trusted worker code issues `LOAD httpfs` on
     the execution connection, then asserts the loaded extension version equals
     the pin (via the trusted `duckdb_extensions()` introspection call, which is
     itself denied to user SQL, §7.2). A missing or mismatched extension fails
     the query **closed** before any user statement runs.
   - **Then seal.** Only after the trusted load does the worker set
     `autoload_known_extensions=false`, `autoinstall_known_extensions=false`,
     and `allow_unsigned_extensions=false` and lock the configuration (layer 4).
     No user statement can trigger an extension load or install: `INSTALL`,
     `LOAD`, and `FORCE INSTALL` are statement-denied (§7.1), no known extension
     is auto-loaded or auto-installed, unsigned extensions are refused, and the
     extension directory is not writable by the query process.

   The trusted `LOAD` therefore executes on a different code path from user SQL:
   it is worker code run before the validator (layer 1) ever sees a user
   statement and before `lock_configuration` seals the connection, so it cannot
   be reached, imitated, or widened by the query text.

1. **Parser/AST validation (primary SQL boundary).** The user statement is
   parsed to an AST with a real parser. Before any engine work: the statement
   count is exactly one; the statement is a `SELECT` (optionally `WITH … SELECT`);
   every table reference resolves to a generated catalog relation for a `READY`
   dataset version (§7.3); every function call resolves to a name in the closed
   §7.2 allowlist (deny-by-default); and no path/URL literal is present. Because
   every file/URL reader and every scalar function that accepts a path, URL, or
   secret is absent from the allowlist, the user cannot *express* an arbitrary
   read. Rejection returns `query_denied` before DuckDB is involved.

2. **Server-side table binding (no user-controlled paths).** The worker resolves
   each allowed `catalog.<dataset>_v<n>` reference to the version's committed
   attempt prefix — `dataset_version.object_prefix` =
   `lake/<namespace>/<dataset>/version=<n>/attempt=<n>/` — stored in the catalog
   and binds it to a server-generated
   `read_parquet('s3://…/lake/…/version=<n>/attempt=<n>/**')` scan. Binding to
   the committed attempt prefix (not the whole `version=<n>/` subtree) means a
   stale or losing attempt's objects are never read, even before the sweeper
   removes them. The `**` here is a file glob that expands to every object under
   that prefix; it is unrelated to the secret `SCOPE`, which is a literal path
   prefix (layer 3). The prefix comes from the catalog row, never from the SQL
   text.
   There is no `CREATE VIEW` issued from user SQL at query time, so there is no
   read-only-versus-DDL ordering conflict; the trust boundary is this binding
   step, which runs entirely in worker code before the rewritten statement is
   handed to the engine.

3. **Per-query scoped object-store credential (independently enforced
   boundary).** For each query the worker obtains a short-lived, prefix-scoped
   credential — an S3/MinIO STS token whose policy allows `s3:GetObject` and
   `s3:ListBucket` on exactly the committed attempt prefix
   `lake/<namespace>/<dataset>/version=<n>/attempt=<n>/`
   (a real S3 prefix key, not a glob) — and registers it as a
   connection-scoped DuckDB secret whose `SCOPE` is that same clean prefix
   (`CREATE SECRET … IN connection … SCOPE
   's3://<bucket>/lake/…/version=<n>/attempt=<n>/'`). A secret scope is a path
   **prefix**, not a glob: the `SCOPE` therefore ends at `attempt=<n>/` (no `*`
   or `**` suffix), so the matching read URL
   `s3://<bucket>/lake/…/version=<n>/attempt=<n>/[<enc-col>=<enc-value>/…]part-<uuid>.parquet`
   always begins with it. DuckDB's Secrets Manager selects a secret only when its scope
   is the longest-prefix match for the path being read; every read outside the
   granted prefixes has **no matching secret** and — because no unscoped or
   ambient credential is available (see the fail-closed requirement below) —
   fails, and the object store's own STS policy denies it independently. This is
   the boundary that holds even if a parser bug let an unlisted reader through:
   the read still fails at the object store for any prefix not granted to that
   query.

4. **Engine configuration (defense in depth, not the read boundary).** The
   execution connection is locked down before the user statement runs:

   - `access_mode=READ_ONLY` — the connection cannot perform DDL/DML or writes.
     This complements, not replaces, parser rejection, and is applied only on the
     execution connection after the scoped secret is registered.
   - `disabled_filesystems='LocalFileSystem'` — local filesystem I/O is disabled;
     only the S3-compatible httpfs filesystem remains for the registered reads.
   - `autoload_known_extensions=false`, `autoinstall_known_extensions=false`,
     `allow_unsigned_extensions=false` — no *user- or auto-initiated* extension
     load or install: user SQL cannot `INSTALL`/`LOAD`/`FORCE INSTALL` (§7.1), no
     known extension is auto-loaded or auto-installed, and unsigned extensions
     are refused. The sole loaded extension is the trusted, pinned `httpfs`,
     loaded by worker code **before** these flags are set and the configuration
     is locked (layer 0); it is never reachable from user SQL.
   - `enable_global_s3_configuration=false` — DuckDB never fetches ambient AWS
     credentials from the process environment, so an out-of-prefix read cannot
     fall back to a process-wide credential (see the fail-closed requirement
     below).
   - `lock_configuration=true` with `allowed_configs` limited to the per-query
     budget settings (`threads`, `memory_limit`) — user SQL cannot `SET`/`PRAGMA`
     its way out; `SET` and `PRAGMA` are also statement-denied (§7.1).

   `enable_external_access` is **not** the read boundary and is deliberately not
   used to distinguish worker SQL from user SQL: setting it to `false` would also
   disable the `read_parquet` scans the query legitimately needs, and
   `allowed_directories`/`allowed_paths` govern *local* paths, not `s3://`
   prefixes. The worker/user distinction is structural — user SQL is parsed,
   validated, and re-bound (layers 1–2), then executed under a scoped credential
   (layer 3); the engine settings above only narrow what an already bound
   statement can do.

**Fail-closed: no ambient or unscoped credential fallback.** The boundary in
layer 3 only holds if the query execution connection has **no other S3
credential** that DuckDB could fall back to. DuckDB's default behavior
(`enable_global_s3_configuration=true`) auto-loads ambient AWS credentials from
the process environment — the deprecated global `s3_access_key_id`,
`s3_secret_access_key`, and `s3_session_token` settings, the environment
variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, and
`AWS_PROFILE`, and `~/.aws/credentials`. With any of these present, an
out-of-prefix read that matches no scoped secret can still succeed under the
ambient credential, silently defeating the boundary. Therefore the worker
**must** harden the query execution path so that no ambient or unscoped
credential exists:

- `enable_global_s3_configuration=false` on the execution connection, so DuckDB
  never fetches ambient AWS credentials from the environment.
- Only per-query, prefix-scoped `config`-provider secrets are registered, using
  the explicit `KEY_ID`/`SECRET`/`SESSION_TOKEN` from the STS response. The
  `credential_chain` provider and the deprecated global `s3_*` settings are
  never used for query credentials, so the AWS SDK credential chain and legacy
  settings are not consulted.
- No unscoped secret is registered on the execution connection, and
  persistent-secret loading is disabled for the query path (an empty/isolated
  secret directory, or `allow_persistent_secrets=false`), so an operator's
  stored unscoped secret cannot become an implicit fallback.

The worker asserts these preconditions at startup and immediately before each
query and **fails closed** if an ambient credential or unscoped secret is
detected in the query execution path. Under these preconditions a read outside
the granted prefixes has **no applicable credential at all** and fails with a
credential/authorization error — never with data — regardless of what the
deployment injects into the worker's environment.

### 7.5 Testability

The boundary is tested by both positive and adversarial cases (recorded in
`plan/06-security-and-observability.md`):

- **Registered queries succeed**: a validated `SELECT` over
  `catalog.<dataset>_v<n>` returns rows for every `READY` version; joins and
  aggregates across two registered versions of the same or different datasets
  succeed; `unnest` over a list/struct column succeeds. The test asserts that the
  per-query secret's `SCOPE` is the clean `lake/…/version=<n>/attempt=<n>/`
  prefix, so the
  granted secret is actually selected (longest-prefix match) for every legitimate
  read.
- **Out-of-prefix denial must fail**: DDL/DML, `COPY`, `ATTACH`, `INSTALL`/
  `LOAD`, `EXPORT`, `PRAGMA`, `SET`, `CALL`, multiple statements, local paths
  (`read_csv('/etc/…')`), arbitrary S3 URLs
  (`read_parquet('s3://bucket/uploads/…')`, an unregistered prefix, a different
  dataset's prefix), `http_get`, extension loading, unlisted functions (e.g.,
  `read_text`, `current_setting`, `gen_random_uuid`), and oversized result
  requests. Each must be denied with `query_denied` (or the mapped engine/limit
  error) and must read no data outside the registered prefixes.
- **Scoped-credential boundary holds independently**: a test harness that
  bypasses layers 1–2 (invoking `read_parquet` directly against the engine to
  simulate a parser bug) must still fail to read (a) another dataset's prefix,
  (b) `uploads/`, `staging/`, or another query's `exports/` prefix, and (c) a
  sentinel object placed outside every granted prefix — because no matching
  secret scope exists, no ambient/unscoped credential fallback is configured
  (see the fail-closed requirement in §7.4), and the STS policy denies the read.
  The same sentinel must never be observed by any passing query.
- **Ambient-credential bypass test (fail closed)**: the scoped-credential test
  above runs with the worker's **normal deployment environment** — including any
  ambient AWS credential variables (`AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_PROFILE`) and credential
  files that the Compose/deployment ordinarily injects for ingestion — and the
  out-of-prefix read must still fail with a credential/authorization error,
  never data. This proves the boundary does not depend on the worker process
  happening to be clean: even with ambient credentials present,
  `enable_global_s3_configuration` is `false` and no unscoped secret is
  registered, so the read has no applicable credential.

- **HTTPFS extension pin test**: the worker's connection setup loads `httpfs`
  from the pinned local binary and asserts the loaded version equals the pin,
  failing closed on a missing or mismatched binary; with the connection sealed
  (`autoload_known_extensions=false`, `autoinstall_known_extensions=false`,
  `allow_unsigned_extensions=false`, `lock_configuration=true`), user SQL
  attempting `INSTALL`, `LOAD`, `FORCE INSTALL`, or any known-extension
  reference is denied with `query_denied` and no extension is loaded or
  installed; and a registered S3 dataset read succeeds only through the trusted
  pinned `httpfs` load — never through autoload/autoinstall. A companion test
  removes the bundled extension binary and asserts the worker fails closed
  before executing any query.

These behaviors are pinned in phase 6 with adversarial tests, including a
sentinel object placed outside every registered prefix that a passing test must
never observe.

## 8. Object access and credentials

- Credentials live only in the API/worker configuration (environment or secret
  store), never in catalog rows, logs, metrics, traces, browser bundles, or API
  responses. They are injected server-side and never appear in SQL text.
- Credential classes, by function:

  - **Ingest** (trusted worker code, operator-driven): split into a *read* and a
    *write* capability so that stale-attempt cleanup is safe (§4 rule 8).
    The **read** side may use a least-privilege, deployment-scoped credential to
    read the source prefixes it is told to read (`uploads/` and/or an existing
    Parquet prefix); ingestion input is the operator's, not untrusted SQL, so
    scoping the *read* per job is optional hardening, not required. The **write**
    side (`staging/` and `lake/`) is **always** an attempt-scoped, short-lived
    STS credential whose policy allows write to exactly this attempt's
    `staging/<namespace>/<dataset>/<job-id>/attempt=<n>/` and
    `lake/<namespace>/<dataset>/version=<n>/attempt=<n>/` prefixes and whose TTL
    is the full lease window plus a slack margin at issuance time, so it expires
    no earlier than the attempt's current `lease_expires_at` — the credential
    covers the entire lease window (the actual expiry is recorded as
    `write_credential_expires_at`, §3.1). There is **no** long-lived,
    deployment-wide `lake/` or `staging/` write credential on the ingestion path;
    this is what lets the sweeper guarantee "no writes after delete" (§4 rule 8,
    §6.1) — a fenced/delayed worker loses the ability to write the moment its
    attempt credential expires or is revoked.
  - **Query (read)** (per query, short-lived, prefix-scoped): before execution
    the worker obtains an STS credential whose policy allows read of **exactly
    the committed attempt prefixes
    `lake/<namespace>/<dataset>/version=<n>/attempt=<n>/` referenced by that
    query** (S3 prefix, not a glob) and registers it as a connection-scoped
    DuckDB secret whose `SCOPE` is that same clean prefix, ending at
    `attempt=<n>/` (§7.4 layer 3). The query connection has no unscoped secret
    and `enable_global_s3_configuration=false` (see the fail-closed requirement
    in §7.4), so there is no ambient fallback. There is no bucket-wide `lake/`
    read credential for query execution: a query can only present credentials
    for the prefixes it was granted, so even a DuckDB-control bypass cannot read
    a different dataset's prefix, `uploads/`, `staging/`, or another query's
    exports.
  - **Export/spool (write)** (trusted worker code): the worker writes the
    result-set spool on a separate connection using a write-scoped credential to
    only the `exports/<namespace>/<query-id>/attempt=<n>/` prefix (again a clean
    prefix, not a glob); this is trusted code, not user SQL (§9).
  - **Result serving (read)** (trusted API code): paging and CSV export are
    served by the API, which is the **only** component that reads the spool. For
    each result/export request the API obtains a short-lived, prefix-scoped
    credential whose policy allows `s3:GetObject` (and `s3:ListBucket` only where
    enumeration is required) on exactly the `exports/<namespace>/<query-id>/`
    prefix of that one query — no other prefix, no `lake/`, `uploads/`,
    `staging/`, or another query's `exports/`, and no write. CSV export is
    streamed from the spool to the client; no separate export object is written
    (§9).

- The dashboard/API never returns bucket credentials, secret keys, or raw
  object keys. Preview/schema and result-serving credentials are server-side only
  and never reach the browser. Preview/schema results are bounded (§9).

## 9. Result paging, export, and retention

- Query result payloads are never stored as rows in PostgreSQL.
- On completion, the worker (trusted code, on a separate write-scoped
  connection) spools results to
  `exports/<namespace>/<query-id>/attempt=<n>/result.parquet` and summarizes the
  `query_job` row (row count, byte count, result reference pointing at that
  attempt-scoped key). Small and large results use the same path for consistency.
  The `attempt=<n>` segment is taken from the job's current claim (§13), so a
  stale attempt never overwrites a live attempt's spool.
- **Reader**: the API is the sole reader of the spool. Paging
  (`GET …/queries/{id}/results?offset=&limit=`) and CSV export
  (`GET …/queries/{id}/export`) both read
  `exports/<namespace>/<query-id>/attempt=<n>/result.parquet` using a
  short-lived, prefix-scoped **read** credential that permits `s3:GetObject`
  (and `s3:ListBucket` where enumeration is needed) on exactly
  `exports/<namespace>/<query-id>/` — no other prefix and no write (§8). The
  read credential is requested per query, scoped to that query's export prefix,
  and never broadens to `lake/`, `uploads/`, `staging/`, or another query's
  `exports/`.
- Paging returns a bounded page read from the spool with the scoped read
  credential, bounded by **both** row count and bytes (§9 byte bounds below):
  default `limit` 1,000 rows (hard max 10,000) and `MAX_PAGE_BYTES`
  (default 4 MiB, hard max 64 MiB), always stopping at a whole-row boundary.
- Row bound: a query returns at most `MAX_RESULT_ROWS` (default 10,000, hard
  max 100,000) rows to the client. To make `truncated` exact, the engine
  materializes at most `MAX_RESULT_ROWS + 1` rows: if a
  (`MAX_RESULT_ROWS + 1`)-th row is produced, it is discarded and
  `truncated:true` is recorded; otherwise `truncated:false`. The probe row is
  never returned, never written to the spool, and never counted in
  `rows_returned`; execution stops as soon as the (`MAX_RESULT_ROWS + 1`)-th row
  is produced, so a result with exactly `MAX_RESULT_ROWS` rows is
  distinguishable from one with more rows. `rows_returned` is the count actually
  returned (at most `MAX_RESULT_ROWS`) and `truncated` is an independent boolean,
  so the two are never conflated.
- **Byte bounds (materialization, paging, export).** Row-count limits alone do
  not bound payload or spool size: a permitted row may hold an arbitrarily large
  scalar, list, struct, or map value, so one page or export could still produce
  an unbounded payload and spool. Every result path is therefore bounded in
  bytes as well as rows, and every bound is enforced on the **serialized** size
  of what is actually written or streamed — never on an estimate from column
  types. The limits are `MAX_RESULT_BYTES` (total encoded size of the spooled
  `result.parquet`; default 256 MiB, hard max 1 GiB), `MAX_ROW_BYTES` (encoded
  size of a single materialized row; default 1 MiB, hard max 16 MiB),
  `MAX_PAGE_BYTES` (serialized size of one paged response's row array; default
  4 MiB, hard max 64 MiB), and `MAX_EXPORT_BYTES` (CSV payload bytes of one
  export response; default 64 MiB, hard max 512 MiB). The invariant
  `MAX_ROW_BYTES ≤ MIN(MAX_PAGE_BYTES, MAX_EXPORT_BYTES, MAX_RESULT_BYTES)` is
  validated at startup (a violating configuration is rejected) and guarantees a
  non-empty result always yields at least one whole row on every path: no page
  or export is ever empty, and no row is ever split across a page or cut
  mid-cell.

  - **Materialization is byte-bounded and streamed.** The worker streams rows
    from the engine in bounded chunks and counts both rows and encoded bytes as
    it writes `result.parquet`. A row's encoded size is the size of its
    contribution to the spool — the serialized columnar bytes the worker's own
    encoder produces for that row, including the full contents of any nested
    list/struct/map cell — not a type-width estimate. The same per-row size is
    measured conservatively so that it upper-bounds the row's JSON and CSV wire
    encodings as well (worst-case quoting/escaping), which is what lets the
    invariant below guarantee every page and export can emit a whole row.
    Writing stops at the first
    violated bound: more than `MAX_RESULT_ROWS` rows, or a row that would push
    the spool past `MAX_RESULT_BYTES`, truncates the result to the largest
    whole-row prefix that fits and records `truncated=true`; the crossing row is
    the probe row and is never written, returned, or counted. The spool
    therefore never exceeds `MAX_RESULT_BYTES`, and the worker's in-memory
    footprint during materialization is bounded by the chunk size, not the full
    result.
  - **Oversized row (single row too large).** A row whose encoded size exceeds
    `MAX_ROW_BYTES` cannot be returned within any page, export, or result bound;
    it is a property of the data, not of the request. The worker terminates the
    query and records `FAILED` with `failure_reason=resource_exceeded` (surfaced
    as `resource_exhausted` per §11); no spool is published and `result_ref` is
    not set, and the oversized row is never written to the spool. This is the
    only case in which a byte bound fails the query instead of truncating: an
    *aggregate* result that is too large is truncated to the largest fitting
    whole-row prefix, while a result containing a single oversized row is
    refused. Because `MAX_ROW_BYTES ≤ MAX_RESULT_BYTES`, every non-oversized row
    fits the spool, so byte truncation can never make an otherwise-valid result
    empty.
  - **Paging is bounded in bytes and rows.** A page returns whole rows from the
    spool until either `limit` rows or `MAX_PAGE_BYTES` (the serialized size of
    the page's row array as the API encodes it) is reached. When the byte bound
    stops a page before `limit` rows, the pagination cursor advances by the
    number of whole rows actually emitted, so no row is split across pages and a
    page is never empty while rows remain.
  - **CSV export is bounded in bytes and rows.** Export streams whole rows only,
    bounded by both the export row limit and `MAX_EXPORT_BYTES` measured on the
    CSV text after quoting/escaping; the API stops before emitting a row whose
    CSV-encoded size would exceed the remaining byte budget, so the streamed
    payload never exceeds the bound and no CSV row is truncated mid-cell.
- CSV export streams a bounded CSV (default max 50,000 rows, capped by the
  result bound, and `MAX_EXPORT_BYTES` whole-row bytes) directly from the spool
  to the client; no separate `export.csv` object is written, so no export-write
  credential is needed.
- TTL: `exports/` objects expire after `EXPORT_TTL` (default 24h) via an
  object-store lifecycle rule plus a background sweeper; a query whose spool
  has expired returns a clear `result_expired` error rather than stale rows.
- **Testability**: a test asserts that the result-read credential's prefix scope
  is exactly the query's `exports/<namespace>/<query-id>/` prefix and that reads
  of another query's `exports/` prefix, or of `lake/`, `uploads/`, or `staging/`,
  fail with an authorization error and read no data. The same test asserts the
  API's result/export endpoints never receive a credential broader than that
  query's export prefix.
- **Byte-limit tests.** (1) *Oversized row*: a dataset containing one row whose
  encoded size exceeds `MAX_ROW_BYTES` (a generated `VARCHAR`/`BLOB` of
  `MAX_ROW_BYTES + 1` bytes) makes the query `FAILED` with
  `failure_reason=resource_exceeded` (surfaced as `resource_exhausted`), no
  `result_ref`, and no spool object — while the same query against rows each
  below the limit succeeds, proving the failure is per-row, not per-query. (2)
  *Total-bytes truncation*: many rows each below `MAX_ROW_BYTES` whose aggregate
  encoded size exceeds `MAX_RESULT_BYTES` yield `truncated=true`, a
  `rows_returned` equal to the largest whole-row prefix that fits, and a spool
  object whose size is ≤ `MAX_RESULT_BYTES`, with no row split. (3) *Page byte
  bound*: with `limit` large enough that `MAX_PAGE_BYTES` binds first, a page's
  serialized row array is ≤ `MAX_PAGE_BYTES`, the cursor advances by whole rows,
  no row is split across pages, and a page is never empty while rows remain.
  (4) *Export byte bound*: CSV export of rows whose quoted/escaped text exceeds
  `MAX_EXPORT_BYTES` stops at a whole-row boundary, the streamed payload is ≤
  `MAX_EXPORT_BYTES`, at least one row is exported, and the final emitted row is
  complete, valid CSV. (5) *Invariant*: startup rejects any configuration with
  `MAX_ROW_BYTES > MIN(MAX_PAGE_BYTES, MAX_EXPORT_BYTES, MAX_RESULT_BYTES)`.

## 10. Data retention policy

| Data | Retention |
|---|---|
| `lake/` Parquet objects | Retained for the life of the deployment. Archive does **not** delete them (unarchive is reversible); no automatic deletion in v1. Hard delete/GC is out of scope. |
| Original uploads | `retain_original=true`: retained for the life of the deployment (no auto-delete). `retain_original=false`: deleted promptly after the version reaches `READY`; on a terminal `FAILED`/`CANCELLED`/crash-recovered job, retained for `UPLOAD_RETRY_WINDOW` (default 7 days, max 90 days) so a retry can reuse the input, then deleted |
| `staging/` objects | Reclaimed by the stale-attempt sweeper once the job is terminal (`SUCCEEDED`, `FAILED`, or `CANCELLED`) and after `max(write_credential_expires_at, write_credential_reserved_until) + CLEANUP_GRACE` (§4 rules 6, 8, and 9) — never at the transition itself. A successful attempt's staging is best-effort deleted by the worker after the `READY` commit, but the sweeper is the authoritative backstop, so a worker that crashes after the commit strands nothing |
| `exports/` (results/exports) | TTL default 24h, swept |
| query_job history rows | Bounded retention (default 30 days, configurable); the SQL statement is retained for the job's full lifetime and is never purged or overwritten before the job reaches a terminal state; after that the statement and its `statement_hash` are removed together with the row under the same retention window. There is no separate hash store, so no hash outlives its row |
| Failed version error summaries | Retained with the version for its lifetime (archive does not delete version rows) |

**Original-upload lifecycle (complete, including terminal jobs).** An upload
object under `uploads/<namespace>/<dataset>/<job-id>/` is the durable input for
its ingestion job and for any retry of it (via `retry_of`). Its lifetime is
fully determined by the job's outcome and the dataset `retain_original` policy,
so no upload is ever left indefinitely:

- `retain_original=true` — the upload is retained for the life of the
  deployment (the same rule as `lake/` objects), regardless of job outcome, and
  is never auto-deleted in v1.
- `retain_original=false` — the upload is deleted as soon as it is provably no
  longer needed:
  - **Success**: once the version reaches `READY` the upload is deleted (§4 rule
    4); the converted Parquet is the durable artifact.
  - **Terminal failure / cancel / crash-recovery**: the upload is retained for
    `UPLOAD_RETRY_WINDOW` (default 7 days, configurable, hard max 90 days)
    measured from the job's terminal transition (`finished_at`), so a retry can
    re-stage the same input; after the window it is deleted even if no retry was
    ever submitted. The window is a bound on *retention*, not a promise of
    indefinite input availability.
- **Cleanup is serialized against retry creation (release ↔ retry mutual
  exclusion).** Upload release and retry are made mutually exclusive by a shared
  catalog lock and a durable release flag, so (a) a concurrent retry can never
  have its source upload deleted while it is reading or re-staging it, and (b) no
  retry can be created after the upload is released. Both operations serialize on
  the **row lock of the job that owns the upload** — the `ingestion_job` whose id
  appears in the `uploads/<namespace>/<dataset>/<job-id>/` key:

  - **Durable state.** `upload_released_at` (timestamptz, NULL until released) is
    the single durable flag that the owning job's upload prefix has been declared
    due and handed to the lifecycle rule; it is written only by the release
    transaction below, and once set it is a barrier no dependent retry may cross.
    `source_upload_consumed_at` (timestamptz, NULL until consumed) is set by a
    retry the moment it has finished re-staging the source upload into its own
    staging prefix, after which it no longer reads the source.
  - **Release (one transaction, commit-last).** The upload sweeper runs, in a
    single transaction: `SELECT … FOR UPDATE` on the owning job row; re-read the
    job and verify the release precondition — the job is terminal
    (`SUCCEEDED`/`FAILED`/`CANCELLED`), the dataset's `retain_original` is
    `false`, and either the version reached `READY` (success) or
    `now() > finished_at + UPLOAD_RETRY_WINDOW` (non-success) — **and** that no
    dependent retry is still relying on the source: there is no row with
    `retry_of` = this job whose `status IN ('QUEUED','RUNNING')` and
    `source_upload_consumed_at IS NULL`. If all hold, set
    `upload_released_at = now()`; commit. If any condition fails, nothing is
    committed and the upload is not released.
  - **Retry creation (one transaction, commit-last).** Creating a retry that
    re-reads the source upload runs, in a single transaction: `SELECT … FOR
    UPDATE` on the owning job row; verify the source job is `FAILED` and
    `retryable`, and that `upload_released_at IS NULL`. If `upload_released_at`
    is already set, the upload has been released, so the retry **cannot
    reference it**: the request is refused (the operator must re-upload the input
    as a fresh `convert`). Otherwise the new `ingestion_job` is inserted with
    `retry_of` = the owning job in the same transaction; commit.
  - **Why this is race-free.** Because both transactions `SELECT … FOR UPDATE`
    the same owning job row, they are totally ordered. If retry creation commits
    first, its new dependent row is visible to release's under-lock check, which
    then skips the upload. If release commits first, `upload_released_at` is
    visible to retry creation's under-lock check, which then refuses to reference
    the source. There is no interleaving in which a retry references a released
    upload, and none in which release deletes an upload a retry still depends on.
  - **External action (after commit, idempotent).** Only after the release
    transaction commits does the sweeper perform the object-store action: it
    re-tags the upload objects `lakehouse-upload-retain=release` and records
    `lakehouse-upload-release-at=<finished_at + UPLOAD_RETRY_WINDOW>` (arming the
    tag-filtered lifecycle rule below), and may also delete the prefix directly
    (idempotent). Because `upload_released_at` is durable, a sweeper that crashes
    after the commit but before the re-tag re-derives the released prefix on the
    next pass and completes the re-tag; no retry can be created in the interim
    because the flag is already set. The sweep is idempotent and re-derives the
    due set from the catalog each pass, so a crashed sweeper re-runs safely.
- **No blanket age-based lifecycle rule on `uploads/`.** The upload retention
  decision depends on catalog state an age rule cannot observe — the job's
  terminal status, the dataset's `retain_original`, a retry window measured from
  `finished_at`, and whether an in-flight retry has not yet re-staged the input.
  A prefix-wide age rule operates independently of the catalog sweeper and would
  therefore delete `retain_original=true` uploads (which are promised to live for
  the deployment), uploads of still-`QUEUED`/`RUNNING` jobs, or a long-running
  job's input before its retry window begins. The object store therefore applies
  **no** unqualified age expiry to `uploads/`; upload deletion is keyed to the
  catalog decision alone, and the object-store lifecycle rule is **tag-filtered**
  so it can only act on uploads the catalog has already declared due:
  1. Every upload is written with the retention-class tag
     `lakehouse-upload-retain=active`. The lifecycle rule expires **only**
     objects tagged `lakehouse-upload-retain=release` (after a short grace,
     default `CLEANUP_GRACE`). An object tagged `active` never matches the rule,
     so a retained (`retain_original=true`), `QUEUED`/`RUNNING`, or
     still-within-window upload can never be expired no matter how long it has
     existed.
  2. Only after the release transaction has set `upload_released_at` does the
     sweeper re-tag the upload objects `lakehouse-upload-retain=release` and
     record `lakehouse-upload-release-at=<finished_at + UPLOAD_RETRY_WINDOW>`.
     The re-tag is the only event that arms the lifecycle rule, so deletion
     remains driven by the catalog decision and the retry window is measured from
     `finished_at` — accounting for arbitrary job duration rather than object age.
     The release flag (not the tag) is what blocks retries; the tag only arms the
     object-store rule. The sweeper may also delete the prefix itself directly
     (idempotent); the tag-filtered lifecycle rule is the backstop that completes
     the delete after the grace even if the sweeper dies mid-delete.
  3. Uploads are written once at upload time and are never rewritten or appended
     to by ingestion or retry, so no write-quiescence wait applies to upload
     deletion (unlike `lake/`/`staging/`, §4 rules 6–8); the retry re-reads the
     original rather than re-writing it.
- **Retry and re-staging.** A retry (`retry_of` = the owning job) re-reads the
  original upload under the owning job's `uploads/…/<job-id>/` prefix and
  re-stages it into its own `staging/…/attempt=<n>/`; the moment the re-stage
  completes it sets `source_upload_consumed_at`, so from that point the retry no
  longer depends on the original upload and the original can be released/deleted
  at the window's close without affecting the in-flight retry (the release
  transaction's under-lock check observes `source_upload_consumed_at` and no
  longer treats the retry as dependent). A retry created after the upload was
  released (`upload_released_at` set) cannot re-read the source and must
  re-upload its input as a fresh `convert` (§6.2). The exclusion holds regardless
  of submission order: a retry submitted before release proceeds under the shared
  lock and delays release until it re-stages; a retry submitted after release is
  refused.
- **Testability**: a `convert` job whose version reaches `READY` has its upload
  deleted when `retain_original=false` and retained when `retain_original=true`;
  a failed/cancelled/crash-recovered job retains its upload until
  `UPLOAD_RETRY_WINDOW` elapses, after which the sweeper deletes it, while the
  same job with `retain_original=true` is never deleted. The sweeper never
  deletes the upload of a `QUEUED`/`RUNNING` job or of a job whose retry has not
  yet re-staged the input. A companion test asserts the tag-filtered lifecycle
  rule never expires an upload still tagged `lakehouse-upload-retain=active`
  (retained, `QUEUED`/`RUNNING`, or within the retry window) even after its age
  exceeds any configured duration, and only expires an upload after it is
  re-tagged `lakehouse-upload-retain=release` by the sweeper.
- **Release/retry race test**: two transactions race on the owning job's row
  lock — one performs the release (verify due and no dependent retry, then set
  `upload_released_at`) and one creates a retry (verify `upload_released_at IS
  NULL`, then insert a `retry_of` row). The test drives both orderings and
  asserts the invariant that release and a dependent retry are never both
  committed: (1) release-then-retry — retry creation is refused (or requires a
  fresh upload) because `upload_released_at` is set, and the source upload is
  never re-read by a retry; (2) retry-then-release — release observes the
  un-consumed dependent retry under the lock, does **not** set
  `upload_released_at`, and does not re-tag or delete the upload until the retry
  sets `source_upload_consumed_at`, after which the next release pass proceeds.
  A concurrency test runs the two transactions in parallel many times and asserts
  no interleaving produces both a released upload and a dependent retry, and that
  the object-store re-tag/delete never removes an upload a committed retry is
  still reading.

## 11. API error contract

All non-2xx responses use one JSON envelope:

```json
{
  "error": {
    "code": "invalid_argument",
    "message": "human-readable, safe, bounded",
    "details": { }
  }
}
```

Codes (enum) and status mapping:

| code | HTTP |
|---|---|
| invalid_argument | 400 |
| not_found | 404 |
| already_exists | 409 |
| conflict | 409 |
| invalid_state | 409 |
| query_denied | 400 |
| query_cancelled | 409 |
| query_timeout | 504 |
| result_expired | 410 |
| resource_exhausted | 429 |
| rate_limited | 429 |
| unauthorized | 401 |
| forbidden | 403 |
| unavailable | 503 |
| internal | 500 |

Safety rules for errors: never echo SQL text, object keys, credentials, or
filesystem paths; messages are length-bounded; field-level `details` is typed
and sanitized.

`unauthorized` (401) and `forbidden` (403) have precise meanings under the
trust model in §15: `unauthorized` means a missing or invalid deployment token
when token authentication is enabled; `forbidden` means the request was refused
by the mandatory browser-request defense (Host/Origin/fetch-site checks or CSRF
protection, §15.2) or by a deployment policy gate (there is no per-user
authorization in v1).

## 12. Timeouts, limits, and concurrency

Defaults are configuration values (overridable); tests assert the bounds, and
only pin a default where a test explicitly chooses to.

| Control | Default | Bound |
|---|---|---|
| Query execution timeout | 60s | max 300s |
| Ingestion lease duration (`LEASE_DURATION`) | 15m | max 30m |
| Query lease duration (`QUERY_LEASE_DURATION`) | 120s | max 600s |
| Ingestion lease heartbeat interval | `LEASE_DURATION / 3` | `< LEASE_DURATION − LEASE_RENEWAL_MARGIN` |
| Query lease heartbeat interval | `QUERY_LEASE_DURATION / 3` | `< QUERY_LEASE_DURATION − LEASE_RENEWAL_MARGIN` |
| Lease renewal margin (`LEASE_RENEWAL_MARGIN`) | 30s | configurable |
| Write-credential slack margin (`CREDENTIAL_SLACK`) | 30s | configurable, covers STS latency + clock skew |
| Min write-credential TTL (`MIN_WRITE_CREDENTIAL_TTL`) | 30s | configurable |
| Cleanup grace (`CLEANUP_GRACE`) | 15m | configurable |
| Upload retention window (`UPLOAD_RETRY_WINDOW`) | 7 days | max 90 days |
| Worker concurrency (queries) | 2 | configurable, ≥1, hard max 16 |
| Worker concurrency (ingestion) | 1 | configurable, ≥1, hard max 8 |
| DuckDB threads | min(4, cpus) | configurable |
| DuckDB memory | 4 GiB | configurable |
| SQL text length | 64 KiB | hard cap |
| Statements per query | 1 | exactly 1 |
| Result rows | 10,000 | hard max 100,000 |
| Result bytes (`MAX_RESULT_BYTES`) | 256 MiB | hard max 1 GiB |
| Row bytes (`MAX_ROW_BYTES`) | 1 MiB | hard max 16 MiB |
| Page size | 1,000 | hard max 10,000 |
| Page bytes (`MAX_PAGE_BYTES`) | 4 MiB | hard max 64 MiB |
| Export rows | 50,000 | capped by result bound |
| Export bytes (`MAX_EXPORT_BYTES`) | 64 MiB | hard max 512 MiB |
| Queue depth | 100 | bounded, rejects when full |

The query timeout is enforced at two levels: the worker cancels via a Go context
deadline, and the engine enforces the same budget via DuckDB's
`max_execution_time` (milliseconds). Cancellation is observable and persisted in
the job row (§13).

**Worker concurrency is bounded per job class and shares one process budget.** A
single Go worker process executes both ingestion and query jobs under two
independent bounded pools, so one job class cannot starve the other:
`INGESTION_WORKERS` (default 1; configurable, ≥1, hard max 8) bounds the number
of concurrently `RUNNING` ingestion jobs, and `QUERY_WORKERS` (default 2;
configurable, ≥1, hard max 16) bounds the number of concurrently `RUNNING` query
jobs. A job is claimed (`QUEUED → RUNNING`) only when its class's pool has a free
slot; otherwise it remains `QUEUED` and is retried on a later claim pass, so
neither class can ever run more jobs than configured. The two pools share the
single worker process and therefore its CPU and memory budget: the DuckDB thread
and memory limits (§12 table) are applied per query job and ingestion conversion
bounds its own per-job memory, so peak resource use is bounded by
`(INGESTION_WORKERS × ingestion per-job budget) + (QUERY_WORKERS × query per-job
budget)` within the single node (§14). Startup validation rejects any concurrency
setting outside its class's bounds (including `0`) and any configuration whose
worst-case per-job resource product exceeds the node's configured budget. The
queue-depth limit (§12 table) applies to the combined queue (the sum of `QUEUED`
ingestion and query jobs), so the worker never accumulates an unbounded backlog.

**Testability (concurrency).** With `INGESTION_WORKERS=1` and `QUERY_WORKERS=1`, a
test submits `N` ingestion jobs and `M` query jobs and asserts that at most one
ingestion job and one query job is ever `RUNNING` concurrently, that excess jobs
stay `QUEUED` and are claimed as slots free, and that a concurrency setting of `0`
or above a class's hard max is rejected at startup. A companion test asserts that
a long ingestion job does not delay a query job's claim (separate pools) and that
the combined queue-depth limit rejects new submissions once full.

### 12.1 Leases and heartbeat (liveness, not a time budget)

Every job claim (ingestion §6.1, query §13) carries a lease — `lease_owner` and
`lease_expires_at = claim_time + LEASE_DURATION` (ingestion) or
`claim_time + QUERY_LEASE_DURATION` (query). A lease is a **liveness and
ownership** mechanism, not the job's compute budget. It answers one question —
"is the claiming worker still alive and in charge?" — so a dead or partitioned
worker's job can be reclaimed (ingestion fails retryably, query requeues). The
job's actual runtime budget is a separate control: the query execution timeout
(§12 table) for queries, and for ingestion there is no fixed runtime budget —
liveness is carried entirely by the lease.

- **Durations.** `LEASE_DURATION` (ingestion) defaults to 15 minutes, hard max
  30 minutes. `QUERY_LEASE_DURATION` (query) defaults to 120 seconds, hard max
  600 seconds. Both are configurable and validated against their bounds at
  startup; a test asserts the bounds are enforced.
- **Heartbeat / renewal.** Each job type has its own heartbeat interval and
  extends with **its own** lease duration. While a query job is `RUNNING`, its
  holder renews every `QUERY_HEARTBEAT_INTERVAL` (default
  `QUERY_LEASE_DURATION / 3`) with
  `UPDATE … SET lease_expires_at = now() + QUERY_LEASE_DURATION WHERE id=? AND
  fencing_token=? AND status='RUNNING'`, after validating
  `QUERY_HEARTBEAT_INTERVAL + LEASE_RENEWAL_MARGIN < QUERY_LEASE_DURATION`
  (default: 40s + 30s < 120s). A query therefore renews well before its
  120-second lease can expire and is never requeued while healthy. An
  **ingestion** job never performs a plain lease extension: its heartbeat is the
  combined commit-last renewal in the next bullet, which extends with
  `LEASE_DURATION`, and its interval must satisfy
  `INGESTION_HEARTBEAT_INTERVAL + LEASE_RENEWAL_MARGIN < LEASE_DURATION`
  (default: 5m + 30s < 15m). Configuration validation rejects any interval or
  duration combination that violates the precondition for the job type it
  governs, and a test asserts both validations independently (a query is never
  scheduled against the ingestion interval, and vice versa). A renewal that
  affects zero rows means the worker was fenced; it must abort without writing or
  committing.
- **Renewal and write credentials (ingestion only, commit-last).** Ingestion
  renewal follows the reserve → issue → persist ordering of §6.1, but the lease
  extension is moved to the persist step so that **the lease is never extended
  before a replacement credential that covers the new window is usable.** On
  each renewal the worker:
  1. **Reserve (no lease change).** computes the target new deadline
     `D = now() + LEASE_DURATION` and the reservation bound
     `reserved_until = D + CREDENTIAL_SLACK`, then commits a
     conditional update keyed on the current token and `status='RUNNING'` that
     sets `write_credential_reserved_until = reserved_until` **only** — it does
     **not** change `lease_expires_at`. A zero-row result means fenced → no
     credential is requested.
  2. **Issue.** requests a write credential whose TTL is
     `reserved_until − now()` (so its nominal expiry is `reserved_until`) and
     **verifies** the provider's actual expiry `E` satisfies
     `D ≤ E ≤ reserved_until`; an expiry outside either bound is rejected as an
     issuance failure.
  3. **Commit lease + expiry (atomic, conditional).** in a single conditional
     update keyed on the current token and `status='RUNNING'`, sets
     `lease_expires_at = D` **and**
     `write_credential_expires_at = GREATEST(COALESCE(write_credential_expires_at, 'epoch'), E)`
     **and** appends the new session ID to `write_credential_session_ids`, and
     clears the reservation. The persisted expiry is the **maximum** across every
     issued credential — never lowered by a newer issuance — so a renewal whose
     credential expires before an earlier, still-valid credential's expiry cannot
     shorten the sweep bound, and every session ID is retained so revocation
     covers the full set (§3.1, §4 rule 8).
     **Only this step extends the lease**, and no `staging/`/`lake/` write may
     use the *old* credential once it is known the lease is not being extended.
  Because the lease extension and the credential's persisted expiry are written
  in one transaction and `E ≥ D = lease_expires_at`,
  `write_credential_expires_at ≥ lease_expires_at` holds at every instant a
  credential exists, and the worker has a usable credential for the entire new
  window (the credential never expires before the lease). If the worker is
  fenced after step 1 or step 2 but before step 3, the durable reservation still
  bounds every credential it could have obtained (a step-2 credential has actual
  expiry `E ≤ reserved_until`), and the lease was **not** extended — so the job
  proceeds toward lease expiry and recovery, and the sweeper waits on
  `max(write_credential_expires_at, write_credential_reserved_until)` before
  deleting (§4 rule 8). If the credential cannot be re-issued (STS failure, a
  provider expiry outside `[D, reserved_until]`, or the requested TTL falling
  below `MIN_WRITE_CREDENTIAL_TTL`), step 3 is not committed and **the lease is
  not extended**; the worker keeps its existing credential and lease and retries
  the combined renewal within the current window. A worker that cannot commit a
  step 3 before its current lease would expire —
  `now() ≥ lease_expires_at` — stops writing and fails the job
  retryably: it releases the claim
  (`status='FAILED'`, `retryable=true`, `attempt=attempt+1`,
  `fencing_token=NULL`, `lease_owner=NULL`, `lease_expires_at=NULL`) and carries
  the write-quiescence bound forward —
  `write_credential_expires_at = GREATEST(COALESCE(write_credential_expires_at,
  'epoch'), COALESCE(write_credential_reserved_until, 'epoch'))` — retaining the
  reservation, so the existing credential and any reservation still bound the
  sweeper (§4 rule 8). If it fails to do so in time, the lease (never later than
  the credential) also expires and the recovery pass fences the job by the same
  carry-forward rule (§6.1), so a job is never left `RUNNING` under an extended
  lease without write capability. The stale-attempt sweeper reclaims the prefix
  only after
  `max(write_credential_expires_at, write_credential_reserved_until) +
  CLEANUP_GRACE` (§4 rule 8). An active ingestion therefore never runs out of
  write capability mid-job: its lease is renewed only together with a credential
  that covers the new window, and the credential always expires no earlier than
  the lease.
- **Query requeue is liveness-only.** Because a healthy worker renews the query
  lease continuously while the query runs, requeue (the recovery pass's
  `lease_expires_at < now()` transition, §13) fires only when the worker stops
  heartbeating — that is, when it has died or been partitioned — never when a
  live query is still executing within its declared execution timeout. The
  execution timeout is enforced separately by the worker (Go context deadline)
  and by DuckDB's `max_execution_time`; when it fires the worker itself
  transitions the job to `TIMED_OUT` via a fenced conditional update, which does
  not require the lease to have expired.

## 13. Query job lifecycle and crash model

Query status: `QUEUED → RUNNING → SUCCEEDED | FAILED | CANCELLED | TIMED_OUT`.

Every `query_job` carries the same two lease-fencing fields as an ingestion job
(`attempt` and `fencing_token`, §6.1) with the same rules: `attempt` is
monotonically incremented and `fencing_token` regenerated on every claim; all
transitions out of a non-terminal state are conditional on the current token and
expected status; and a zero-row conditional update means the worker was fenced
and must abort without publishing results.

| From | Event | To | Notes |
|---|---|---|---|
| (new) | submit validated | QUEUED | durable row (`attempt=0`, no token); the full SQL statement is persisted in `statement` and its hash in `statement_hash` — the worker executes the persisted statement, never a reconstruction from the hash |
| QUEUED | worker claims the job | RUNNING | conditional `UPDATE … SET status='RUNNING', lease_owner=?, lease_expires_at=now()+QUERY_LEASE_DURATION, attempt=attempt+1, fencing_token=<fresh UUID> WHERE id=? AND status='QUEUED'`; the holder then renews the lease by heartbeat (§12.1), so a live query is never requeued while it stays within its execution timeout |
| RUNNING | results committed to object store + row marked | SUCCEEDED | worker writes the spool to `exports/…/attempt=<n>/result.parquet`, then one conditional update `SET status='SUCCEEDED', result_ref=?, rows_returned=?, truncated=?, … WHERE id=? AND fencing_token=? AND status='RUNNING'`; zero rows → fenced, abort (spool left to TTL, never referenced) |
| RUNNING | cancel accepted | CANCELLED | conditional on token; observable and persisted |
| RUNNING | timeout exceeded | TIMED_OUT | conditional on token; distinct from FAILED |
| RUNNING | engine/object-store error | FAILED | conditional on token; `failure_reason` set (engine_error, object_store, resource_exceeded, …) |
| RUNNING | lease expires (worker died or partitioned — a liveness failure, not a normal timeout) | QUEUED (requeue) | recovery pass re-claims in one conditional update keyed on `lease_expires_at < now() AND status='RUNNING'`: `SET status='QUEUED', lease_owner=NULL, lease_expires_at=NULL, attempt=attempt+1, fencing_token=NULL`; the old worker is fenced by the token change. Requeue is a **liveness** signal only: a healthy worker renews the lease while the query runs, so a job is requeued only when its worker stops heartbeating — never because a live query outlived its lease while still within its declared execution timeout (§12.1) |

- Queued jobs survive worker restart (state is in PostgreSQL; the queue is
  reconstructed by scanning for `QUEUED` rows).
- A `RUNNING` query's lease is a sliding window renewed by heartbeat (§12.1); it
  is a liveness mechanism, not the query's compute budget (that is the execution
  timeout in §12). The worker renews the lease continuously while the query runs
  and has not exceeded its execution timeout, so a healthy query is never
  requeued mid-execution.
- The submitted statement is persisted verbatim in `query_job.statement` at
  submission and is the single source the worker uses to execute the job: on
  claim, the worker reads `statement` from the row, re-validates it against the
  §7 boundary, and executes it. It is never derived from the hash, so a job
  requeued after a worker restart executes the exact submitted text.
  `statement_hash` (SHA-256 of the normalized statement) is stored alongside it
  as an integrity/dedup aid and for the retention policy in §10; it is not a
  substitute for the statement.
- **Statement access and retention.** `statement` is written once by the API at
  submission and never updated thereafter. It is read by the worker on every
  claim (including a requeue after restart) and by the API when serving query
  history and the rerun action. It is retained for the job's full lifetime — it
  is never purged or overwritten before the job reaches a terminal state — and
  thereafter follows the history retention window (§10). It is never emitted in
  error responses or logs (§11); it is returned to the operator only through the
  history/rerun surface, which already carries the operator's own query text.
- An interrupted running job is requeued, never silently `SUCCEEDED`: the
  recovery pass increments `attempt` and clears the `fencing_token`, so any later
  commit by the delayed worker matches zero rows and is detected. The requeued
  job is claimed again by a (possibly different) worker under a fresh token, and
  its result is written to that attempt's own export prefix. The only
  `result_ref` that ever becomes visible is the one set by the current token
  holder's conditional commit; a stale attempt's spool is never referenced and is
  reclaimed by TTL.
- Cancellation and timeout are observable in status and persisted.
- **Lease/requeue liveness test**: a healthy query running at the maximum
  execution timeout (300s) and heartbeating on schedule is never requeued — its
  `status` stays `RUNNING` until the worker itself transitions it to `SUCCEEDED`
  or `TIMED_OUT`. A companion test kills the worker (no heartbeat) and asserts
  the job is requeued after `QUERY_LEASE_DURATION` and re-executed from the
  persisted `statement` (never from the hash). This pins requeue to liveness,
  not to the execution timeout.

## 14. Single-node limit

- DuckDB is single-node for this release. There is no sharding, no distributed
  execution, and no claim of horizontal scale-out.
- The query engine sits behind `internal/query.Engine` (defined in phase 4) so
  a measured need can justify Trino or another distributed engine later; no
  such work is scheduled in this plan.
- Documentation and the README must state this limit explicitly and must not
  describe the product as a distributed OLAP database.

## 15. Caller trust, authentication, and authorization

### 15.1 Trust model

v1 is a **single-operator, trusted-local deployment**. There is exactly one
namespace (`default`); there is no multi-tenant isolation. Every party that can
reach the API — the operator, the bundled dashboard, and any operator tooling —
is in the operator's trust domain and holds full capability to enumerate
datasets, submit queries, and consume results. This is a deliberate, documented
limit; multi-tenant identity and per-user authorization are out of scope (§1).

### 15.2 Enforceable exposure boundary

The exposure boundary is **layered**; no single control is treated as proof that
a caller is trusted. Loopback/private-network binding by itself is **not** a
caller boundary: a page on another site can cause the operator's browser to send
requests to `127.0.0.1` or a private-network address (cross-origin requests and
DNS-rebinding attacks), so a browser can reach the API even when the bound
address is loopback. The controls below are mandatory in the default deployment,
not optional hardening.

- **Network boundary (primary, necessary but not sufficient)**: by default the
  API and the dashboard bind to `127.0.0.1` (or the private Docker Compose
  network) and are **not** published to public or host-facing interfaces.
  Exposure beyond the local host is out of scope; a remote or shared deployment
  requires adding authentication before it is permitted (a precondition, not an
  option). This reduces the set of hosts that can originate packets, but it does
  not by itself identify or authorize the *caller* (see the browser-request
  defense below).

- **Host/Origin/fetch-site checks (mandatory)**: the API and the dashboard
  reject any request whose `Host` header is not in a closed, configured
  allowlist of the exact `host:port` values the services are bound to (for
  example `127.0.0.1:8080` and `localhost:8080`). A DNS-rebinding attack
  presents the attacker's domain in the `Host` header, so it fails the allowlist
  even though the underlying socket terminates on loopback. In addition:
  - If the request carries an `Origin` header, the API validates it against the
    exact scheme + host + port of the served dashboard and rejects a mismatch
    (cross-origin) with `403 forbidden`. `Origin` is validated **when supplied**;
    its **absence is not an error**. Browsers commonly send `Sec-Fetch-Site`
    without an `Origin` on same-origin safe `GET` requests, and non-browser
    clients omit `Origin` entirely, so a missing `Origin` does not by itself
    reject a request — the request still must satisfy the Host check and, for
    mutations, the CSRF proof below.
  - If the request carries a `Sec-Fetch-Site` header, its value is checked as an
    additional signal: `cross-site` is rejected with `403 forbidden`;
    `same-origin`, `same-site`, and `none` are accepted; a missing header is
    accepted. `Sec-Fetch-Site` is **not** used to demand an `Origin` header, so
    the dashboard's same-origin polling and result paging (which may send
    `Sec-Fetch-Site: same-origin` without `Origin`) are not rejected.
  - The API does not emit permissive CORS headers
    (`Access-Control-Allow-Origin: *`); it emits CORS allow headers only for the
    exact dashboard origin, so a foreign page cannot read any response.
  - Read-only `GET`/`HEAD` requests with a valid `Host` and, when present, a
    valid `Origin`/`Sec-Fetch-Site` are accepted. State-changing requests are
    not secured by `Origin` presence (a cross-site page may omit or forge a
    missing `Origin` on some flows); they rely on the mandatory CSRF proof below.

- **CSRF protection for state changes (mandatory)**: every state-changing
  request (`POST`, `PUT`, `PATCH`, `DELETE`) must carry proof of same-origin
  intent that a cross-site page cannot forge, regardless of whether an `Origin`
  header is present. The API requires one of: (a) a custom request header that
  forces a CORS preflight (for example `X-Lakehouse-CSRF: 1`), whose preflight
  is rejected for any non-allowlisted origin, or (b) an unguessable CSRF token
  issued by the API and bound to the operator's session (synchronizer token or
  double-submit cookie), presented on every mutation. Read-only `GET`/`HEAD`
  requests have no side effects and are covered by the Host/Origin/fetch-site
  checks above plus same-origin CORS; they are not exempt from the Host check.

- **Deployment token (optional hardening)**: the API MAY additionally be
  configured with a single static deployment token (`API_TOKEN`, supplied via
  environment/secret). When set, every request must present it as a bearer
  credential; a missing or invalid token returns `401 unauthorized`. Because the
  token is carried in the `Authorization` header (or an equivalent
  same-origin-only credential store), a cross-site page cannot learn or attach
  it, so it independently defeats cross-origin mutations. This is one shared
  operator credential, not per-user identity.

- The token is never embedded in the static dashboard bundle. In the default
  single-host Compose deployment the dashboard is served same-origin and relies
  on the Host/Origin/fetch-site checks plus CSRF protection (not on loopback
  alone); if token authentication is enabled, the token is provided to the
  operator's browser session at runtime (for example, via a same-origin
  login/session route that keeps the credential server-side), never baked into
  client-visible data.

### 15.3 Authorization

- There is **no per-user authorization** in v1: an authenticated caller is the
  operator and may perform any operation.
- `forbidden` (403) is reserved and is produced by (a) the mandatory
  browser-request defense — a request rejected by the Host/Origin/fetch-site
  checks or CSRF protection (§15.2) — and (b) deployment policy gates that are
  independent of caller identity, for example refusing to archive a dataset with
  in-flight jobs, or refusing a mutation on an immutable version.
- Both `unauthorized` and `forbidden` are defined now (rather than added later)
  so the error contract (§11) and its meaning are stable when per-user authn/z
  is introduced in a future release.

### 15.4 Testability

- With token authentication disabled, the API answers only on the configured
  bind address; a request from outside that boundary cannot reach it.
- **Host allowlist**: a request whose `Host` header is the attacker's domain
  (a DNS-rebinding simulation) or any value not in the allowlist returns
  `403 forbidden` and is not processed, even though the TCP connection
  terminates on the loopback interface.
- **Origin/fetch-site checks**: a browser-initiated request whose `Origin` is a
  foreign origin returns `403 forbidden`; a request whose `Origin` is the exact
  dashboard origin is accepted; a same-origin safe `GET` that sends
  `Sec-Fetch-Site` (e.g. `same-origin`) but **no** `Origin` header is accepted
  (dashboard polling and result paging are not rejected); a request carrying
  `Sec-Fetch-Site: cross-site` is rejected; and the API's CORS allow headers name
  only that exact origin (never `*`).
- **CSRF**: a state-changing request without the required CSRF proof (custom
  header or valid CSRF token) returns `403 forbidden` and causes no state
  change; the same request with valid proof succeeds. A companion test issues a
  state-changing request from a simulated cross-site page (foreign `Origin` plus
  no CSRF proof) and asserts it is rejected with no side effect.
- With token authentication enabled, requests without a token return `401`, and
  requests with a wrong token return `401`.
- A policy-gated operation (e.g., archive with in-flight jobs) returns `403`
  with a stable code, proving the authorization path exists independently of
  caller identity.
