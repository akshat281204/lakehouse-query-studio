# Catalog and dataset registration (phase 2)

This document records the phase-2 implementation of the durable metadata
catalog and Parquet registration. The normative contracts (object layout,
version semantics, error codes) remain in `docs/design.md`.

## Durable entities

PostgreSQL holds the catalog. Migrations live in `migrations/` and are applied
in filename order by `internal/catalog.Migrate`, each in its own transaction,
tracked in `schema_migrations`.

| Table | Purpose |
|---|---|
| `namespaces` | Exactly one `default` namespace, seeded at a fixed id. |
| `datasets` | Name (unique per namespace), source prefix, format, status, timestamps. |
| `dataset_versions` | Immutable version (monotonic per dataset), object prefix, format, partition columns, status, row count. |
| `schema_columns` | Captured column descriptions, unique position per version. |
| `ingestion_jobs` | The durable job record (a synchronous `register` job in this phase). |

Uniqueness and foreign-key constraints are declared in the migrations; a
duplicate `(namespace, name)` registration is rejected as `already_exists`
(409).

## Registration flow

`POST /api/v1/datasets` runs synchronously:

1. Validate the dataset name against the canonical key grammar (lowercase ASCII
   `[a-z0-9-]`, no leading/trailing hyphen, ≤ 63 chars) and the object prefix
   (a clean relative prefix).
2. List the prefix with a bounded number of pages (≤ 10 pages of 1,000 keys).
   A missing prefix or an object-store failure is rejected.
3. Inspect the footer of the first Parquet object (bounded read of the footer
   only, never the whole object) to capture the flattened schema and row count.
4. Detect Hive-style `name=value` partition columns from the object keys unless
   the request declares them explicitly.
5. Persist the dataset, its first `READY` version, the schema columns, and a
   `SUCCEEDED` register job in one transaction.

Registration is durable: after an API restart the catalog is re-read from
PostgreSQL. In this phase registration is a **reference** to existing data
(`object_prefix == source_prefix`); copying source objects into the canonical
`lake/…` layout arrives with ingestion (phase 3).

## APIs

- `POST /api/v1/datasets` — register a Parquet prefix.
- `GET /api/v1/datasets?offset=&limit=&q=` — paged, searchable list.
- `GET /api/v1/datasets/{id}` — dataset detail plus latest version and schema.
- `GET /api/v1/datasets/{id}/schema` — schema columns.
- `GET /api/v1/datasets/{id}/preview?limit=` — bounded sample rows.

All non-2xx responses use the `docs/design.md` §11 error envelope.

## Boundedness

- Prefix listing is paged and capped.
- Schema inspection reads only the Parquet footer.
- Preview reads the footer and the first data page of each column, capped at 20
  rows by default (max 100) and 64 MiB per decompressed page.

## Phase-2 limitations

- Preview decodes flat (non-nested) schemas with `UNCOMPRESSED`, `SNAPPY`, or
  `GZIP` compression and `PLAIN`/dictionary encoding. Nested schemas are
  reported via the preview response's `note` field, and unsupported codecs are
  surfaced as errors rather than silently empty results.
- CSV/JSON conversion and SQL execution are out of scope for this phase.
