#!/bin/sh
# Generate a gitignored `.env` with local-only credentials so no secret is ever
# committed to the repository.
#
# Idempotent: any value already present in `.env` is preserved, and only a
# missing/blank secret is filled with a fresh random value. `make up` runs this
# before `docker compose up`, so a clean checkout starts without a manual step.
set -eu

ENV_FILE=".env"

# Non-secret identifiers. These are names, not credentials, and are safe to pin.
POSTGRES_USER="lakehouse"
POSTGRES_DB="lakehouse"
MINIO_ROOT_USER="lakehouse"
MINIO_BUCKET="lakehouse"
LOG_LEVEL="info"
# Secrets start empty; they are generated below.
POSTGRES_PASSWORD=""
MINIO_ROOT_PASSWORD=""

if [ -f "$ENV_FILE" ]; then
    # shellcheck disable=SC1090
    . "./$ENV_FILE"
fi

POSTGRES_USER="${POSTGRES_USER:-lakehouse}"
POSTGRES_DB="${POSTGRES_DB:-lakehouse}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-lakehouse}"
MINIO_BUCKET="${MINIO_BUCKET:-lakehouse}"
LOG_LEVEL="${LOG_LEVEL:-info}"

if [ -z "${POSTGRES_PASSWORD:-}" ]; then
    POSTGRES_PASSWORD="$(openssl rand -hex 32)"
fi
if [ -z "${MINIO_ROOT_PASSWORD:-}" ]; then
    MINIO_ROOT_PASSWORD="$(openssl rand -hex 32)"
fi

umask 077
{
    printf 'POSTGRES_USER=%s\n' "$POSTGRES_USER"
    printf 'POSTGRES_PASSWORD=%s\n' "$POSTGRES_PASSWORD"
    printf 'POSTGRES_DB=%s\n' "$POSTGRES_DB"
    printf 'MINIO_ROOT_USER=%s\n' "$MINIO_ROOT_USER"
    printf 'MINIO_ROOT_PASSWORD=%s\n' "$MINIO_ROOT_PASSWORD"
    printf 'MINIO_BUCKET=%s\n' "$MINIO_BUCKET"
    printf 'LOG_LEVEL=%s\n' "$LOG_LEVEL"
} > "$ENV_FILE"

echo "Wrote $ENV_FILE with local-only random credentials (gitignored)."
